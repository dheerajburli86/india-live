/**
 * India Live Prices — Railway Server
 * Kite WebSocket → Supabase india_live_prices
 *
 * Flow:
 * 1. On startup: auto-login to Kite using TOTP, get access token
 * 2. Fetch all india_stocks symbols + tokens from Supabase
 * 3. Subscribe to Kite WebSocket for live ticks
 * 4. Upsert prices into india_live_prices every tick
 * 5. Every day at 7:30am IST: re-login, get fresh token, reconnect WS
 */

const { KiteConnect, KiteTicker } = require("kiteconnect");
const { createClient } = require("@supabase/supabase-js");
const { TOTP } = require("otpauth");
const axios = require("axios");
const cron = require("node-cron");

// ── ENV ───────────────────────────────────────────────────────────────────────
const {
  KITE_API_KEY,
  KITE_API_SECRET,
  KITE_USER_ID,
  KITE_PASSWORD,
  KITE_TOTP_SECRET,
  SUPABASE_URL,
  SUPABASE_SERVICE_KEY,
} = process.env;

if (!KITE_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("❌ Missing required environment variables");
  process.exit(1);
}

// ── SUPABASE ──────────────────────────────────────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ── STATE ─────────────────────────────────────────────────────────────────────
let ticker = null;
let accessToken = null;
let symbolMap = {};      // instrument_token → { symbol, prev_close }
let priceBuffer = {};    // symbol → { ltp, percent_change }
let flushInterval = null;

// ── TOTP ──────────────────────────────────────────────────────────────────────
function generateTOTP() {
  const totp = new TOTP({ secret: KITE_TOTP_SECRET, algorithm: "SHA1", digits: 6, period: 30 });
  return totp.generate();
}

// ── LOGIN ─────────────────────────────────────────────────────────────────────
async function kiteLogin() {
  console.log("🔐 Logging into Kite...");
  try {
    // Step 1: Get request token via headless login
    const loginResp = await axios.post(
      "https://kite.zerodha.com/api/login",
      new URLSearchParams({ user_id: KITE_USER_ID, password: KITE_PASSWORD }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );

    const requestId = loginResp.data?.data?.request_id;
    if (!requestId) throw new Error("No request_id from login");

    // Step 2: Submit TOTP
    const totpCode = generateTOTP();
    const twoFaResp = await axios.post(
      "https://kite.zerodha.com/api/twofa",
      new URLSearchParams({ user_id: KITE_USER_ID, request_id: requestId, twofa_value: totpCode, twofa_type: "totp" }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );

    const requestToken = twoFaResp.data?.data?.request_token;
    if (!requestToken) throw new Error("No request_token from 2FA");

    // Step 3: Exchange for access token
    const kc = new KiteConnect({ api_key: KITE_API_KEY });
    const session = await kc.generateSession(requestToken, KITE_API_SECRET);
    accessToken = session.access_token;

    console.log(`✅ Kite login success. Token: ${accessToken.slice(0, 8)}...`);
    return accessToken;
  } catch (err) {
    console.error("❌ Kite login failed:", err.message);
    throw err;
  }
}

// ── FETCH SYMBOLS FROM SUPABASE ───────────────────────────────────────────────
async function fetchSymbols() {
  console.log("📋 Fetching india_stocks symbols from Supabase...");
  const { data, error } = await supabase
    .from("instruments")
    .select("symbol, kite_token, last_price")
    .eq("universe", "india_stocks")
    .eq("is_active", true)
    .not("kite_token", "is", null);

  if (error) throw new Error(`Supabase fetch error: ${error.message}`);

  symbolMap = {};
  const tokens = [];
  for (const row of data || []) {
    if (row.kite_token) {
      symbolMap[row.kite_token] = {
        symbol: row.symbol,
        prev_close: row.last_price || null,
      };
      tokens.push(row.kite_token);
    }
  }

  console.log(`✅ ${tokens.length} symbols loaded`);
  return tokens;
}

// ── FLUSH BUFFER TO SUPABASE ──────────────────────────────────────────────────
async function flushPrices() {
  const entries = Object.entries(priceBuffer);
  if (!entries.length) return;

  const rows = entries.map(([symbol, d]) => ({
    symbol,
    ltp: d.ltp,
    percent_change: d.percent_change,
    updated_at: new Date().toISOString(),
  }));

  priceBuffer = {};

  const { error } = await supabase
    .from("india_live_prices")
    .upsert(rows, { onConflict: "symbol" });

  if (error) console.warn("⚠️  Supabase upsert error:", error.message);
  else console.log(`📤 Flushed ${rows.length} prices`);
}

// ── START TICKER ──────────────────────────────────────────────────────────────
async function startTicker(tokens) {
  if (ticker) {
    try { ticker.disconnect(); } catch (e) {}
    ticker = null;
  }

  ticker = new KiteTicker({ api_key: KITE_API_KEY, access_token: accessToken });

  ticker.on("connect", () => {
    console.log("🟢 Kite WebSocket connected");
    // Subscribe in batches of 200
    const batchSize = 200;
    for (let i = 0; i < tokens.length; i += batchSize) {
      const batch = tokens.slice(i, i + batchSize);
      ticker.subscribe(batch);
      ticker.setMode(ticker.modeLTP, batch);
    }
    console.log(`📡 Subscribed to ${tokens.length} instruments`);
  });

  ticker.on("ticks", (ticks) => {
    for (const tick of ticks) {
      const info = symbolMap[tick.instrument_token];
      if (!info) continue;

      const ltp = tick.last_price;
      const prevClose = info.prev_close;
      const pctChange = prevClose && prevClose > 0
        ? parseFloat(((ltp - prevClose) / prevClose * 100).toFixed(4))
        : null;

      priceBuffer[info.symbol] = { ltp, percent_change: pctChange };
    }
  });

  ticker.on("disconnect", (err) => {
    console.warn("🔴 WebSocket disconnected:", err?.message || "unknown");
    setTimeout(() => reconnect(), 5000);
  });

  ticker.on("error", (err) => {
    console.error("❌ WebSocket error:", err?.message || err);
  });

  ticker.on("noreconnect", () => {
    console.warn("⚠️  Max reconnects reached, restarting in 30s...");
    setTimeout(() => reconnect(), 30000);
  });

  ticker.connect();

  // Flush prices to Supabase every 2 seconds
  if (flushInterval) clearInterval(flushInterval);
  flushInterval = setInterval(flushPrices, 2000);
}

// ── RECONNECT ─────────────────────────────────────────────────────────────────
async function reconnect() {
  console.log("🔄 Reconnecting...");
  try {
    const tokens = Object.keys(symbolMap).map(Number);
    await startTicker(tokens);
  } catch (err) {
    console.error("❌ Reconnect failed:", err.message);
    setTimeout(() => reconnect(), 15000);
  }
}

// ── DAILY TOKEN REFRESH ───────────────────────────────────────────────────────
// Runs every day at 7:30am IST (2:00 UTC)
cron.schedule("0 2 * * *", async () => {
  console.log("🕐 Daily token refresh...");
  try {
    await kiteLogin();
    const tokens = await fetchSymbols();
    await startTicker(tokens);
    console.log("✅ Daily refresh complete");
  } catch (err) {
    console.error("❌ Daily refresh failed:", err.message);
  }
}, { timezone: "UTC" });

// ── STARTUP ───────────────────────────────────────────────────────────────────
async function main() {
  console.log("🚀 India Live Prices Server starting...");

  try {
    await kiteLogin();
    const tokens = await fetchSymbols();

    if (tokens.length === 0) {
      console.warn("⚠️  No tokens found in instruments table. Check kite_token column.");
      console.log("💡 Server will retry in 60 seconds...");
      setTimeout(main, 60000);
      return;
    }

    await startTicker(tokens);
    console.log("✅ Server running");
  } catch (err) {
    console.error("❌ Startup failed:", err.message);
    console.log("🔄 Retrying in 30 seconds...");
    setTimeout(main, 30000);
  }
}

main();

// ── HEALTH CHECK ──────────────────────────────────────────────────────────────
const http = require("http");
http.createServer((req, res) => {
  res.writeHead(200);
  res.end(JSON.stringify({
    status: "ok",
    symbols: Object.keys(symbolMap).length,
    token: accessToken ? "set" : "missing",
    buffer: Object.keys(priceBuffer).length,
  }));
}).listen(process.env.PORT || 3000, () => {
  console.log(`🌐 Health check on port ${process.env.PORT || 3000}`);
});
