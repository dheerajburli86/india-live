/**
 * India Live Prices — Railway Server
 * Kite WebSocket → Supabase india_live_prices
 */

const { KiteConnect, KiteTicker } = require("kiteconnect");
const { createClient } = require("@supabase/supabase-js");
const { TOTP } = require("otpauth");
const axios = require("axios");
const cron = require("node-cron");

const {
  KITE_API_KEY,
  KITE_API_SECRET,
  KITE_USER_ID,
  KITE_PASSWORD,
  KITE_TOTP_SECRET,
  KITE_ACCESS_TOKEN,
  SUPABASE_URL,
  SUPABASE_SERVICE_KEY,
} = process.env;

if (!KITE_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("❌ Missing required environment variables");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

let ticker = null;
let accessToken = null;
let symbolMap = {};
let priceBuffer = {};
let flushInterval = null;

function generateTOTP() {
  const totp = new TOTP({ secret: KITE_TOTP_SECRET, algorithm: "SHA1", digits: 6, period: 30 });
  return totp.generate();
}

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
      symbolMap[row.kite_token] = { symbol: row.symbol, prev_close: row.last_price || null };
      tokens.push(row.kite_token);
    }
  }
  console.log(`✅ ${tokens.length} symbols loaded`);
  return tokens;
}

async function flushPrices() {
  const entries = Object.entries(priceBuffer);
  if (!entries.length) return;
  const rows = entries.map(([symbol, d]) => ({
    symbol, ltp: d.ltp, percent_change: d.percent_change, updated_at: new Date().toISOString(),
  }));
  priceBuffer = {};
  const { error } = await supabase.from("india_live_prices").upsert(rows, { onConflict: "symbol" });
  if (error) console.warn("⚠️  Supabase upsert error:", error.message);
  else console.log(`📤 Flushed ${rows.length} prices`);
}

async function startTicker(tokens) {
  if (ticker) { try { ticker.disconnect(); } catch (e) {} ticker = null; }
  ticker = new KiteTicker({ api_key: KITE_API_KEY, access_token: accessToken });

  ticker.on("connect", () => {
    console.log("🟢 Kite WebSocket connected");
    for (let i = 0; i < tokens.length; i += 200) {
      const batch = tokens.slice(i, i + 200);
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
      const pctChange = info.prev_close && info.prev_close > 0
        ? parseFloat(((ltp - info.prev_close) / info.prev_close * 100).toFixed(4)) : null;
      priceBuffer[info.symbol] = { ltp, percent_change: pctChange };
    }
  });

  ticker.on("disconnect", (err) => { console.warn("🔴 WS disconnected:", err?.message); setTimeout(reconnect, 5000); });
  ticker.on("error", (err) => { console.error("❌ WS error:", err?.message || err); });
  ticker.on("noreconnect", () => { setTimeout(reconnect, 30000); });
  ticker.connect();

  if (flushInterval) clearInterval(flushInterval);
  flushInterval = setInterval(flushPrices, 2000);
}

async function reconnect() {
  try {
    const tokens = Object.keys(symbolMap).map(Number);
    await startTicker(tokens);
  } catch (err) {
    console.error("❌ Reconnect failed:", err.message);
    setTimeout(reconnect, 15000);
  }
}

async function main() {
  console.log("🚀 India Live Prices Server starting...");
  try {
    // ── TODAY: use manual token from env ──
    accessToken = KITE_ACCESS_TOKEN;
    console.log(`✅ Using manual token: ${accessToken.slice(0, 8)}...`);

    const tokens = await fetchSymbols();
    if (tokens.length === 0) {
      console.warn("⚠️  No tokens found. Retrying in 60s...");
      setTimeout(main, 60000);
      return;
    }
    await startTicker(tokens);
    console.log("✅ Server running");
  } catch (err) {
    console.error("❌ Startup failed:", err.message);
    setTimeout(main, 30000);
  }
}

main();

const http = require("http");
http.createServer((req, res) => {
  res.writeHead(200);
  res.end(JSON.stringify({ status: "ok", symbols: Object.keys(symbolMap).length, token: accessToken ? "set" : "missing" }));
}).listen(process.env.PORT || 3000, () => console.log(`🌐 Health check on port ${process.env.PORT || 3000}`));
