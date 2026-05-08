/**
 * India Live Prices — Railway Server
 * Kite WebSocket → Supabase india_live_prices
 *
 * Flow:
 * 1. Startup: auto-login via TOTP, get access token
 * 2. Fetch all india_stocks symbols + tokens from Supabase
 * 3. Subscribe to Kite WebSocket for live ticks
 * 4. Flush prices to Supabase every 2s
 * 5. Daily at 2:00 UTC (7:30 AM IST): re-login, refresh token, reconnect WS
 * 6. Daily at 10:30 UTC (4:00 PM IST, after market close): refresh India returns + GLOCOM
 */

const { KiteConnect, KiteTicker } = require("kiteconnect");
const { createClient } = require("@supabase/supabase-js");
const { TOTP } = require("otpauth");
const axios = require("axios");
const cron = require("node-cron");
const http = require("http");

// ── ENV ───────────────────────────────────────────────────────────────────────
const KITE_API_KEY         = process.env.KITE_API_KEY;
const KITE_API_SECRET      = process.env.KITE_API_SECRET;
const KITE_USER_ID         = process.env.KITE_USER_ID;
const KITE_PASSWORD        = process.env.KITE_PASSWORD;
const KITE_TOTP_SECRET     = process.env.KITE_TOTP_SECRET;
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_KEY;

if (!KITE_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("❌ Missing required environment variables");
  process.exit(1);
}

// ── SUPABASE ──────────────────────────────────────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ── STATE ─────────────────────────────────────────────────────────────────────
let ticker        = null;
let accessToken   = null;
let kc            = null;
let symbolMap     = {};   // instrument_token → { symbol, prev_close }
let priceBuffer   = {};   // symbol → { ltp, percent_change }
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
    const jar = {};
    const withCookies = (headers = {}) => ({
      ...headers,
      Cookie: Object.entries(jar).map(([k, v]) => `${k}=${v}`).join("; "),
    });
    const saveCookies = (setCookieHeader) => {
      if (!setCookieHeader) return;
      const cookies = Array.isArray(setCookieHeader) ? setCookieHeader : [setCookieHeader];
      for (const c of cookies) {
        const [pair] = c.split(";");
        const [k, v] = pair.split("=");
        jar[k.trim()] = (v || "").trim();
      }
    };

    // Step 1: Login with user_id + password
    const loginResp = await axios.post(
      "https://kite.zerodha.com/api/login",
      new URLSearchParams({ user_id: KITE_USER_ID, password: KITE_PASSWORD }).toString(),
      { headers: { "Content-Type": "application/x-www-form-urlencoded", "User-Agent": "Mozilla/5.0", ...withCookies() }, maxRedirects: 0, validateStatus: (s) => s < 500 }
    );
    saveCookies(loginResp.headers["set-cookie"]);
    const requestId = loginResp.data?.data?.request_id;
    if (!requestId) throw new Error(`No request_id: ${JSON.stringify(loginResp.data)}`);

    // Step 2: TOTP 2FA
    const totpCode = generateTOTP();
    console.log(`🔑 TOTP: ${totpCode}`);
    const twoFaResp = await axios.post(
      "https://kite.zerodha.com/api/twofa",
      new URLSearchParams({ user_id: KITE_USER_ID, request_id: requestId, twofa_value: totpCode, twofa_type: "totp" }).toString(),
      { headers: { "Content-Type": "application/x-www-form-urlencoded", "User-Agent": "Mozilla/5.0", ...withCookies() }, maxRedirects: 0, validateStatus: (s) => s < 500 }
    );
    saveCookies(twoFaResp.headers["set-cookie"]);
    if (twoFaResp.data?.status !== "success") throw new Error(`2FA failed: ${JSON.stringify(twoFaResp.data)}`);

    // Step 3: OAuth redirect → request_token
    const connectResp = await axios.get(
      `https://kite.trade/connect/login?api_key=${KITE_API_KEY}&v=3`,
      { headers: { "User-Agent": "Mozilla/5.0", ...withCookies() }, maxRedirects: 0, validateStatus: (s) => s < 500 }
    );
    const location = connectResp.headers["location"] || "";
    if (!location) throw new Error("No redirect from Kite Connect OAuth");
    const url = new URL(location.startsWith("http") ? location : `https://placeholder${location}`);
    const requestToken = url.searchParams.get("request_token");
    if (!requestToken) throw new Error(`No request_token in: ${location}`);
    console.log(`🎫 request_token: ${requestToken.slice(0, 8)}...`);

    // Step 4: Exchange for access_token
    kc = new KiteConnect({ api_key: KITE_API_KEY });
    const session = await kc.generateSession(requestToken, KITE_API_SECRET);
    accessToken = session.access_token;
    kc.setAccessToken(accessToken);

    console.log(`✅ Kite login success. Token: ${accessToken.slice(0, 8)}...`);
    return accessToken;
  } catch (err) {
    console.error("❌ Kite login failed:", err.message);
    throw err;
  }
}

// ── FETCH SYMBOLS ─────────────────────────────────────────────────────────────
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

// ── FLUSH PRICES ──────────────────────────────────────────────────────────────
async function flushPrices() {
  const entries = Object.entries(priceBuffer);
  if (!entries.length) return;
  const rows = entries.map(([symbol, d]) => ({
    symbol, ltp: d.ltp, percent_change: d.percent_change, updated_at: new Date().toISOString(),
  }));
  priceBuffer = {};
  const { error } = await supabase.from("india_live_prices").upsert(rows, { onConflict: "symbol" });
  if (error) console.warn("⚠️  Supabase upsert error:", error.message);
}

// ── START TICKER ──────────────────────────────────────────────────────────────
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
  ticker.on("noreconnect", () => { console.warn("⚠️ Max reconnects, retrying in 30s..."); setTimeout(reconnect, 30000); });
  ticker.connect();

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
    setTimeout(reconnect, 15000);
  }
}

// ── INDIA DATA REFRESH ────────────────────────────────────────────────────────
// Uses Kite quote API (accurate NSE data) to refresh ret_1d + price for all
// india_stocks and india_etfs, then recomputes GLOCOM

async function refreshIndiaReturns() {
  console.log("📊 India returns refresh starting...");
  if (!kc || !accessToken) {
    console.warn("⚠️ No Kite session — skipping refresh");
    return;
  }

  try {
    for (const universe of ["india_stocks", "india_etfs"]) {
      let allRows = [], offset = 0;
      while (true) {
        const { data } = await supabase
          .from("returns_latest")
          .select("instrument_id, symbol")
          .eq("universe", universe)
          .range(offset, offset + 999);
        if (!data?.length) break;
        allRows = [...allRows, ...data];
        if (data.length < 1000) break;
        offset += 1000;
      }
      console.log(`  ${universe}: ${allRows.length} instruments`);

      let updated = 0;
      // Kite getQuote accepts up to 500 symbols per call
      for (let i = 0; i < allRows.length; i += 500) {
        const batch = allRows.slice(i, i + 500);
        try {
          kc.setAccessToken(accessToken);
          const quotes = await kc.getQuote(batch.map(s => `NSE:${s.symbol}`));
          for (const row of batch) {
            const q = quotes[`NSE:${row.symbol}`];
            if (!q) continue;
            const ltp       = q.last_price;
            const prevClose = q.ohlc?.close || ltp;
            const ret1d     = prevClose > 0
              ? parseFloat(((ltp - prevClose) / prevClose * 100).toFixed(4)) : null;
            await supabase
              .from("returns_latest")
              .update({ ret_1d: ret1d, price_native: ltp, price_usd: ltp })
              .eq("instrument_id", row.instrument_id);
            updated++;
          }
        } catch (err) {
          console.warn(`  ⚠️ Quote batch ${i}-${i+500} failed:`, err.message);
        }
        await new Promise(r => setTimeout(r, 300));
      }
      console.log(`  ✅ ${universe}: updated ${updated}`);
    }

    // Recompute GLOCOM for both India universes
    await recomputeIndiaGlocom();
    console.log("✅ India refresh complete");

  } catch (err) {
    console.error("❌ India refresh error:", err.message);
  }
}

async function recomputeIndiaGlocom() {
  console.log("  🔄 Recomputing India GLOCOM...");

  const classify = (s1w, s6m, s1y) => {
    if (s1w > 80 && s6m > 40 && s1y > 80)                return [1, "US 80 80"];
    if (s1w <= 80 && s6m > 40 && s1y > 80)               return [1, "LTS 80"];
    if (s1w > 60 && s6m > 40 && s1y > 60 && s1y <= 80)   return [2, "US 60 60"];
    if (s1w <= 60 && s6m > 40 && s1y > 60 && s1y <= 80)  return [2, "LTS 60"];
    if (s1w < 30 && s6m < 30)                             return [5, "UW 30 30"];
    if (s1w >= 30 && s6m < 30)                            return [5, "LTW 30"];
    if (s1w < 40 && s6m < 40)                             return [4, "UW 40 40"];
    if (s1w <= 40 && s6m < 40)                            return [4, "LTW 40"];
    return [3, "Neutral 40 60"];
  };

  for (const universe of ["india_stocks", "india_etfs"]) {
    let rows = [], offset = 0;
    while (true) {
      const { data } = await supabase
        .from("returns_latest")
        .select("instrument_id, ret_1w, ret_6m, ret_1y")
        .eq("universe", universe)
        .range(offset, offset + 999);
      if (!data?.length) break;
      rows = [...rows, ...data];
      if (data.length < 1000) break;
      offset += 1000;
    }
    if (!rows.length) continue;

    // Compute within-universe percentiles
    const pctile = (key) => {
      const valid = rows.filter(r => r[key] != null).sort((a, b) => a[key] - b[key]);
      const n = valid.length;
      const map = {};
      valid.forEach((r, i) => { map[r.instrument_id] = (i / Math.max(n - 1, 1)) * 100; });
      return map;
    };
    const p1w = pctile("ret_1w");
    const p6m = pctile("ret_6m");
    const p1y = pctile("ret_1y");

    const glocomRows = rows.map(r => {
      const s1w = p1w[r.instrument_id] ?? 50;
      const s6m = p6m[r.instrument_id] ?? 50;
      const s1y = p1y[r.instrument_id] ?? 50;
      const [code, label] = classify(s1w, s6m, s1y);
      return {
        instrument_id: r.instrument_id, universe,
        glocom_code: code, glocom_label: label,
        sort_score: Math.round(s1w * 10) / 10,
        pctile_1w: Math.round(s1w * 10) / 10,
        pctile_6m: Math.round(s6m * 10) / 10,
        pctile_1y: Math.round(s1y * 10) / 10,
      };
    });

    for (let i = 0; i < glocomRows.length; i += 500) {
      const { error } = await supabase
        .from("glocom_latest")
        .upsert(glocomRows.slice(i, i + 500), { onConflict: "instrument_id" });
      if (error) console.warn(`  ⚠️ GLOCOM upsert [${universe}]:`, error.message);
    }
    console.log(`  ✅ GLOCOM done: ${glocomRows.length} ${universe}`);
  }
}

// ── CRON JOBS ─────────────────────────────────────────────────────────────────

// 7:30 AM IST (2:00 UTC) weekdays — token refresh + reconnect WS
cron.schedule("0 2 * * 1-5", async () => {
  console.log("🕐 [2:00 UTC] Daily token refresh...");
  try {
    await kiteLogin();
    const tokens = await fetchSymbols();
    await startTicker(tokens);
    console.log("✅ Token refresh done");
  } catch (err) {
    console.error("❌ Token refresh failed:", err.message);
  }
}, { timezone: "UTC" });

// 4:00 PM IST (10:30 UTC) weekdays — India data refresh after market close
cron.schedule("30 10 * * 1-5", async () => {
  console.log("📊 [10:30 UTC] India market close — refreshing returns + GLOCOM...");
  await refreshIndiaReturns();
}, { timezone: "UTC" });

// ── STARTUP ───────────────────────────────────────────────────────────────────
async function main() {
  console.log("🚀 India Live Prices Server starting...");
  try {
    await kiteLogin();
    const tokens = await fetchSymbols();
    if (tokens.length === 0) {
      console.warn("⚠️  No tokens found. Retrying in 60s...");
      setTimeout(main, 60000);
      return;
    }
    await startTicker(tokens);
    console.log("✅ Server running — WS live, data refresh at 4 PM IST daily");
  } catch (err) {
    console.error("❌ Startup failed:", err.message);
    console.log("🔄 Retrying in 30s...");
    setTimeout(main, 30000);
  }
}

main();

// ── HEALTH CHECK ──────────────────────────────────────────────────────────────
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
