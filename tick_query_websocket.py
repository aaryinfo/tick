"""
Tick Query Feed (WebSocket) — Nifty 500, real tick-by-tick
--------------------------------------------------------------
Same minimal Iris-style feed (Symbol + Volume) as tick_query_feed.py,
but this version is event-driven off TradingView's own live WebSocket
protocol instead of polling the scanner every 5s — so a "big print"
shows up the instant TradingView's backend sees it, not up to 5
seconds later.

HOW IT WORKS
- Opens one WebSocket to TradingView's internal quote feed (the same
  channel their own charting UI uses to update prices/volume live).
- Subscribes to every symbol in NIFTY500_STOCKS on an NSE quote session.
- Each incoming "qsd" push carries a symbol's *cumulative day volume*.
  The size of the print that just happened is:
        tick_volume = current_cumulative_volume - previous_cumulative_volume
  That delta — not the raw cumulative number — is what gets compared
  against TICK_VOLUME_THRESHOLD to decide "unusual single print".
- Anything over the threshold is pushed straight into the feed, no
  polling interval involved at all.

HONEST CAVEATS (please read before relying on this):
1. This is TradingView's undocumented internal protocol, not a public
   API — it's reverse-engineered (multiple open-source projects use
   the same approach), not something TradingView publishes or
   supports. It can change or start blocking without notice.
2. NSE data is real-time by default for non-professional TradingView
   accounts (per TradingView's own docs) — but this connects
   anonymously (no login), and anonymous sessions have occasionally
   been delayed/rate-limited in practice. If you see the feed lagging
   real trades, log in with a real TradingView auth token (see
   AUTH_TOKEN below) rather than assuming it's broken.
3. Subscribing to 200-500 symbols on one anonymous connection may get
   throttled. If you see gaps or disconnects, split NIFTY500_STOCKS
   across a few connections/sessions rather than one giant one.
4. This is still "biggest single print" detection, not full order-flow
   / bid-ask aggressor classification like real Iris tick reads.

Install:
    pip install websocket-client flask --break-system-packages

Run:
    python tick_query_websocket.py
    -> open http://localhost:5003
"""

from __future__ import annotations

import os
import json
import random
import re
import string
import threading
import time
from datetime import datetime, timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))
from collections import deque
import yfinance as yf
import pandas as pd

import websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Universe — starter Nifty 500 set (paste your full constituent list here)
# ---------------------------------------------------------------------------
NIFTY500_STOCKS = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
    "KOTAKBANK","ITC","LT","AXISBANK","BAJFINANCE","ASIANPAINT","MARUTI","SUNPHARMA",
    "TITAN","ULTRACEMCO","NESTLEIND","WIPRO","ONGC","NTPC","POWERGRID","M&M","TATAMOTORS",
    "TATASTEEL","JSWSTEEL","ADANIPORTS","ADANIENT","COALINDIA","BAJAJFINSV","HCLTECH",
    "TECHM","GRASIM","DIVISLAB","DRREDDY","CIPLA","EICHERMOT","BRITANNIA","HEROMOTOCO",
    "BAJAJ-AUTO","APOLLOHOSP","SBILIFE","HDFCLIFE","INDUSINDBK","BPCL","SHREECEM","UPL",
    "TATACONSUM","VEDL","GAIL","PIDILITIND","DLF","GODREJCP","SIEMENS","HAVELLS","AMBUJACEM",
    "DABUR","MARICO","BANDHANBNK","BANKBARODA","PNB","CANBK","IDFCFIRSTB","FEDERALBNK",
    "AUBANK","RBLBANK","MUTHOOTFIN","CHOLAFIN","LICHSGFIN","PFC","RECLTD","IRFC","SAIL",
    "NMDC","HINDALCO","NATIONALUM","JINDALSTEL","MOIL","RATNAMANI","APLAPOLLO","JSL",
    "ASHOKLEY","TVSMOTOR","MOTHERSON","BOSCHLTD","BALKRISIND","MRF","EXIDEIND","AMARAJABAT",
    "BHARATFORG","CUMMINSIND","SKFINDIA","SCHAEFFLER","ESCORTS","GODFRYPHLP","VOLTAS",
    "BLUESTARCO","CROMPTON","WHIRLPOOL","DIXON","POLYCAB","KEI","FINOLEXIND","ASTRAL",
    "SUPREMEIND","PIIND","SRF","AARTIIND","DEEPAKNTR","NAVINFLUOR","GNFC","GUJGASLTD",
    "PETRONET","IGL","MGL","OIL","HINDPETRO","IOC","BEL","HAL","BEML","BHEL","CONCOR",
    "IRCTC","RAILTEL","RVNL","IEX","CDSL","BSE","MCX","ANGELONE","IIFL","MFSL","LTIM",
    "PERSISTENT","COFORGE","MPHASIS","LTTS","OFSS","TATAELXSI","ZENSARTECH","CYIENT",
    "KPITTECH","SONACOMS","UNOMINDA","SUZLON","INOXWIND","CGPOWER","THERMAX","ABB",
    "TIINDIA","CARBORUNIV","GRINDWELL","POLYMED","SYNGENE","GLAND","LAURUSLABS","ALKEM",
    "TORNTPHARM","LUPIN","AUROPHARMA","BIOCON","IPCALAB","GLENMARK","ZYDUSLIFE","ABBOTINDIA",
    "SANOFI","PFIZER","GILLETTE","COLPAL","EMAMILTD","JYOTHYLAB","VBL","UBL","MCDOWELL-N",
    "RADICO","TATACOMM","INDUSTOWER","GMRINFRA","ADANIPOWER","ADANIGREEN","ADANIENSOL",
    "TATAPOWER","JSWENERGY","TORNTPOWER","CESC","NHPC","SJVN","INDIGO","SPICEJET","TRENT",
    "DMART","ABFRL","PAGEIND","BATAINDIA","RELAXO","METROBRAND","CAMPUS","KALYANKJIL",
    "TITAGARH","IRB","GMDCLTD","NBCC","NCC","HUDCO","IREDA","IRCON","PGHL","LODHA","OBEROIRLTY",
    "PRESTIGE","GODREJPROP","BRIGADE","SOBHA","PHOENIXLTD","MANAPPURAM","SUNDARMFIN",
    "M&MFIN","MAXHEALTH","FORTIS","NARAYANHRUD","APOLLOTYRE","CEATLTD","JKTYRE","BALRAMCHIN",
    "EIDPARRY","RAJESHEXPO","PATANJALI","IDEA","TATACHEM","DEEPAKFERT","COROMANDEL","CHAMBLFERT",
]
NIFTY500_STOCKS = sorted(set(NIFTY500_STOCKS))
SYMBOLS_TV = [f"NSE:{s}" for s in NIFTY500_STOCKS]

# Anonymous token. If you have a TradingView login and see delayed/rate
# limited data, replace with a real session auth token (extracted from
# your logged-in browser's websocket traffic) instead.
AUTH_TOKEN = "unauthorized_user_token"

TICK_VOLUME_THRESHOLD = 20_000     # shares in a single print to count as "unusual" — tune per price band
SYMBOLS_PER_SESSION = 50            # batch size per quote session (keeps subscribe messages small)
FEED_MAX_ROWS = 150

WS_URL = "wss://data.tradingview.com/socket.io/websocket"

_lock = threading.Lock()
_feed = deque(maxlen=FEED_MAX_ROWS)
_last_volume = {}          # symbol -> last known cumulative day volume
_last_push_ts = None

_historical_volume = {}

def _fetch_historical_volume():
    try:
        tickers = [s + ".NS" for s in NIFTY500_STOCKS]
        data = yf.download(tickers, period="5d", progress=False)
        if "Volume" in data:
            vol_data = data["Volume"].sum()
            with _lock:
                for symbol, vol in vol_data.items():
                    clean_sym = symbol.replace(".NS", "")
                    _historical_volume[clean_sym] = int(vol)
    except Exception as e:
        print("Failed to fetch historical volume:", e)

threading.Thread(target=_fetch_historical_volume, daemon=True).start()
_connected = False


# ---------------------------------------------------------------------------
# TradingView socket.io-style framing: ~m~<len>~m~<json>
# ---------------------------------------------------------------------------
def _frame(payload: dict) -> str:
    text = json.dumps(payload, separators=(",", ":"))
    return f"~m~{len(text)}~m~{text}"


def _gen_session_id(prefix: str) -> str:
    return prefix + "_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))


_FRAME_RE = re.compile(r"~m~(\d+)~m~")


def _split_frames(raw: str):
    """A single WS message can contain several concatenated ~m~len~m~json frames."""
    frames = []
    pos = 0
    while True:
        m = _FRAME_RE.match(raw, pos)
        if not m:
            break
        length = int(m.group(1))
        start = m.end()
        frames.append(raw[start:start + length])
        pos = start + length
    return frames


_last_state = {} # symbol -> {"price": None, "vol": None, "dir": 1, "buy_vol": 0, "sell_vol": 0, "buy_vol_1m": 0, "sell_vol_1m": 0, "minute": -1}

def _on_message(ws, message):
    global _last_push_ts
    for frame in _split_frames(message):
        if frame.startswith("~h~"):
            ws.send(_frame_raw_heartbeat(frame))
            continue
        try:
            data = json.loads(frame)
        except json.JSONDecodeError:
            continue

        if data.get("m") != "qsd":
            continue
        try:
            payload = data["p"][1]
            sym_full = payload["n"]
            values = payload.get("v", {})
        except (KeyError, IndexError, TypeError, ValueError):
            continue

        symbol = sym_full.split(":")[-1]
        with _lock:
            state = _last_state.get(symbol)
            curr_min = datetime.now(IST).minute
            
            if state is None:
                state = {"price": None, "vol": None, "dir": 1, "buy_vol": 0, "sell_vol": 0, "buy_vol_1m": 0, "sell_vol_1m": 0, "minute": curr_min, "chp": 0.0}
                _last_state[symbol] = state
            
            if state["minute"] != curr_min:
                state["buy_vol_1m"] = 0
                state["sell_vol_1m"] = 0
                state["minute"] = curr_min
            
            _last_push_ts = time.time()
            
            lp = values.get("lp")
            if lp is not None:
                try:
                    lp = float(lp)
                    if state["price"] is not None:
                        if lp > state["price"]:
                            state["dir"] = 1
                        elif lp < state["price"]:
                            state["dir"] = -1
                    state["price"] = lp
                except ValueError:
                    pass

            chp = values.get("chp")
            if chp is not None:
                try:
                    state["chp"] = float(chp)
                except ValueError:
                    pass

            vol = values.get("volume")
            if vol is not None:
                try:
                    vol = float(vol)
                    if state["vol"] is not None:
                        tick_size = vol - state["vol"]
                        if tick_size > 0:
                            if state["dir"] == 1:
                                state["buy_vol"] += tick_size
                                state["buy_vol_1m"] += tick_size
                            else:
                                state["sell_vol"] += tick_size
                                state["sell_vol_1m"] += tick_size
                            
                            if tick_size >= TICK_VOLUME_THRESHOLD:
                                entry = {
                                    "symbol": symbol,
                                    "price": float(state["price"]) if state["price"] else 0.0,
                                    "chp": state.get("chp", 0.0),
                                    "volume": int(tick_size),
                                    "cumulative_volume": int(vol),
                                    "time": datetime.now(IST).strftime("%H:%M:%S.%f")[:-3],
                                    "side": "BUY" if state["dir"] == 1 else "SELL"
                                }
                                _feed.appendleft(entry)
                    state["vol"] = vol
                except ValueError:
                    pass


def _frame_raw_heartbeat(frame: str) -> str:
    return f"~m~{len(frame)}~m~{frame}"


def _on_open(ws):
    global _connected
    _connected = True
    ws.send(_frame({"m": "set_auth_token", "p": [AUTH_TOKEN]}))

    for i in range(0, len(SYMBOLS_TV), SYMBOLS_PER_SESSION):
        batch = SYMBOLS_TV[i:i + SYMBOLS_PER_SESSION]
        session_id = _gen_session_id("qs")
        ws.send(_frame({"m": "quote_create_session", "p": [session_id]}))
        ws.send(_frame({
            "m": "quote_set_fields",
            "p": [session_id, "lp", "volume", "chp", "ch"],
        }))
        ws.send(_frame({"m": "quote_add_symbols", "p": [session_id, *batch]}))
        time.sleep(0.15)  # small stagger so TradingView doesn't see one giant burst


def _on_error(ws, error):
    print(f"[tick feed] websocket error: {error}")


def _on_close(ws, code, msg):
    global _connected
    _connected = False
    print(f"[tick feed] websocket closed ({code}: {msg}) — reconnecting in 5s")


def _run_forever():
    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            header=[
                "Origin: https://www.tradingview.com",
            ],
            on_open=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
        time.sleep(5)  # reconnect loop


threading.Thread(target=_run_forever, daemon=True).start()


@app.route("/api/feed")
def api_feed():
    with _lock:
        feed_snapshot = list(_feed)
        ts = _last_push_ts
        live = _connected
        stats = {}
        for sym, st in _last_state.items():
            if st.get("minute") == datetime.now(IST).minute and (st["buy_vol_1m"] > 0 or st["sell_vol_1m"] > 0):
                stats[sym] = {
                    "price": float(st["price"]) if st["price"] else 0.0,
                    "chp": float(st.get("chp", 0.0)),
                    "buy": int(st["buy_vol_1m"]),
                    "sell": int(st["sell_vol_1m"]),
                    "delta": int(st["buy_vol_1m"] - st["sell_vol_1m"]),
                    "vol_5d": _historical_volume.get(sym, 0),
                    "vol_today": int(st.get("vol") or 0)
                }
    return jsonify({
        "ok": True,
        "connected": live,
        "timestamp": datetime.fromtimestamp(ts, IST).strftime("%H:%M:%S") if ts else "",
        "feed": feed_snapshot,
        "stats": stats
    })


@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)


DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Tick Query Feed (live WS) — Nifty 500</title>
<script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
<style>
  :root { --bg:#0e1117; --panel:#161b22; --border:#2a2f3a; --text:#e6e6e6; --muted:#8a8f98; --accent:#f5a623; --live:#2ecc71; --dead:#ff5c5c; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--text); font-family:'Segoe UI',Roboto,Arial,sans-serif; }
  header { display:flex; align-items:center; gap:14px; padding:14px 20px; border-bottom:1px solid var(--border); }
  header h1 { font-size:16px; margin:0; font-weight:600; }
  .dot { width:8px; height:8px; border-radius:50%; display:inline-block; margin-right:6px; }
  .status { margin-left:auto; font-size:12px; color:var(--muted); display:flex; align-items:center; }
  
  .container { display:flex; gap:20px; max-width:1800px; margin:16px auto; align-items:flex-start; padding:0 16px; }
  .feed-wrap { flex:1; background:var(--panel); border:1px solid var(--border); border-radius:10px; overflow-x:auto; }
  .hot-wrap { flex:1.5; background:var(--panel); border:1px solid var(--border); border-radius:10px; overflow-x:auto; }
  
  h2 { font-size:14px; margin:0; padding:12px 16px; border-bottom:1px solid var(--border); background:rgba(255,255,255,0.03); }
  table { width:100%; border-collapse:collapse; font-size:14px; }
  th, td { padding:10px 16px; text-align:left; border-bottom:1px solid var(--border); }
  th { color:var(--muted); font-weight:500; font-size:11px; text-transform:uppercase; }
  td.vol { text-align:right; font-variant-numeric:tabular-nums; }
  td.time { color:var(--muted); font-size:12px; white-space:nowrap; }
  tr.new-row { animation: flash 1.2s ease-out; }
  @keyframes flash { from { background:rgba(245,166,35,.25); } to { background:transparent; } }
  .empty { padding:24px 16px; color:var(--muted); font-size:13px; text-align:center; }
  .hot-symbol { font-weight:bold; color:var(--accent); }
  
  .clickable-sym { cursor:pointer; color:var(--accent); text-decoration:none; transition: color 0.1s; }
  .clickable-sym:hover { color:#fff; text-decoration:underline; }
  
  .modal-overlay { display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.8); z-index:100; align-items:center; justify-content:center; backdrop-filter:blur(4px); }
  .modal-content { width:480px; background:var(--panel); border:1px solid var(--border); border-radius:12px; box-shadow: 0 16px 40px rgba(0,0,0,0.5); display:flex; flex-direction:column; overflow:hidden; }
  .modal-header { display:flex; justify-content:space-between; align-items:center; padding:20px 24px; border-bottom:1px solid rgba(255,255,255,0.05); }
  .modal-header h2 { font-size:20px; margin:0; padding:0; border:none; background:transparent; font-weight:600; color:#fff; }
  .close-btn { cursor:pointer; font-size:28px; color:var(--muted); border:none; background:none; line-height:1; transition:0.2s; }
  .close-btn:hover { color:var(--text); transform:scale(1.1); }
  .modal-body { padding:24px; display:flex; flex-direction:column; gap:20px; }
  
  .stat-grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
  .stat-card { background:rgba(255,255,255,0.02); border:1px solid var(--border); border-radius:8px; padding:16px; text-align:center; }
  .stat-title { font-size:12px; color:var(--muted); text-transform:uppercase; font-weight:500; letter-spacing:0.5px; margin-bottom:8px; }
  .stat-value { font-size:24px; font-weight:700; font-variant-numeric:tabular-nums; }
  .stat-value.buy { color:var(--live); }
  .stat-value.sell { color:var(--dead); }
  .stat-value.delta.pos { color:var(--live); }
  .stat-value.delta.neg { color:var(--dead); }
  .stat-value.delta.neu { color:var(--text); }
  
  .modal-footer { padding:20px 24px; border-top:1px solid rgba(255,255,255,0.05); text-align:center; }
  .chart-btn { display:inline-flex; align-items:center; gap:8px; background:var(--accent); color:#000; padding:12px 24px; border-radius:6px; font-weight:600; text-decoration:none; transition:0.2s; }
  .chart-btn:hover { opacity:0.9; transform:translateY(-1px); }
</style>
</head>
<body>
<header>
  <h1>Tick Query Feed — Live WebSocket (Nifty 500)</h1>
  <span class="status" id="statusText"><span class="dot" id="statusDot" style="background:var(--muted)"></span>connecting…</span>
</header>

<div class="container">
  <div class="feed-wrap">
    <h2>Live Tick Feed</h2>
    <table>
      <thead><tr><th>Time</th><th>Symbol</th><th style="text-align:right">Price</th><th style="text-align:right">% Chg</th><th>Side</th><th style="text-align:right">Tick Size</th></tr></thead>
      <tbody id="feedBody"><tr><td colspan="6" class="empty">Watching the tape…</td></tr></tbody>
    </table>
  </div>
  
  <div class="hot-wrap">
    <h2>Continuously Detected (Current 1m Candle)</h2>
    <table>
      <thead><tr><th>Symbol</th><th style="text-align:right">Price</th><th style="text-align:right">% Chg</th><th style="text-align:right">Prints</th><th style="text-align:right">Buy Vol</th><th style="text-align:right">Sell Vol</th><th style="text-align:right">Delta</th><th style="text-align:right">5D Vol</th><th style="text-align:right">Curr Vol</th><th style="text-align:right">Vol Delta</th></tr></thead>
      <tbody id="hotBody"><tr><td colspan="10" class="empty">None yet</td></tr></tbody>
    </table>
  </div>
</div>

<div class="modal-overlay" id="detailModal" onclick="if(event.target===this) closeChart()">
  <div class="modal-content">
    <div class="modal-header">
      <h2 id="modalTitle">Symbol</h2>
      <button class="close-btn" onclick="closeChart()">&times;</button>
    </div>
    <div class="modal-body">
      <div style="text-align:center; color:var(--muted); font-size:13px; margin-bottom:8px;">
        Last Updated: <span id="modalTime" style="color:var(--text);font-weight:500;">--:--:--</span>
      </div>
      <div class="stat-grid">
        <div class="stat-card">
          <div class="stat-title">Buy Volume (1m)</div>
          <div class="stat-value buy" id="modalBuy">0</div>
        </div>
        <div class="stat-card">
          <div class="stat-title">Sell Volume (1m)</div>
          <div class="stat-value sell" id="modalSell">0</div>
        </div>
        <div class="stat-card" style="grid-column: span 2;">
          <div class="stat-title">Volume Delta</div>
          <div class="stat-value delta neu" id="modalDelta">0</div>
        </div>
      </div>
    </div>
    <div class="modal-footer">
      <a href="#" target="_blank" class="chart-btn" id="modalChartBtn">Open TradingView Chart &#8599;</a>
    </div>
  </div>
</div>

<script>
let knownKeys = new Set();
let activeModalSymbol = null;

function openChart(symbol) {
  activeModalSymbol = symbol;
  const modal = document.getElementById('detailModal');
  modal.style.display = 'flex';
  
  document.getElementById('modalTitle').innerText = symbol + " (1m Tape)";
  document.getElementById('modalChartBtn').href = `https://in.tradingview.com/chart/?symbol=NSE:${symbol}`;
  
  // Instantly trigger a poll to populate data without waiting 1s
  poll();
}

function closeChart() {
  activeModalSymbol = null;
  document.getElementById('detailModal').style.display = 'none';
}

function fmtVol(v) {
  if (v === undefined || v === null) return 0;
  let sign = v < 0 ? '-' : '';
  let absV = Math.abs(v);
  if (absV >= 1e7) return sign + (absV/1e7).toFixed(2) + 'Cr';
  if (absV >= 1e5) return sign + (absV/1e5).toFixed(2) + 'L';
  if (absV >= 1e3) return sign + (absV/1e3).toFixed(1) + 'K';
  return sign + absV;
}

async function poll() {
  try {
    const res = await fetch('/api/feed');
    const data = await res.json();
    if (!data.ok) throw new Error('feed error');

    document.getElementById('statusText').innerHTML =
      `<span class="dot" id="statusDot" style="background:${data.connected ? 'var(--live)' : 'var(--dead)'}"></span>` +
      (data.connected ? `live • last tick ${data.timestamp || '—'}` : 'reconnecting…');

    const body = document.getElementById('feedBody');
    const hotBody = document.getElementById('hotBody');
    
    if (!data.feed.length) {
      body.innerHTML = '<tr><td colspan="5" class="empty">Watching the tape…</td></tr>';
      hotBody.innerHTML = '<tr><td colspan="6" class="empty">None yet</td></tr>';
    } else {
      let counts = {};
      body.innerHTML = data.feed.map(r => {
        counts[r.symbol] = (counts[r.symbol] || 0) + 1;
        const key = r.symbol + r.time;
        const isNew = !knownKeys.has(key);
        knownKeys.add(key);
        return `<tr class="${isNew ? 'new-row' : ''}">
          <td class="time">${r.time}</td>
          <td class="clickable-sym" onclick="openChart('${r.symbol}')">${r.symbol}</td>
          <td class="vol">${r.price ? r.price.toFixed(2) : "0.00"}</td>
          <td class="vol" style="color:${r.chp >= 0 ? 'var(--live)' : 'var(--dead)'}">${r.chp ? (r.chp > 0 ? '+' : '') + r.chp.toFixed(2) + '%' : '0.00%'}</td>
          <td class="${r.side === 'BUY' ? 'live' : (r.side === 'SELL' ? 'dead' : '')}" style="font-size:12px;font-weight:bold;">${r.side || ''}</td>
          <td class="vol">${fmtVol(r.volume)}</td>
        </tr>`;
      }).join('');
      
      let hotArr = Object.entries(counts).filter(e => e[1] > 1).sort((a,b) => b[1] - a[1]);
      if (hotArr.length === 0) {
        hotBody.innerHTML = '<tr><td colspan="10" class="empty">None yet</td></tr>';
      } else {
        hotBody.innerHTML = hotArr.map(e => {
          let sym = e[0];
          let c = e[1];
          let stat = data.stats[sym] || {buy:0, sell:0, delta:0, price:0};
          let vol_5d = stat.vol_5d || 0;
          let vol_today = stat.vol_today || 0;
          let vol_delta = vol_today - vol_5d;
          let deltaColor = stat.delta > 0 ? 'var(--live)' : (stat.delta < 0 ? 'var(--dead)' : 'var(--text)');
          let volDeltaColor = vol_delta > 0 ? 'var(--live)' : (vol_delta < 0 ? 'var(--dead)' : 'var(--text)');
          return `
          <tr>
            <td class="hot-symbol clickable-sym" onclick="openChart('${sym}')">${sym}</td>
            <td class="vol">${stat.price ? stat.price.toFixed(2) : "0.00"}</td>
            <td class="vol" style="color:${(stat.chp || 0) >= 0 ? 'var(--live)' : 'var(--dead)'}">${stat.chp ? (stat.chp > 0 ? '+' : '') + stat.chp.toFixed(2) + '%' : '0.00%'}</td>
            <td class="vol">${c}</td>
            <td class="vol" style="color:var(--live)">${fmtVol(stat.buy)}</td>
            <td class="vol" style="color:var(--dead)">${fmtVol(stat.sell)}</td>
            <td class="vol" style="color:${deltaColor};font-weight:bold">${fmtVol(stat.delta)}</td>
            <td class="vol">${fmtVol(vol_5d)}</td>
            <td class="vol">${fmtVol(vol_today)}</td>
            <td class="vol" style="color:${volDeltaColor};font-weight:bold">${fmtVol(vol_delta)}</td>
          </tr>
          `;
        }).join('');
      }
      
      // Update active modal if open
      if (activeModalSymbol) {
        let stat = data.stats[activeModalSymbol] || {buy:0, sell:0, delta:0, price:0};
        
        document.getElementById('modalTitle').innerText = activeModalSymbol + " @ ₹" + (stat.price ? stat.price.toFixed(2) : "0.00");
        
        document.getElementById('modalBuy').innerText = fmtVol(stat.buy);
        document.getElementById('modalSell').innerText = fmtVol(stat.sell);
        
        const deltaEl = document.getElementById('modalDelta');
        deltaEl.innerText = fmtVol(stat.delta);
        deltaEl.className = 'stat-value delta ' + (stat.delta > 0 ? 'pos' : (stat.delta < 0 ? 'neg' : 'neu'));
        
        document.getElementById('modalTime').innerText = data.timestamp || '--:--:--';
      }
    }
  } catch (e) {
    document.getElementById('statusText').textContent = 'error: ' + e.message;
  }
}

poll();
setInterval(poll, 1000);   // just re-reads the in-memory feed — ticks arrive independently of this
</script>
</body>
</html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5003))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
