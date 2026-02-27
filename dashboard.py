"""
PolyTrading — Weather Markets Web Dashboard
============================================
Runs the Weather collection pipeline as a background asyncio task
and serves a live web dashboard on http://localhost:8765
"""
import asyncio
import os
import sys
import time

# ── Bootstrap weather package ────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_ROOT, 'weather'))

import weather_shared_state as state
from weather_ws_client import main_daemon as weather_clob_daemon

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager
import uvicorn

app = FastAPI()

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_weather_dir_size():
    path = os.path.join(_ROOT, "data", "weather")
    total = 0
    if not os.path.exists(path):
        return 0.0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                if not os.path.islink(fp):
                    total += os.path.getsize(fp)
            except OSError:
                pass
    return round(total / (1024 * 1024), 2)

# ── Lifecycle ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Run weather daemon in the background
    task = asyncio.create_task(weather_clob_daemon())
    yield
    # Shutdown: Clean up task
    task.cancel()

app = FastAPI(lifespan=lifespan)

# ── API ───────────────────────────────────────────────────────────────────────

CITY_ORDER = ["london","seoul","nyc","toronto","wellington",
              "atlanta","chicago","seattle","buenos-aires","miami"]

@app.get("/api/stats")
async def get_stats():
    now = time.time()
    elapsed = int(now - state.state['start_time'])
    uptime_h, rem = divmod(elapsed, 3600)
    uptime_m, uptime_s = divmod(rem, 60)

    markets = []
    for cid, info in state.state.get('markets', {}).items():
        markets.append({
            "condition_id": cid,
            "city": info.get("city", "").lower(),
            "question": info.get("question", ""),
            "end_date": str(info.get("end_date", "")),
            "trades": info.get("trades", 0),
        })

    # Sort by city order then question
    def sort_key(market_entry):
        try:
            ci = CITY_ORDER.index(market_entry["city"])
        except ValueError:
            ci = 99
        return (ci, market_entry["question"])
    markets.sort(key=sort_key)

    # Group by city for counts
    city_counts = {}
    for market_item in markets:
        city_counts[market_item["city"]] = city_counts.get(market_item["city"], 0) + 1

    return JSONResponse({
        "uptime": f"{uptime_h:02d}:{uptime_m:02d}:{uptime_s:02d}",
        "mb_saved": get_weather_dir_size(),
        "slugs_active": state.state["slugs_active"],
        "trades": state.state["polymarket_trades"],
        "ticks": state.state["polymarket_ticks"],
        "snapshots": state.state["polymarket_snapshots"],
        "next_flush": max(0, int(state.state["next_flush_time"] - now)),
        "next_slug": max(0, int(state.state["next_slug_update"] - now)),
        "markets": markets,
        "city_counts": city_counts,
    })

# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PolyTrading — Weather Markets Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:         #060c0a;
    --surface:    #0b1310;
    --card:       #0f1a17;
    --border:     #1a2b25;
    --border-hi:  #2a4a3a;
    --text:       #d8f0e8;
    --muted:      #4a7060;
    --green:      #00e5aa;
    --green-dim:  #00e5aa18;
    --green-med:  #00e5aa40;
    --teal:       #00bcd4;
    --teal-dim:   #00bcd418;
    --orange:     #ffab40;
    --orange-dim: #ffab4018;
    --yellow:     #ffd740;
    --red:        #ff5252;
    --white:      #ffffff;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif; }

  /* ════ LAYOUT ════ */
  .app { display: grid; grid-template-rows: 60px auto 1fr; height: 100vh; overflow: hidden; }

  /* ════ HEADER ════ */
  header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 0 28px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    position: relative; flex-shrink: 0;
  }
  header::after {
    content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg, transparent, var(--green), transparent);
    opacity: 0.5;
  }
  .logo { display: flex; align-items: center; gap: 12px; }
  .logo-icon { font-size: 20px; }
  .logo-text { font-size: 16px; font-weight: 700; letter-spacing: 0.02em; }
  .logo-text em { font-style: normal; color: var(--green); }
  .logo-tag {
    font-size: 10px; font-weight: 600; letter-spacing: 0.12em; text-transform: uppercase;
    color: var(--green); background: var(--green-dim); border: 1px solid var(--green-med);
    padding: 2px 8px; border-radius: 20px; margin-left: 4px;
  }
  .header-right { display: flex; align-items: center; gap: 24px; }
  .hlabel { font-size: 11px; color: var(--muted); }
  .hval { font-family: 'JetBrains Mono', monospace; font-size: 14px; font-weight: 600; color: var(--text); }
  .live-pill {
    display: flex; align-items: center; gap: 7px;
    background: var(--green-dim); border: 1px solid var(--green-med);
    border-radius: 20px; padding: 4px 12px;
    font-size: 11px; font-weight: 600; color: var(--green);
  }
  .live-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--green); box-shadow: 0 0 6px var(--green); animation: blink 2s infinite; }
  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.3} }

  /* ════ METRICS STRIP ════ */
  .metrics-strip {
    display: grid;
    grid-template-columns: repeat(7, 1fr);
    gap: 0;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }
  .metric {
    padding: 14px 20px;
    border-right: 1px solid var(--border);
  }
  .metric:last-child { border-right: none; }
  .metric-label { font-size: 10px; color: var(--muted); font-weight: 500; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 6px; }
  .metric-val { font-family: 'JetBrains Mono', monospace; font-size: 22px; font-weight: 700; color: var(--green); line-height: 1; }
  .metric-val.orange { color: var(--orange); }
  .metric-val.teal   { color: var(--teal); }
  .metric-sub { font-size: 10px; color: var(--muted); margin-top: 4px; }

  /* ════ BODY ════ */
  .body { display: grid; grid-template-columns: 220px 1fr; overflow: hidden; }

  /* ── Sidebar ── */
  .sidebar {
    border-right: 1px solid var(--border);
    overflow-y: auto; padding: 16px 0;
    background: var(--surface);
  }
  .sidebar::-webkit-scrollbar { width: 3px; }
  .sidebar::-webkit-scrollbar-thumb { background: var(--border-hi); border-radius: 2px; }
  .sidebar-title {
    font-size: 9px; font-weight: 700; letter-spacing: 0.14em; text-transform: uppercase;
    color: var(--muted); padding: 0 16px 8px;
  }
  .city-item {
    display: flex; align-items: center; justify-content: space-between;
    padding: 8px 16px; cursor: pointer; border-left: 2px solid transparent;
    transition: all 0.15s;
  }
  .city-item:hover, .city-item.active {
    background: var(--green-dim); border-left-color: var(--green);
  }
  .city-item.active .city-name { color: var(--green); }
  .city-name { font-size: 13px; font-weight: 500; text-transform: capitalize; }
  .city-count {
    font-family: 'JetBrains Mono', monospace; font-size: 11px;
    background: var(--card); color: var(--muted);
    padding: 1px 7px; border-radius: 20px; border: 1px solid var(--border);
    transition: all 0.15s;
  }
  .city-item.active .city-count { background: var(--green-med); color: var(--green); border-color: var(--green); }

  /* ── Main table ── */
  .table-area { display: flex; flex-direction: column; overflow: hidden; }
  .table-toolbar {
    display: flex; align-items: center; justify-content: space-between;
    padding: 10px 20px; border-bottom: 1px solid var(--border);
    background: var(--surface); flex-shrink: 0;
  }
  .toolbar-left { display: flex; align-items: center; gap: 12px; }
  .showing-label { font-size: 12px; color: var(--muted); }
  .showing-label strong { color: var(--text); }
  .search-box {
    background: var(--card); border: 1px solid var(--border); border-radius: 7px;
    color: var(--text); font-family: 'Inter', sans-serif; font-size: 12px;
    padding: 5px 12px; outline: none; width: 220px;
    transition: border-color 0.2s;
  }
  .search-box:focus { border-color: var(--green); }
  .search-box::placeholder { color: var(--muted); }

  .table-scroll { flex: 1; overflow-y: auto; }
  .table-scroll::-webkit-scrollbar { width: 5px; }
  .table-scroll::-webkit-scrollbar-track { background: transparent; }
  .table-scroll::-webkit-scrollbar-thumb { background: var(--border-hi); border-radius: 3px; }

  table { width: 100%; border-collapse: collapse; }
  thead th {
    padding: 10px 16px; text-align: left; font-size: 10px; font-weight: 600;
    letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted);
    background: var(--surface); position: sticky; top: 0; z-index: 10;
    border-bottom: 1px solid var(--border);
  }
  tbody tr { border-bottom: 1px solid #0f1f18; transition: background 0.12s; }
  tbody tr:hover { background: #111f19; }
  td { padding: 10px 16px; vertical-align: middle; }

  .city-pill {
    display: inline-block; font-family: 'JetBrains Mono', monospace;
    font-size: 10px; font-weight: 700; padding: 3px 9px; border-radius: 5px;
    background: var(--green-dim); color: var(--green); border: 1px solid var(--green-med);
    white-space: nowrap;
  }
  .question-text {
    font-size: 12px; color: var(--text); max-width: 380px;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }
  .date-text { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--muted); white-space: nowrap; }
  .trade-val { font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 600; color: var(--text); }
  .trade-val.nonzero { color: var(--green); }

  .empty-row td { text-align: center; padding: 40px; color: var(--muted); font-size: 13px; }
</style>
</head>
<body>
<div class="app">

  <header>
    <div class="logo">
      <div class="logo-icon">🌡</div>
      <div class="logo-text">Poly<em>Trading</em>  <span class="logo-tag">Weather</span></div>
    </div>
    <div class="header-right">
      <div>
        <div class="hlabel">Uptime</div>
        <div class="hval" id="uptime">00:00:00</div>
      </div>
      <div>
        <div class="hlabel">Volume Saved</div>
        <div class="hval" id="mb-saved">0.00 MB</div>
      </div>
      <div>
        <div class="hlabel">Last update</div>
        <div class="hval" id="last-update">—</div>
      </div>
      <div class="live-pill">
        <div class="live-dot" id="live-dot"></div>
        LIVE
      </div>
    </div>
  </header>

  <div class="metrics-strip">
    <div class="metric">
      <div class="metric-label">Active Buckets</div>
      <div class="metric-val" id="m-slugs">0</div>
      <div class="metric-sub">Subscribed tokens</div>
    </div>
    <div class="metric">
      <div class="metric-label">CLOB Trades</div>
      <div class="metric-val" id="m-trades">0</div>
      <div class="metric-sub">last_trade_price</div>
    </div>
    <div class="metric">
      <div class="metric-label">Tick Updates</div>
      <div class="metric-val teal" id="m-ticks">0</div>
      <div class="metric-sub">price_change</div>
    </div>
    <div class="metric">
      <div class="metric-label">Book Snapshots</div>
      <div class="metric-val teal" id="m-snaps">0</div>
      <div class="metric-sub">L2 orderbook</div>
    </div>
    <div class="metric">
      <div class="metric-label">Cities Tracked</div>
      <div class="metric-val" id="m-cities">0</div>
      <div class="metric-sub">of 10 configured</div>
    </div>
    <div class="metric">
      <div class="metric-label">Next Flush</div>
      <div class="metric-val orange" id="m-flush">—</div>
      <div class="metric-sub">seconds</div>
    </div>
    <div class="metric">
      <div class="metric-label">Next Slug Rot.</div>
      <div class="metric-val orange" id="m-slug">—</div>
      <div class="metric-sub">seconds</div>
    </div>
  </div>

  <div class="body">
    <div class="sidebar">
      <div class="sidebar-title">Filter by City</div>
      <div class="city-item active" data-city="all" onclick="setCity('all')">
        <span class="city-name">All Cities</span>
        <span class="city-count" id="count-all">0</span>
      </div>
      <div id="city-list"></div>
    </div>

    <div class="table-area">
      <div class="table-toolbar">
        <div class="toolbar-left">
          <span class="showing-label">Showing <strong id="showing-count">0</strong> sub-markets</span>
        </div>
        <input class="search-box" id="search" placeholder="Filter questions…" oninput="renderTable()">
      </div>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th style="width:90px">City</th>
              <th>Bucket Question</th>
              <th style="width:160px">Resolution (UTC)</th>
              <th style="width:80px">Trades</th>
            </tr>
          </thead>
          <tbody id="market-tbody">
            <tr class="empty-row"><td colspan="4">Connecting to Polymarket CLOB…</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
const fmt = n => Number(n).toLocaleString();
let allMarkets = [];
let cityCounts = {};
let activeCity = 'all';

const CITY_NAMES = {
  'london':'London','seoul':'Seoul','nyc':'New York','toronto':'Toronto',
  'wellington':'Wellington','atlanta':'Atlanta','chicago':'Chicago',
  'seattle':'Seattle','buenos-aires':'Buenos Aires','miami':'Miami'
};

function setCity(c) {
  activeCity = c;
  document.querySelectorAll('.city-item').forEach(el => {
    el.classList.toggle('active', el.dataset.city === c);
  });
  renderTable();
}

function renderSidebar() {
  const cities = Object.keys(cityCounts).sort((a,b) => {
    const order = ['london','seoul','nyc','toronto','wellington','atlanta','chicago','seattle','buenos-aires','miami'];
    return (order.indexOf(a)||99) - (order.indexOf(b)||99);
  });

  document.getElementById('count-all').textContent = allMarkets.length;

  const html = cities.map(c => `
    <div class="city-item${activeCity===c?' active':''}" data-city="${c}" onclick="setCity('${c}')">
      <span class="city-name">${CITY_NAMES[c]||c}</span>
      <span class="city-count" id="count-${c}">${cityCounts[c]||0}</span>
    </div>
  `).join('');

  // Only re-render if city set changed to avoid flicker
  const existing = document.getElementById('city-list');
  const existingCities = [...existing.querySelectorAll('.city-item')].map(el=>el.dataset.city);
  if (JSON.stringify(existingCities.sort()) !== JSON.stringify(cities.sort())) {
    existing.innerHTML = html;
    // Re-attach active class
    document.querySelectorAll('.city-item').forEach(el => {
      el.classList.toggle('active', el.dataset.city === activeCity);
    });
  } else {
    // Just update counts
    cities.forEach(c => {
      const el = document.getElementById('count-' + c);
      if (el) el.textContent = cityCounts[c]||0;
    });
  }
}

function renderTable() {
  const query = document.getElementById('search').value.toLowerCase();

  let filtered = allMarkets;
  if (activeCity !== 'all') {
    filtered = filtered.filter(m => m.city === activeCity);
  }
  if (query) {
    filtered = filtered.filter(m => m.question.toLowerCase().includes(query));
  }

  document.getElementById('showing-count').textContent = filtered.length;

  if (filtered.length === 0) {
    document.getElementById('market-tbody').innerHTML =
      '<tr class="empty-row"><td colspan="4">' +
      (allMarkets.length === 0 ? 'Fetching weather markets…' : 'No markets match the filter.') +
      '</td></tr>';
    return;
  }

  document.getElementById('market-tbody').innerHTML = filtered.map(m => {
    const cityLabel = CITY_NAMES[m.city] || m.city.toUpperCase();
    const date = m.end_date.slice(0,19).replace('T',' ');
    const tradeClass = m.trades > 0 ? ' nonzero' : '';
    return `<tr>
      <td><span class="city-pill">${cityLabel}</span></td>
      <td><div class="question-text" title="${m.question}">${m.question}</div></td>
      <td><span class="date-text">${date}</span></td>
      <td><span class="trade-val${tradeClass}">${fmt(m.trades)}</span></td>
    </tr>`;
  }).join('');
}

async function refresh() {
  let data;
  try {
    const r = await fetch('/api/stats');
    data = await r.json();
    document.getElementById('live-dot').style.background = 'var(--green)';
  } catch(e) {
    document.getElementById('live-dot').style.background = 'var(--red)';
    return;
  }

  // Header
  document.getElementById('uptime').textContent = data.uptime;
  document.getElementById('mb-saved').textContent = data.mb_saved + ' MB';
  document.getElementById('last-update').textContent = new Date().toTimeString().slice(0,8);

  // Metrics
  document.getElementById('m-slugs').textContent = fmt(data.slugs_active);
  document.getElementById('m-trades').textContent = fmt(data.trades);
  document.getElementById('m-ticks').textContent = fmt(data.ticks);
  document.getElementById('m-snaps').textContent = fmt(data.snapshots);
  document.getElementById('m-cities').textContent = Object.keys(data.city_counts).length;
  document.getElementById('m-flush').textContent = data.next_flush + 's';
  document.getElementById('m-slug').textContent = data.next_slug + 's';

  // Table
  allMarkets = data.markets;
  cityCounts = data.city_counts;
  renderSidebar();
  renderTable();
}

refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(HTML)

# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 56)
    print("  PolyTrading — Weather Markets Dashboard")
    print("  Open: http://localhost:8765")
    print("=" * 56)
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="warning")
