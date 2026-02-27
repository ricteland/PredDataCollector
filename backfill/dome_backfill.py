"""
Dome API Historical Backfill Script
====================================
Downloads all available historical Polymarket crypto market data
by generating historical slugs using the same naming convention
as fetch_tokens.js, then querying Dome API for trades, orderbooks,
and OHLCV candlesticks.

Usage:
  set DOME_API_KEY=your_key_here
  python dome_backfill.py
"""

import os
import sys
import time
import json
import requests
import datetime
import pandas as pd

# ─── Configuration ───────────────────────────────────────────────────────────

DOME_API_KEY = "f2e46c2395d9d74419feea87eae520cafbe44eaa"
BASE_URL = "https://api.domeapi.io/v1"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RATE_LIMIT_SLEEP = 0.15  # ~7 QPS to stay under the 10 QPS free tier

# How many days back to generate slugs for
DAYS_BACK = 120  # ~4 months of historical data

COINS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
}

# ─── API Helpers ─────────────────────────────────────────────────────────────

def dome_get(endpoint, params=None, retries=3):
    """Make a rate-limited GET request to the Dome API with automatic retries for 50x errors."""
    headers = {"Authorization": f"Bearer {DOME_API_KEY}"}
    url = f"{BASE_URL}{endpoint}"
    
    time.sleep(RATE_LIMIT_SLEEP)
    
    resp = None
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        status = resp.status_code if resp is not None else 500
        
        # Handle 429 Too Many Requests
        if status == 429:
            print(f"  [RATE LIMITED] Sleeping 10s...")
            time.sleep(10)
            return dome_get(endpoint, params, retries)
            
        # Handle 404/400 gracefully
        if status == 404 or status == 400:
            return None  # Market doesn't exist or bad slug, silently skip
            
        # Handle 502 Bad Gateway / 504 Gateway Timeout
        if status in [500, 502, 503, 504] and retries > 0:
            # Backoff before retrying
            sleep_time = (4 - retries) * 2
            print(f"  [SERVER ERROR {status}] Retrying in {sleep_time}s... ({retries-1} left) -> {params.get('market_slug', '...')}")
            time.sleep(sleep_time)
            return dome_get(endpoint, params, retries - 1)
            
        print(f"  [HTTP ERROR] {e}")
        return None
    except Exception as e:
        if retries > 0:
            print(f"  [NETWORK ERROR] Retrying in 2s... ({retries-1} left)")
            time.sleep(2)
            return dome_get(endpoint, params, retries - 1)
        print(f"  [ERROR] {e}")
        return None


# ─── Slug Generation (Mirrors fetch_tokens.js logic) ────────────────────────

def generate_1h_slugs(coin_long, target_date):
    """
    Generate all 24 hourly slug permutations for a given date.
    Pattern: {coin}-up-or-down-{month}-{day}-{hour}{ampm}-et
    Example: bitcoin-up-or-down-february-23-3pm-et
    """
    slugs = []
    month_name = target_date.strftime("%B").lower()  # "february"
    day = str(target_date.day)  # "23" (no leading zero)
    
    for hour_24 in range(24):
        if hour_24 == 0:
            hour_str = "12"
            ampm = "am"
        elif hour_24 < 12:
            hour_str = str(hour_24)
            ampm = "am"
        elif hour_24 == 12:
            hour_str = "12"
            ampm = "pm"
        else:
            hour_str = str(hour_24 - 12)
            ampm = "pm"
        
        slug = f"{coin_long}-up-or-down-{month_name}-{day}-{hour_str}{ampm}-et"
        slugs.append(slug)
    
    return slugs


def generate_15m_slug_prefixes(coin_short):
    """
    Generate the 15m slug prefix pattern.
    Pattern: {coin}-updown-15m-...  (we'll search by market_slug containing this)
    """
    return f"{coin_short.lower()}-updown-15m"


def generate_5m_slug_prefixes(coin_short):
    """
    Generate the 5m slug prefix pattern.
    """
    return f"{coin_short.lower()}-updown-5m"


# ─── Market Discovery via Slugs ─────────────────────────────────────────────

def discover_markets_by_slugs():
    """
    Generate all historical slugs for the past N days and verify
    which ones actually exist on Dome API.
    """
    all_markets = []
    today = datetime.date.today()
    
    for coin_long, coin_short in COINS.items():
        print(f"\n{'='*60}")
        print(f"  DISCOVERING {coin_short} MARKETS ({DAYS_BACK} days back)")
        print(f"{'='*60}")
        
        # ── 1h Markets ──
        found_1h = 0
        missed_1h = 0
        
        for days_ago in range(DAYS_BACK):
            target_date = today - datetime.timedelta(days=days_ago)
            slugs = generate_1h_slugs(coin_long, target_date)
            
            for slug in slugs:
                # Try to fetch market info from Dome
                data = dome_get("/polymarket/markets", {"market_slug": slug, "limit": 1})
                if not data:
                    missed_1h += 1
                    continue
                
                markets = data.get("markets", [])
                if not markets:
                    missed_1h += 1
                    continue
                
                m = markets[0]
                all_markets.append({
                    "coin": coin_short,
                    "timeframe": "1h",
                    "slug": slug,
                    "condition_id": m.get("condition_id", ""),
                    "end_time": m.get("end_time", ""),
                    "tokens": m.get("tokens", []),
                    "title": m.get("title", ""),
                })
                found_1h += 1
            
            if days_ago % 7 == 0 and days_ago > 0:
                print(f"  [1h] Day -{days_ago}: {found_1h} found, {missed_1h} missed")
        
        print(f"  [1h DONE] Total: {found_1h} markets found")
        
        # ── 15m and 5m Markets (discover via tags + pagination) ──
        for tf, tag_prefix in [("15m", generate_15m_slug_prefixes(coin_short)), 
                                ("5m", generate_5m_slug_prefixes(coin_short))]:
            print(f"\n  [DISCOVERING {tf} markets for {coin_short}...]")
            pagination_key = None
            found_tf = 0
            
            while True:
                params = {"tags": "Crypto", "limit": 100}
                if pagination_key:
                    params["pagination_key"] = pagination_key
                
                data = dome_get("/polymarket/markets", params)
                if not data:
                    break
                
                markets = data.get("markets", [])
                if not markets:
                    break
                
                for m in markets:
                    slug = m.get("market_slug", "")
                    if tag_prefix in slug:
                        all_markets.append({
                            "coin": coin_short,
                            "timeframe": tf,
                            "slug": slug,
                            "condition_id": m.get("condition_id", ""),
                            "end_time": m.get("end_time", ""),
                            "tokens": m.get("tokens", []),
                            "title": m.get("title", ""),
                        })
                        found_tf += 1
                
                pagination = data.get("pagination", {})
                next_cursor = pagination.get("next_cursor")
                if not next_cursor:
                    break
                pagination_key = next_cursor
            
            print(f"  [{tf} DONE] Total: {found_tf} markets found")
    
    print(f"\n[DISCOVERY COMPLETE] Total markets: {len(all_markets)}")
    return all_markets


# ─── Backfill Trade History ──────────────────────────────────────────────────

def backfill_trades(market):
    """Download all historical trades for a single market."""
    slug = market["slug"]
    coin = market["coin"]
    tf = market["timeframe"]
    
    all_trades = []
    pagination_key = None
    pages = 0
    
    while True:
        params = {"market_slug": slug, "limit": 1000}
        if pagination_key:
            params["pagination_key"] = pagination_key
        
        data = dome_get("/polymarket/orders", params)
        if not data:
            break
        
        orders = data.get("orders", data.get("data", []))
        if not orders:
            break
        
        for order in orders:
            all_trades.append({
                "timestamp": order.get("timestamp", ""),
                "token_id": order.get("token_id", ""),
                "token_label": order.get("token_label", ""),
                "side": order.get("side", ""),
                "price": float(order.get("price", 0)),
                "size": float(order.get("shares_normalized", order.get("shares", 0))),
            })
        
        pages += 1
        pagination = data.get("pagination", {})
        next_cursor = pagination.get("pagination_key")
        has_more = pagination.get("has_more", False)
        if not next_cursor or not has_more:
            break
        pagination_key = next_cursor
    
    if not all_trades:
        return 0
    
    df = pd.DataFrame(all_trades)
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception:
        pass
    
    if "timestamp" in df.columns and pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        for date_str, group in df.groupby("date"):
            base_dir = os.path.join(DATA_DIR, coin, tf, slug, date_str)
            os.makedirs(base_dir, exist_ok=True)
            out_path = os.path.join(base_dir, "backfill_trades.parquet")
            if not os.path.exists(out_path):
                group.drop(columns=["date"]).to_parquet(out_path, index=False)
    else:
        base_dir = os.path.join(DATA_DIR, coin, tf, slug, "backfill")
        os.makedirs(base_dir, exist_ok=True)
        df.to_parquet(os.path.join(base_dir, "backfill_trades.parquet"), index=False)
    
    return len(all_trades)


# ─── Backfill L2 Orderbook Snapshots ─────────────────────────────────────────

def backfill_orderbook(market):
    """Download L2 orderbook snapshots (available from Oct 14, 2025)."""
    slug = market["slug"]
    coin = market["coin"]
    tf = market["timeframe"]
    tokens = market.get("tokens", [])
    
    if not tokens:
        return 0
    
    token_id = tokens[0].get("token_id", "") if isinstance(tokens[0], dict) else str(tokens[0])
    if not token_id:
        return 0
    
    all_snapshots = []
    pagination_key = None
    start_ms = int(datetime.datetime(2025, 10, 14, tzinfo=datetime.timezone.utc).timestamp() * 1000)
    
    while True:
        params = {"token_id": token_id, "start_time": start_ms, "limit": 100}
        if pagination_key:
            params["pagination_key"] = pagination_key
        
        data = dome_get("/polymarket/orderbooks", params)
        if not data:
            break
        
        snapshots = data.get("snapshots", data.get("data", []))
        if not snapshots:
            break
        
        for snap in snapshots:
            all_snapshots.append({
                "timestamp": snap.get("timestamp", 0),
                "bids": json.dumps(snap.get("bids", [])),
                "asks": json.dumps(snap.get("asks", [])),
            })
        
        pagination = data.get("pagination", {})
        next_cursor = pagination.get("next_cursor")
        if not next_cursor:
            break
        pagination_key = next_cursor
    
    if not all_snapshots:
        return 0
    
    df = pd.DataFrame(all_snapshots)
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    except Exception:
        pass
    
    if "timestamp" in df.columns and pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        for date_str, group in df.groupby("date"):
            base_dir = os.path.join(DATA_DIR, coin, tf, slug, date_str)
            os.makedirs(base_dir, exist_ok=True)
            out_path = os.path.join(base_dir, "backfill_snapshots.parquet")
            if not os.path.exists(out_path):
                group.drop(columns=["date"]).to_parquet(out_path, index=False)
    else:
        base_dir = os.path.join(DATA_DIR, coin, tf, slug, "backfill")
        os.makedirs(base_dir, exist_ok=True)
        df.to_parquet(os.path.join(base_dir, "backfill_snapshots.parquet"), index=False)
    
    return len(all_snapshots)


# ─── Backfill OHLCV Candlesticks ─────────────────────────────────────────────

def backfill_candlesticks(market):
    """Download OHLCV candlestick data."""
    slug = market["slug"]
    coin = market["coin"]
    tf = market["timeframe"]
    condition_id = market.get("condition_id", "")
    
    if not condition_id:
        return 0
    
    now = int(time.time())
    all_candles = []
    
    for months_back in range(12):
        end_ts = now - (months_back * 30 * 86400)
        start_ts = end_ts - (30 * 86400)
        
        data = dome_get(f"/polymarket/candlesticks/{condition_id}", {
            "start_time": start_ts, "end_time": end_ts, "interval": 60
        })
        if not data:
            continue
        
        # API may return a list directly or a dict with a key
        if isinstance(data, list):
            candles = data
        else:
            candles = data.get("candlesticks", data.get("data", []))
        
        if not candles:
            continue
        
        for c in candles:
            if not isinstance(c, dict):
                continue
            price = c.get("price", {})
            if isinstance(price, dict):
                all_candles.append({
                    "timestamp": c.get("end_period_ts", ""),
                    "open": float(price.get("open", 0)),
                    "high": float(price.get("high", 0)),
                    "low": float(price.get("low", 0)),
                    "close": float(price.get("close", 0)),
                    "volume": float(c.get("volume", 0)),
                })
    
    if not all_candles:
        return 0
    
    df = pd.DataFrame(all_candles)
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception:
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        except Exception:
            pass
    
    base_dir = os.path.join(DATA_DIR, coin, tf, slug)
    os.makedirs(base_dir, exist_ok=True)
    df.to_parquet(os.path.join(base_dir, "backfill_ohlcv_1h.parquet"), index=False)
    return len(all_candles)


# ─── Backfill Binance Spot Prices ────────────────────────────────────────────

def backfill_binance_prices():
    """Download historical BTC and ETH spot prices from Binance via Dome API."""
    pairs = ["btcusdt", "ethusdt"]
    
    for pair in pairs:
        coin = "BTC" if "btc" in pair else "ETH"
        print(f"\n{'='*60}")
        print(f"  BACKFILLING BINANCE {coin}/USDT PRICES ({DAYS_BACK} days)")
        print(f"{'='*60}")
        
        total_prices = 0
        
        for days_ago in range(DAYS_BACK):
            target_date = datetime.date.today() - datetime.timedelta(days=days_ago)
            date_str = target_date.strftime("%Y-%m-%d")
            
            # Check if already downloaded
            out_dir = os.path.join(DATA_DIR, "oracle", "binance", coin)
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"{date_str}.parquet")
            if os.path.exists(out_path):
                continue
            
            # Calculate ms timestamps for the day
            start_dt = datetime.datetime.combine(target_date, datetime.time.min, tzinfo=datetime.timezone.utc)
            end_dt = start_dt + datetime.timedelta(days=1)
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
            
            all_prices = []
            pagination_key = None
            
            while True:
                params = {
                    "currency": pair,
                    "start_time": start_ms,
                    "end_time": end_ms,
                    "limit": 100,
                }
                if pagination_key:
                    params["pagination_key"] = pagination_key
                
                data = dome_get("/crypto/prices/binance", params)
                if not data:
                    break
                
                prices = data.get("prices", data.get("data", []))
                if not prices:
                    break
                
                for p in prices:
                    all_prices.append({
                        "timestamp": p.get("timestamp", 0),
                        "price": float(p.get("value", 0)),
                        "symbol": p.get("symbol", pair),
                    })
                
                # Check pagination
                pag_key = data.get("pagination_key")
                if not pag_key or pag_key == pagination_key:
                    break
                pagination_key = pag_key
            
            if all_prices:
                df = pd.DataFrame(all_prices)
                try:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                except Exception:
                    pass
                df.to_parquet(out_path, index=False)
                total_prices += len(all_prices)
            
            if days_ago % 7 == 0 and days_ago > 0:
                print(f"  Day -{days_ago}: {total_prices:,} prices so far")
        
        print(f"  [DONE] {coin}/USDT: {total_prices:,} total price points")


# ─── Backfill Chainlink Oracle Prices ────────────────────────────────────────

def backfill_chainlink_prices():
    """Download historical BTC and ETH Oracle prices from Chainlink via Dome API."""
    pairs = ["btc/usd", "eth/usd"]
    
    for pair in pairs:
        coin = "BTC" if "btc" in pair else "ETH"
        print(f"\n{'='*60}")
        print(f"  BACKFILLING CHAINLINK {coin}/USD ORACLE ({DAYS_BACK} days)")
        print(f"{'='*60}")
        
        total_prices = 0
        
        for days_ago in range(DAYS_BACK):
            target_date = datetime.date.today() - datetime.timedelta(days=days_ago)
            date_str = target_date.strftime("%Y-%m-%d")
            
            out_dir = os.path.join(DATA_DIR, "oracle", "chainlink", coin)
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"{date_str}.parquet")
            if os.path.exists(out_path):
                continue
            
            start_dt = datetime.datetime.combine(target_date, datetime.time.min, tzinfo=datetime.timezone.utc)
            end_dt = start_dt + datetime.timedelta(days=1)
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
            
            all_prices = []
            pagination_key = None
            
            while True:
                params = {
                    "currency": pair,
                    "start_time": start_ms,
                    "end_time": end_ms,
                    "limit": 100,
                }
                if pagination_key:
                    params["pagination_key"] = pagination_key
                
                data = dome_get("/crypto-prices/chainlink", params)
                if not data:
                    break
                
                prices = data.get("prices", data.get("data", []))
                if not prices:
                    break
                
                for p in prices:
                    all_prices.append({
                        "timestamp": p.get("timestamp", 0),
                        "price": float(p.get("value", 0)),
                        "symbol": p.get("symbol", pair),
                    })
                
                pag_key = data.get("pagination_key")
                if not pag_key or pag_key == pagination_key:
                    break
                pagination_key = pag_key
            
            if all_prices:
                df = pd.DataFrame(all_prices)
                try:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                except Exception:
                    pass
                df.to_parquet(out_path, index=False)
                total_prices += len(all_prices)
            
            if days_ago % 7 == 0 and days_ago > 0:
                print(f"  Day -{days_ago}: {total_prices:,} prices so far")
        
        print(f"  [DONE] {coin}/USD Oracle: {total_prices:,} total price points")


# ─── Main Execution ─────────────────────────────────────────────────────────

def main():
    if not DOME_API_KEY:
        print("[ERROR] Missing DOME_API_KEY environment variable.")
        print("  Set it with: set DOME_API_KEY=your_key_here")
        sys.exit(1)
    
    print("=" * 60)
    print("  DOME API HISTORICAL BACKFILL ENGINE")
    print(f"  Looking back {DAYS_BACK} days")
    print("=" * 60)
    print(f"  API Key: {DOME_API_KEY[:8]}...{DOME_API_KEY[-4:]}")
    print(f"  Data Dir: {DATA_DIR}")
    print("=" * 60)
    
    # Step 1: Discover markets by generating historical slugs
    markets = discover_markets_by_slugs()
    
    if not markets:
        print("[ERROR] No markets found.")
        sys.exit(1)
    
    # Save index
    idx = pd.DataFrame([{k: v for k, v in m.items() if k != "tokens"} for m in markets])
    os.makedirs(DATA_DIR, exist_ok=True)
    idx.to_parquet(os.path.join(DATA_DIR, "discovered_markets.parquet"), index=False)
    print(f"\n[SAVED] Market index ({len(markets)} markets)")
    
    # Step 2: Backfill each market
    total_trades = 0
    total_snapshots = 0
    total_candles = 0
    
    for i, market in enumerate(markets):
        slug = market["slug"]
        print(f"\n[{i+1}/{len(markets)}] {slug}")
        
        try:
            t = backfill_trades(market)
            total_trades += t
            if t > 0:
                print(f"  Trades: {t:,}")
        except Exception as e:
            print(f"  [ERROR] Trades: {e}")
        
        try:
            s = backfill_orderbook(market)
            total_snapshots += s
            if s > 0:
                print(f"  Snapshots: {s:,}")
        except Exception as e:
            print(f"  [ERROR] Orderbook: {e}")
        
        try:
            c = backfill_candlesticks(market)
            total_candles += c
            if c > 0:
                print(f"  Candles: {c:,}")
        except Exception as e:
            print(f"  [ERROR] Candles: {e}")
    
    # Step 3: Backfill Binance + Chainlink Oracle prices
    backfill_binance_prices()
    backfill_chainlink_prices()
    
    print(f"\n{'='*60}")
    print(f"  BACKFILL COMPLETE!")
    print(f"  Markets: {len(markets)}")
    print(f"  Trades: {total_trades:,}")
    print(f"  Snapshots: {total_snapshots:,}")
    print(f"  Candles: {total_candles:,}")
    
    try:
        from pathlib import Path
        total_mb = sum(f.stat().st_size for f in Path(DATA_DIR).rglob("*.parquet")) / (1024*1024)
        print(f"  Total Size: {total_mb:.1f} MB")
    except Exception:
        pass
    
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
