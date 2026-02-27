"""
Dome API & Gamma API Historical Backfill Script for Weather Markets
===================================================================
Downloads historical 1-hour OHLCV and L2 Orderbook snapshots for 
PolyMarket daily temperature markets.
"""

import os
import sys
import time
import json
import argparse
import requests
import datetime
import pandas as pd

# ─── Configuration ───────────────────────────────────────────────────────────

DOME_API_KEY = os.environ.get("DOME_API_KEY", "f2e46c2395d9d74419feea87eae520cafbe44eaa")
BASE_URL = "https://api.domeapi.io/v1"
GAMMA_URL = "https://gamma-api.polymarket.com/events"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "weather")
RATE_LIMIT_SLEEP = 0.15  # ~7 QPS to stay under the 10 QPS free tier

# How many days back to generate slugs for
DAYS_BACK = 90

CITIES = [
    "london", "seoul", "nyc", "toronto", "wellington", 
    "atlanta", "chicago", "seattle", "buenos-aires", "miami"
]

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
        if status in [404, 400]:
            return None
            
        # Handle 502/504
        if status in [500, 502, 503, 504] and retries > 0:
            sleep_time = (4 - retries) * 2
            print(f"  [SERVER ERROR {status}] Retrying in {sleep_time}s...")
            time.sleep(sleep_time)
            return dome_get(endpoint, params, retries - 1)
            
        print(f"  [HTTP ERROR] {e}")
        return None
    except Exception as e:
        if retries > 0:
            print(f"  [NETWORK ERROR] Retrying in 2s...")
            time.sleep(2)
            return dome_get(endpoint, params, retries - 1)
        print(f"  [ERROR] {e}")
        return None

def gamma_get(slug):
    """Fetch event details from Gamma API by slug."""
    url = f"{GAMMA_URL}?slug={slug}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
    except Exception:
        pass
    return None

# ─── Slug Generation ─────────────────────────────────────────────────────────

def generate_weather_slugs(city, target_date):
    """
    Generate weather slug permutations for a specific city and date.
    Polymarket is highly inconsistent with their weather slug formats!
    """
    slugs = []
    month_full = target_date.strftime("%B").lower()
    month_short = target_date.strftime("%b").lower()[:3]
    day = str(target_date.day)
    year = target_date.strftime("%Y")
    prev_year = str(int(year) - 1)
    
    city_variants = [city]
    if city == "nyc":
        city_variants.append("new-york-city")
    if city == "buenos-aires":
        city_variants.append("buenos-aires-ar")
        
    for cv in city_variants:
        for m in [month_full, month_short]:
            # Try with current year, previous year, and no year
            for y in [year, prev_year, ""]:
                suffix = f"-{y}" if y else ""
                slugs.append(f"highest-temperature-in-{cv}-on-{m}-{day}{suffix}")
            
    return list(dict.fromkeys(slugs))

# ─── Market Discovery ────────────────────────────────────────────────────────

def discover_weather_markets(cities, days_back):
    """Generate historical slugs and scrape the markets/tokens from Gamma API."""
    all_events = []
    today = datetime.date.today()
    
    print(f"\n{'='*60}")
    print(f"  DISCOVERING WEATHER MARKETS ({days_back} days back)")
    print(f"{'='*60}")
    
    for city in cities:
        found = 0
        missed = 0
        print(f"\n  [DISCOVERING {city} markets...]")
        
        for days_ago in range(days_back):
            target_date = today - datetime.timedelta(days=days_ago)
            slugs_to_try = generate_weather_slugs(city, target_date)
            
            event = None
            for slug in slugs_to_try:
                event = gamma_get(slug)
                if event:
                    break
                    
            if not event:
                missed += 1
                continue
                
            markets = event.get("markets", [])
            buckets = []
            
            for m in markets:
                condition_id = m.get("conditionId") or m.get("condition_id")
                question = m.get("question", "")
                market_slug = m.get("slug", "")
                
                try:
                    outcomes = json.loads(m.get("outcomes", "[]"))
                    clobTokenIds = json.loads(m.get("clobTokenIds", "[]"))
                except Exception:
                    continue
                
                yes_token = None
                for i, outcome in enumerate(outcomes):
                    if outcome.lower() == "yes" and i < len(clobTokenIds):
                        yes_token = clobTokenIds[i]
                        break
                
                if condition_id and yes_token:
                    buckets.append({
                        "question": question,
                        "market_slug": market_slug,
                        "condition_id": condition_id,
                        "yes_token": yes_token
                    })
            
            if buckets:
                all_events.append({
                    "city": city,
                    "date": target_date.strftime("%Y-%m-%d"),
                    "slug": slug,
                    "event_id": event.get("id"),
                    "buckets": buckets
                })
                found += 1
            else:
                missed += 1
        
        print(f"  [{city.upper()} DONE] Total: {found} found, {missed} missed")
        
    print(f"\n[DISCOVERY COMPLETE] Total events: {len(all_events)}")
    return all_events

# ─── Backfill Candlesticks (OHLCV) ───────────────────────────────────────────

def backfill_candlesticks(city, date_str, bucket):
    """Download OHLCV candlestick data for a submarket bucket."""
    condition_id = bucket.get("condition_id")
    if not condition_id:
        return 0
        
    now = int(time.time())
    all_candles = []
    
    # Dome API restricts 1-hour candles to 31-day chunks, so we paginate 3 times (90 days)
    for months_back in range(3):
        end_ts = now - (months_back * 30 * 86400)
        start_ts = end_ts - (30 * 86400)
        
        data = dome_get(f"/polymarket/candlesticks/{condition_id}", {
            "start_time": start_ts, "end_time": end_ts, "interval": 60
        })
        
        if not data:
            continue
            
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
            
    # Clean the bucket question for the filename
    clean_name = bucket.get("question", "market").replace(" ", "_").replace("/", "").replace(">", "gt").replace("<", "lt").replace("?", "")
    base_dir = os.path.join(DATA_DIR, city, date_str, "ohlcv")
    
    # Limit filename length
    filename = f"{clean_name[:50]}_1h.parquet"
    out_path = os.path.join(base_dir, filename)
    
    if os.path.exists(out_path):
        return -1 # Sentinel for skipped
        
    os.makedirs(base_dir, exist_ok=True)
    df.to_parquet(out_path, index=False)
    
    return len(all_candles)

# ─── Backfill Orderbooks (Fall back / Comprehensive) ─────────────────────────

def backfill_orderbooks(city, date_str, bucket):
    """Download L2 orderbook snapshots for the YES token."""
    token_id = bucket.get("yes_token")
    if not token_id:
        return 0
        
    start_ms = int(time.time() * 1000) - (90 * 86400 * 1000)
    end_ms = int(time.time() * 1000)
    all_snapshots = []
    pagination_key = None
    
    while True:
        params = {"token_id": token_id, "start_time": start_ms, "end_time": end_ms, "limit": 1000}
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
        next_cursor = pagination.get("pagination_key")
        if not next_cursor or next_cursor == pagination_key:
            break
        pagination_key = next_cursor
        
    if not all_snapshots:
        return 0
        
    df = pd.DataFrame(all_snapshots)
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    except Exception:
        pass
        
    # To keep it lightweight and simulated at Top Of Hour, we could resample
    # But writing raw the full snapshots gives maximum flexibility:
    clean_name = bucket.get("question", "market").replace(" ", "_").replace("/", "").replace(">", "gt").replace("<", "lt").replace("?", "")
    base_dir = os.path.join(DATA_DIR, city, date_str, "orderbook")
    filename = f"{clean_name[:50]}_snapshots.parquet"
    out_path = os.path.join(base_dir, filename)

    if os.path.exists(out_path):
        return -1 # Sentinel for skipped
        
    os.makedirs(base_dir, exist_ok=True)
    df.to_parquet(out_path, index=False)
    
    return len(all_snapshots)

# ─── Main Execution ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Dome API Weather Markets Backfill")
    parser.add_argument("--discover", action="store_true", help="Force fresh market discovery over Gamma API instead of using local cache.")
    parser.add_argument("--days", type=int, default=DAYS_BACK, help=f"Number of days to look back (default: {DAYS_BACK})")
    parser.add_argument("--cities", type=str, help="Comma-separated list of cities to backfill (e.g. 'nyc,london')")
    args = parser.parse_args()

    active_cities = CITIES
    if args.cities:
        active_cities = [c.strip().lower() for c in args.cities.split(",")]

    print("=" * 60)
    print("  DOME API WEATHER MARKETS BACKFILL")
    print(f"  Looking back {args.days} days")
    print(f"  Cities: {', '.join(active_cities)}")
    print("=" * 60)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    cache_path = os.path.join(DATA_DIR, "discovered_weather_markets.json")
    
    events = None
    if not args.discover and os.path.exists(cache_path):
        print(f"  [INFO] Loading pre-discovered markets from {cache_path}")
        try:
            with open(cache_path, "r") as f:
                events = json.load(f)
                # Filter cached events by requested cities and days
                cutoff_date = (datetime.date.today() - datetime.timedelta(days=args.days)).strftime("%Y-%m-%d")
                events = [e for e in events if e["city"] in active_cities and e["date"] >= cutoff_date]
                print(f"  [INFO] Loaded {len(events)} events for requested scope from cache.")
        except Exception as e:
            print(f"  [ERROR] Could not load local cache: {e}. Re-discovering markets...")
            events = None
            
    if not events:
        events = discover_weather_markets(active_cities, args.days)
        if events:
            print(f"\n  [INFO] Caching {len(events)} events to {cache_path}")
            # Merge with existing cache if it exists
            existing_cache = []
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, "r") as f:
                        existing_cache = json.load(f)
                except: pass
            
            # Simple merge: use slugs as keys to avoid duplicates
            merged = {e["slug"]: e for e in existing_cache}
            for e in events:
                merged[e["slug"]] = e
            
            with open(cache_path, "w") as f:
                json.dump(list(merged.values()), f, indent=2)

    if not events:
        print("[ERROR] No weather events found.")
        sys.exit(1)
        
    total_candles = 0
    total_orderbooks = 0
    skipped_candles = 0
    skipped_orderbooks = 0
    total_buckets = 0
    
    for e in events:
        city = e["city"]
        date_str = e["date"]
        buckets = e["buckets"]
        print(f"\n[PROCESSING] {e['slug']} ({len(buckets)} sub-markets)")
        
        for bucket in buckets:
            total_buckets += 1
            # 1. Try Candlesticks
            c = backfill_candlesticks(city, date_str, bucket)
            if c == -1:
                skipped_candles += 1
                c_str = "SKIP"
            else:
                total_candles += c
                c_str = str(c)
            
            # 2. Try Orderbooks
            o = backfill_orderbooks(city, date_str, bucket)
            if o == -1:
                skipped_orderbooks += 1
                o_str = "SKIP"
            else:
                total_orderbooks += o
                o_str = str(o)
            
            print(f"  -> {bucket['question'][:40]}: {c_str} OHLCV | {o_str} Orderbooks")

    print(f"\n{'='*60}")
    print(f"  BACKFILL COMPLETE!")
    print(f"  Events: {len(events)}")
    print(f"  Total Buckets: {total_buckets}")
    print(f"  Candles Saved: {total_candles:,} (Skipped: {skipped_candles})")
    print(f"  Orderbook Snaps: {total_orderbooks:,} (Skipped: {skipped_orderbooks})")
    print(f"{'='*60}")

    print(f"\n{'='*60}")
    print(f"  BACKFILL COMPLETE!")
    print(f"  Events: {len(events)}")
    print(f"  Total Buckets: {total_buckets}")
    print(f"  Candles Saved: {total_candles:,}")
    print(f"  Orderbook Snaps: {total_orderbooks:,}")
    print(f"{'='*60}")
    
if __name__ == "__main__":
    main()
