import json
import os
import sys
import asyncio
import websockets
import datetime
import time
import pandas as pd

# ── Allow running directly as `python crypto/ws_client.py` ──────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _HERE)

import shared_state

# Project-root-relative data directory
DATA_DIR = os.path.join(_ROOT, "data")

class DataLogger:
    def __init__(self, coin, timeframe, market_slug, end_date):
        self.coin = coin
        self.timeframe = timeframe
        self.market_slug = market_slug
        self.end_date = end_date
        self.trades_buffer = []
        self.snapshots_buffer = []
        self.ticks_buffer = []
        self.last_flush = time.time()
        self.flush_interval = 900 # 15 minutes
        
        # Deduplication state
        self.last_snapshot = None # (bids_json, asks_json)
        self.last_tick = None     # (price, size, side, best_bid, best_ask)

    def add_snapshot(self, timestamp, asset_id, bids, asks):
        # Deduplicate: only add if the orderbook actually changed
        bids_json = json.dumps(bids)
        asks_json = json.dumps(asks)
        if self.last_snapshot == (bids_json, asks_json):
            return

        self.snapshots_buffer.append({
            'timestamp': float(timestamp) if timestamp else 0,
            'market_slug': self.market_slug,
            'asset_id': asset_id,
            'bids': bids_json,
            'asks': asks_json,
            'end_date': self.end_date
        })
        self.last_snapshot = (bids_json, asks_json)
        shared_state.state['polymarket_snapshots'] += 1

    def add_tick(self, timestamp, asset_id, price, size, side, best_bid, best_ask):
        # Deduplicate ticks/BBO updates
        this_tick = (float(price), float(size), side, best_bid, best_ask)
        if self.last_tick == this_tick:
            return

        self.ticks_buffer.append({
            'timestamp': float(timestamp) if timestamp else 0,
            'market_slug': self.market_slug,
            'asset_id': asset_id,
            'price': float(price),
            'size': float(size),
            'side': side,
            'best_bid': float(best_bid) if best_bid != 'N/A' else None,
            'best_ask': float(best_ask) if best_ask != 'N/A' else None
        })
        self.last_tick = this_tick
        shared_state.state['polymarket_ticks'] += 1

    def add_trade(self, timestamp, asset_id, price, size, side):
        self.trades_buffer.append({
            'timestamp': float(timestamp) if timestamp else 0,
            'market_slug': self.market_slug,
            'asset_id': asset_id,
            'price': float(price),
            'size': float(size),
            'side': side,
            'end_date': self.end_date
        })
        shared_state.state['polymarket_trades'] += 1
        if self.market_slug in shared_state.state['markets']:
            shared_state.state['markets'][self.market_slug]['trades'] += 1

    def flush_if_needed(self):
        if time.time() - self.last_flush >= self.flush_interval:
            self.flush()

    def flush(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        date_folder = now.strftime("%Y-%m-%d")
        time_suffix = now.strftime("%H_00") # Hourly files

        base_dir = os.path.join(DATA_DIR, self.coin, self.timeframe, self.market_slug, date_folder)
        
        try:
            os.makedirs(base_dir, exist_ok=True)

            for target_buffer, prefix in [(self.trades_buffer, "trades"), 
                                          (self.snapshots_buffer, "snapshots"), 
                                          (self.ticks_buffer, "ticks")]:
                if not target_buffer:
                    continue
                
                new_df = pd.DataFrame(target_buffer)
                new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms', utc=True)
                
                file_path = os.path.join(base_dir, f"{time_suffix}_{prefix}.parquet")
                
                # Check for existing hourly file to append
                if os.path.exists(file_path):
                    try:
                        old_df = pd.read_parquet(file_path)
                        final_df = pd.concat([old_df, new_df], ignore_index=True)
                        final_df.to_parquet(file_path, index=False)
                    except Exception:
                        new_df.to_parquet(file_path, index=False)
                else:
                    new_df.to_parquet(file_path, index=False)
                
                target_buffer.clear()

        except Exception as e:
            pass

        self.last_flush = time.time()
        shared_state.state['next_flush_time'] = self.last_flush + self.flush_interval

# --- Global State ---
# Maps token_id (string) -> { 'coin', 'timeframe', 'slug', 'side', 'logger' }
active_tokens = {}

async def update_markets_loop():
    """Background task to fetch Gamma API tokens every 15 minutes."""
    fetch_script = os.path.join(_HERE, 'fetch_tokens.js')
    json_out = os.path.join(_HERE, 'polymarket_data_fetched.json')
    while True:
        try:
            process = await asyncio.create_subprocess_shell(
                f'node "{fetch_script}"',
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE)
            try:
                await asyncio.wait_for(process.communicate(), timeout=30.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.communicate()
                raise Exception("Node API Zombie Timeout")
            
            if os.path.exists(json_out):
                with open(json_out, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    update_global_routing(data)
            
        except Exception as e:
            pass
            
        await asyncio.sleep(15 * 60) # Wait 15 minutes


def update_global_routing(data):
    """Parses the JSON and specifically tracks sliding windows for BTC/ETH out of all events."""
    global active_tokens
    
    old_loggers = { meta['slug']: meta['logger'] for meta in active_tokens.values() }
    new_active_tokens = {}
    
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    configs = {
        '1h': 2,    # Current + Next 1
        '15m': 2,   # Current + Next 1
        '5m': 4     # Current + Next 3
    }

    tracked_slugs = []

    for coin in ['BTC', 'ETH']:
        coin_data = data.get('markets', {}).get(coin, {})
        
        for tf, limit in configs.items():
            events = coin_data.get(tf, {}).get('events', [])
            
            future_events = []
            for ev in events:
                end_str = ev.get('end_date')
                if not end_str: continue
                try:
                    dt_str = end_str.replace('Z', '+00:00')
                    end_dt = datetime.datetime.fromisoformat(dt_str)
                    if end_dt > now_utc:
                        future_events.append((end_dt, ev))
                except:
                    continue
                    
            future_events.sort(key=lambda x: x[0])
            active_slice = future_events[:limit]
            
            for _, ev in active_slice:
                slug = ev.get('event_slug')
                end_date = ev.get('end_date')
                tokens = ev.get('tokens', {})
                
                yes_obj = tokens.get('yes') or tokens.get('up')
                no_obj = tokens.get('no') or tokens.get('down')
                if not yes_obj or not no_obj: continue
                
                logger = old_loggers.get(slug, DataLogger(coin, tf, slug, end_date))
                
                new_active_tokens[yes_obj['token_id']] = {
                    'coin': coin, 'timeframe': tf, 'slug': slug, 'side': 'YES', 'logger': logger
                }
                new_active_tokens[no_obj['token_id']] = {
                    'coin': coin, 'timeframe': tf, 'slug': slug, 'side': 'NO', 'logger': logger
                }
                tracked_slugs.append(slug)
                
                if slug not in shared_state.state['markets']:
                    shared_state.state['markets'][slug] = {
                        'coin': coin,
                        'timeframe': tf,
                        'end_date': end_date,
                        'trades': logger.trades_buffer.__len__() if logger else 0
                    }
                
    active_slugs_set = set(tracked_slugs)
    keys_to_remove = [s for s in shared_state.state['markets'].keys() if s not in active_slugs_set]
    for s in keys_to_remove:
        del shared_state.state['markets'][s]
        
    for token_id, meta in active_tokens.items():
        if token_id not in new_active_tokens:
             meta['logger'].flush()
             
    active_tokens = new_active_tokens
    shared_state.state['slugs_active'] = len(active_slugs_set)
    shared_state.state['next_slug_update'] = time.time() + 900


async def subscribe_and_listen():
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    backoff = 3
    async for websocket in websockets.connect(url, ping_interval=10, ping_timeout=10):
        backoff = 3
        last_data_time = time.time()
        current_sub_ids = []
        
        try:
            while True:
                latest_ids = list(active_tokens.keys())
                if set(latest_ids) != set(current_sub_ids) and latest_ids:
                    sub_msg = { "assets_ids": latest_ids, "type": "market" }
                    await websocket.send(json.dumps(sub_msg))
                    current_sub_ids = latest_ids
                
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    data = json.loads(response)
                    last_data_time = time.time()
                    
                    if isinstance(data, list):
                        for msg in data:
                            process_ws_message(msg)
                    else:
                        process_ws_message(data)
                except asyncio.TimeoutError:
                    if time.time() - last_data_time > 60:
                        raise Exception("Watchdog timeout - No data for 60s")
                except websockets.exceptions.ConnectionClosed:
                    raise
                
                unique_loggers = { meta['logger'] for meta in active_tokens.values() }
                for logger in unique_loggers:
                     logger.flush_if_needed()
                    
        except websockets.exceptions.ConnectionClosed:
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)
            continue
        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)
            continue


def process_ws_message(msg):
    event_type = msg.get('event_type')
    asset_id = msg.get('asset_id')
    
    if not asset_id and event_type != 'price_change':
        return
        
    server_time = msg.get('timestamp', '0')
    
    if event_type == 'book':
        bids = msg.get('bids', [])
        asks = msg.get('asks', [])
        meta = active_tokens.get(asset_id)
        if not meta: return
        meta['logger'].add_snapshot(server_time, asset_id, bids, asks)

    elif event_type == 'price_change':
        changes = msg.get('price_changes', [])
        for change in changes:
            c_asset = change.get('asset_id')
            meta = active_tokens.get(c_asset)
            if not meta: continue
            
            price = change.get('price')
            size = change.get('size')
            side = change.get('side')
            best_bid = change.get('best_bid', 'N/A')
            best_ask = change.get('best_ask', 'N/A')
            
            meta['logger'].add_tick(server_time, c_asset, price, size, side, best_bid, best_ask)

    elif event_type == 'last_trade_price':
        meta = active_tokens.get(asset_id)
        if not meta: return
        
        price = msg.get('price')
        size = msg.get('size')
        side = msg.get('side', 'UNKNOWN')
        if price is None or size is None: return
        meta['logger'].add_trade(server_time, asset_id, price, size, side)


async def main_daemon():
    print("\nStarting Polymarket Multi-Market Continuous Daemon...")
    print("Tracking 1h (2), 15m (2), and 5m (4) resolving markets for BTC and ETH.\n")
    
    fetcher_task = asyncio.create_task(update_markets_loop())
    
    while not active_tokens:
         await asyncio.sleep(1)
         
    ws_task = asyncio.create_task(subscribe_and_listen())
    
    try:
        await asyncio.gather(fetcher_task, ws_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        unique_loggers = { meta['logger'] for meta in active_tokens.values() }
        for l in unique_loggers:
             l.flush()
