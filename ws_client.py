import json
import os
import sys
import asyncio
import websockets
import datetime
import time
import pandas as pd
import shared_state

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
        self.flush_interval = 30 # seconds

    def add_snapshot(self, timestamp, asset_id, bids, asks):
        self.snapshots_buffer.append({
            'timestamp': float(timestamp) if timestamp else 0,
            'market_slug': self.market_slug,
            'asset_id': asset_id,
            'bids': json.dumps(bids),
            'asks': json.dumps(asks),
            'end_date': self.end_date
        })
        shared_state.state['polymarket_snapshots'] += 1

    def add_tick(self, timestamp, asset_id, price, size, side, best_bid, best_ask):
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
        time_suffix = now.strftime("%H_%M_%S")

        base_dir = f"data/{self.coin}/{self.timeframe}/{self.market_slug}/{date_folder}"
        os.makedirs(base_dir, exist_ok=True)

        flushed = False

        if self.trades_buffer:
            df = pd.DataFrame(self.trades_buffer)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.to_parquet(f"{base_dir}/{time_suffix}_trades.parquet", index=False)
            self.trades_buffer.clear()
            flushed = True

        if self.snapshots_buffer:
            df = pd.DataFrame(self.snapshots_buffer)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.to_parquet(f"{base_dir}/{time_suffix}_snapshots.parquet", index=False)
            self.snapshots_buffer.clear()
            flushed = True

        if self.ticks_buffer:
            df = pd.DataFrame(self.ticks_buffer)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.to_parquet(f"{base_dir}/{time_suffix}_ticks.parquet", index=False)
            self.ticks_buffer.clear()
            flushed = True

        self.last_flush = time.time()
        shared_state.state['next_flush_time'] = self.last_flush + self.flush_interval

# --- Global State ---
# Maps token_id (string) -> { 'coin', 'timeframe', 'slug', 'side', 'logger' }
active_tokens = {}

async def update_markets_loop():
    """Background task to fetch Gamma API tokens every 15 minutes."""
    while True:
        try:
            process = await asyncio.create_subprocess_shell('node fetch_tokens.js', 
                                                            stdout=asyncio.subprocess.PIPE, 
                                                            stderr=asyncio.subprocess.PIPE)
            try:
                await asyncio.wait_for(process.communicate(), timeout=30.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.communicate()
                raise Exception("Node API Zombie Timeout")
            
            file_path = 'polymarket_data_fetched.json'
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    update_global_routing(data)
            
        except Exception as e:
            pass
            
        await asyncio.sleep(15 * 60) # Wait 15 minutes


def update_global_routing(data):
    """Parses the JSON and specifically tracks sliding windows for BTC/ETH out of all events."""
    global active_tokens
    
    # Store old loggers to maintain persistence over same market runs
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
            
            # Filter to only future resolving events
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
                    
            # Sort by ascending end time and slice the exactly requested limit
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
                
                # Update Market Trackers for Dashboard TUI
                if slug not in shared_state.state['markets']:
                    shared_state.state['markets'][slug] = {
                        'coin': coin,
                        'timeframe': tf,
                        'end_date': end_date,
                        'trades': logger.trades_buffer.__len__() if logger else 0
                    }
                
    # Force flush any loggers that fell out of the active window
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
        
        # We need a copy of the currently subscribed IDs so we know if the routing table changes
        current_sub_ids = []
        
        try:
            while True:
                # 1. Check if we need to update our active subscription list onto the WS
                latest_ids = list(active_tokens.keys())
                if set(latest_ids) != set(current_sub_ids) and latest_ids:
                    print(f"[WS] Subscription mismatch detected. Subscribing to {len(latest_ids)} active tokens...")
                    sub_msg = { "assets_ids": latest_ids, "type": "market" }
                    await websocket.send(json.dumps(sub_msg))
                    current_sub_ids = latest_ids
                
                # 2. Wait for incoming packet
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
                
                # 3. Check flush loops for all active logging buffers concurrently
                # Use a set of unique loggers to avoid double-checking
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
    
    # If it's a batch message like `price_change` it might lack an outer `asset_id`
    if not asset_id and event_type != 'price_change':
        return
        
    local_time = datetime.datetime.now().strftime("%H:%M:%S")
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
    
    # Run fetcher in background to swap markets out live every 15 min
    fetcher_task = asyncio.create_task(update_markets_loop())
    
    # Wait until we have at least fetched data once before bringing the WS up
    while not active_tokens:
         await asyncio.sleep(1)
         
    # Launch standard WS event loop multiplexer 
    ws_task = asyncio.create_task(subscribe_and_listen())
    
    try:
        await asyncio.gather(fetcher_task, ws_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        unique_loggers = { meta['logger'] for meta in active_tokens.values() }
        for l in unique_loggers:
             l.flush()
