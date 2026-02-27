import json
import os
import sys
import asyncio
import websockets
import datetime
import time
import pandas as pd

# ── Allow running directly as `python weather/weather_ws_client.py` ──────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _HERE)

import weather_shared_state

DATA_DIR = os.path.join(_ROOT, "data", "weather")

class DataLogger:
    def __init__(self, city, target_date, condition_id, market_slug, end_date):
        self.city = city
        self.target_date = target_date
        self.condition_id = condition_id
        self.market_slug = market_slug
        self.end_date = end_date
        self.trades_buffer = []
        self.snapshots_buffer = []
        self.ticks_buffer = []
        self.last_flush = time.time()
        self.flush_interval = 30

    def add_snapshot(self, timestamp, asset_id, bids, asks):
        self.snapshots_buffer.append({
            'timestamp': float(timestamp) if timestamp else 0,
            'market_slug': self.market_slug,
            'condition_id': self.condition_id,
            'asset_id': asset_id,
            'bids': json.dumps(bids),
            'asks': json.dumps(asks),
            'end_date': self.end_date
        })
        weather_shared_state.state['polymarket_snapshots'] += 1

    def add_tick(self, timestamp, asset_id, price, size, side, best_bid, best_ask):
        self.ticks_buffer.append({
            'timestamp': float(timestamp) if timestamp else 0,
            'market_slug': self.market_slug,
            'condition_id': self.condition_id,
            'asset_id': asset_id,
            'price': float(price),
            'size': float(size),
            'side': side,
            'best_bid': float(best_bid) if best_bid != 'N/A' else None,
            'best_ask': float(best_ask) if best_ask != 'N/A' else None
        })
        weather_shared_state.state['polymarket_ticks'] += 1

    def add_trade(self, timestamp, asset_id, price, size, side):
        self.trades_buffer.append({
            'timestamp': float(timestamp) if timestamp else 0,
            'market_slug': self.market_slug,
            'condition_id': self.condition_id,
            'asset_id': asset_id,
            'price': float(price),
            'size': float(size),
            'side': side,
            'end_date': self.end_date
        })
        weather_shared_state.state['polymarket_trades'] += 1
        if self.condition_id in weather_shared_state.state['markets']:
            weather_shared_state.state['markets'][self.condition_id]['trades'] += 1

    def flush_if_needed(self):
        if time.time() - self.last_flush >= self.flush_interval:
            self.flush()

    def flush(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        time_suffix = now.strftime("%H_%M_%S")

        base_dir = os.path.join(DATA_DIR, self.city, self.target_date, self.condition_id)
        
        try:
            os.makedirs(base_dir, exist_ok=True)

            if self.trades_buffer:
                df = pd.DataFrame(self.trades_buffer)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                df.to_parquet(os.path.join(base_dir, f"{time_suffix}_trades.parquet"), index=False)
                self.trades_buffer.clear()

            if self.snapshots_buffer:
                df = pd.DataFrame(self.snapshots_buffer)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                df.to_parquet(os.path.join(base_dir, f"{time_suffix}_snapshots.parquet"), index=False)
                self.snapshots_buffer.clear()

            if self.ticks_buffer:
                df = pd.DataFrame(self.ticks_buffer)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                df.to_parquet(os.path.join(base_dir, f"{time_suffix}_ticks.parquet"), index=False)
                self.ticks_buffer.clear()

        except Exception:
            pass

        self.last_flush = time.time()
        weather_shared_state.state['next_flush_time'] = self.last_flush + self.flush_interval

# --- Global State ---
active_tokens = {}

async def update_markets_loop():
    fetch_script = os.path.join(_HERE, 'fetch_weather_tokens.py')
    json_out = os.path.join(_HERE, 'weather_data_fetched.json')
    while True:
        try:
            process = await asyncio.create_subprocess_shell(
                f'python "{fetch_script}"',
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE)
            try:
                await asyncio.wait_for(process.communicate(), timeout=60.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.communicate()
            
            if os.path.exists(json_out):
                with open(json_out, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    update_global_routing(data)
            
        except Exception:
            pass
            
        await asyncio.sleep(15 * 60)


def update_global_routing(data):
    global active_tokens
    
    old_loggers = { meta['condition_id']: meta['logger'] for meta in active_tokens.values() }
    new_active_tokens = {}
    
    tracked_cids = []
    events = data.get('events', [])
    for ev in events:
        city = ev.get('city')
        target_date = ev.get('date')
        market_slug = ev.get('market_slug')
        condition_id = ev.get('condition_id')
        question = ev.get('question', '')
        end_date = ev.get('end_date')
        tokens = ev.get('tokens', {})
        
        yes_obj = tokens.get('yes')
        no_obj = tokens.get('no')
        if not yes_obj or not no_obj: continue
        
        logger = old_loggers.get(condition_id, DataLogger(city, target_date, condition_id, market_slug, end_date))
        
        new_active_tokens[yes_obj['token_id']] = {
            'city': city, 'condition_id': condition_id, 'side': 'YES', 'logger': logger
        }
        new_active_tokens[no_obj['token_id']] = {
            'city': city, 'condition_id': condition_id, 'side': 'NO', 'logger': logger
        }
        tracked_cids.append(condition_id)
        
        if condition_id not in weather_shared_state.state['markets']:
            weather_shared_state.state['markets'][condition_id] = {
                'city': city,
                'question': question,
                'end_date': end_date,
                'trades': 0
            }
                
    active_cid_set = set(tracked_cids)
    keys_to_remove = [s for s in weather_shared_state.state['markets'].keys() if s not in active_cid_set]
    for s in keys_to_remove:
        del weather_shared_state.state['markets'][s]
        
    for token_id, meta in active_tokens.items():
        if token_id not in new_active_tokens:
             meta['logger'].flush()
             
    active_tokens = new_active_tokens
    weather_shared_state.state['slugs_active'] = len(active_cid_set)
    weather_shared_state.state['next_slug_update'] = time.time() + 900


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
                        raise Exception("Watchdog timeout")
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
