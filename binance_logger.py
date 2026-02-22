import json
import os
import asyncio
import websockets
import datetime
import time
import pandas as pd
import shared_state

class BinanceDataLogger:
    def __init__(self, coin):
        self.coin = coin
        self.trades_buffer = []
        self.ticks_buffer = []
        self.last_flush = time.time()
        self.flush_interval = 60 # seconds

    def add_trade(self, timestamp, price, size, is_buyer_maker):
        # In Binance, if the buyer is the maker, the taker (aggressor) was selling.
        side = 'SELL' if is_buyer_maker else 'BUY' 
        self.trades_buffer.append({
            'timestamp': float(timestamp),
            'price': float(price),
            'size': float(size),
            'side': side
        })
        shared_state.state['binance_trades'] += 1

    def add_tick(self, timestamp, best_bid, best_bid_size, best_ask, best_ask_size):
        self.ticks_buffer.append({
            'timestamp': float(timestamp),
            'best_bid': float(best_bid),
            'best_bid_size': float(best_bid_size),
            'best_ask': float(best_ask),
            'best_ask_size': float(best_ask_size)
        })
        shared_state.state['binance_ticks'] += 1

    def flush_if_needed(self):
        if time.time() - self.last_flush >= self.flush_interval:
            self.flush()

    def flush(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        date_folder = now.strftime("%Y-%m-%d")
        time_suffix = now.strftime("%H_%M_%S")

        base_dir = f"data/{self.coin}/SPOT/{date_folder}"
        os.makedirs(base_dir, exist_ok=True)

        flushed = False

        if self.trades_buffer:
            df = pd.DataFrame(self.trades_buffer)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.to_parquet(f"{base_dir}/{time_suffix}_trades.parquet", index=False)
            self.trades_buffer.clear()
            flushed = True

        if self.ticks_buffer:
            df = pd.DataFrame(self.ticks_buffer)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.to_parquet(f"{base_dir}/{time_suffix}_ticks.parquet", index=False)
            self.ticks_buffer.clear()
            flushed = True

        self.last_flush = time.time()
        shared_state.state['next_flush_time'] = self.last_flush + self.flush_interval
        # Removed the print statement as requested.


async def binance_ws_loop():
    # We subscribe to @aggTrade for executions and @bookTicker for the top of the orderbook
    url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@bookTicker/ethusdt@bookTicker"
    
    loggers = {
        'BTC': BinanceDataLogger('BTC'),
        'ETH': BinanceDataLogger('ETH')
    }

    backoff = 3
    async for websocket in websockets.connect(url, ping_interval=20, ping_timeout=20):
        backoff = 3
        last_data_time = time.time()
        try:
            while True:
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    data = json.loads(response)
                    last_data_time = time.time()
                    
                    # Binance streams provide the symbol in 's'
                    stream_symbol = data.get('s', '')
                    coin = 'BTC' if 'BTC' in stream_symbol else 'ETH' if 'ETH' in stream_symbol else None
                    if not coin:
                        continue
                        
                    logger = loggers[coin]
                    
                    # 1. Executed Trades (aggTrade)
                    if data.get('e') == 'aggTrade':
                        timestamp = data.get('E')  # Event time
                        price = data.get('p')
                        size = data.get('q')
                        is_buyer_maker = data.get('m')
                        
                        logger.add_trade(timestamp, price, size, is_buyer_maker)
                        
                    # 2. Incremental Top of Book updates (bookTicker)
                    elif 'u' in data and 'b' in data and 'a' in data:
                        # @bookTicker payloads do not contain a millisecond server timestamp 'E' by default
                        # We use the local precise time so it correlates securely with the loop
                        local_ms_timestamp = time.time() * 1000
                        
                        best_bid = data.get('b')
                        best_bid_size = data.get('B')
                        best_ask = data.get('a')
                        best_ask_size = data.get('A')
                        
                        logger.add_tick(local_ms_timestamp, best_bid, best_bid_size, best_ask, best_ask_size)
                        
                except asyncio.TimeoutError:
                    if time.time() - last_data_time > 60:
                        raise Exception("Watchdog timeout - No data for 60s")
                except websockets.exceptions.ConnectionClosed:
                    raise
                
                # Check 60-second flush logic
                for l in loggers.values():
                    l.flush_if_needed()
                    
        except websockets.exceptions.ConnectionClosed:
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)
            continue
        except Exception as e:
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)
            continue

if __name__ == "__main__":
    try:
        asyncio.run(binance_ws_loop())
    except KeyboardInterrupt:
        print("\nDisconnected.")
