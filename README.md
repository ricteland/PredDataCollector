# Polymarket Data Collector

This tool connects to the Polymarket Gamma API to continuously discover active crypto markets for short timeframes (5m, 15m, 1h, 4h). It then connects to the Polymarket Central Limit Order Book (CLOB) via WebSockets to stream tick-by-tick orderbook updates and saves them directly to partitioned Parquet files for easy data analysis.

## Features

- **Dynamic Market Discovery**: Automatically finds currently active 5m, 15m, 1h, and 4h crypto markets.
- **Auto-Subscription Updates**: Subscribes to newly opened markets and unsubscribes from finished markets dynamically without dropping the connection.
- **Robust Websocket Client**: Handles dropped connections with exponential back-off reconnects and a built-in heartbeat (PING) mechanism.
- **Batched Parquet Writes**: Efficiently batches events and flushes them to Parquet files organized by `data/<timeframe>/<date>/<timestamp>.parquet`, preventing huge memory overhead.

## Requirements

Install dependencies using `pip`:

```bash
pip install -r requirements.txt
```

## Running the Collector

Run the main script to start listening:

```bash
python main.py
```

*(Press `Ctrl+C` to safely shut down and flush all remaining data to disk).*

## Architecture

- `main.py`: The entry point orchestration script.
- `clob_client.py`: The core WebSocket loop and event consumer.
- `market_discovery.py`: The REST API client for the Gamma `/events` endpoint that filters crypto tokens.
- `parquet_writer.py`: Thread-safe buffer that groups events by timeframe and writes them to PyArrow Parquet files.
