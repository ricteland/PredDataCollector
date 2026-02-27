import time

state = {
    'polymarket_trades': 0,
    'polymarket_snapshots': 0,
    'polymarket_ticks': 0,
    'slugs_active': 0,
    'next_flush_time': time.time() + 30,
    'next_slug_update': time.time(), # Immediate on startup
    'mb_saved': 0.0,
    'start_time': time.time(),
    'markets': {} # Tracks individual market stats
}
