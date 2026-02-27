"""
Backfill Verification Script
============================
Audits the downloaded historical trades against the Dome API's expected total.
If a market was partially downloaded (e.g., due to a 502/504 error during pagination),
this script identifies the mismatch, deletes the incomplete local files, and triggers
a fresh re-download using the `dome_backfill.py` logic.

Usage:
  set DOME_API_KEY=your_key_here
  python verify_backfill.py
"""

import os
import sys
import time
import requests
import pandas as pd
from pathlib import Path

# Import the backfill logic so we can re-trigger it
from dome_backfill import dome_get, backfill_trades, DATA_DIR, DOME_API_KEY

def verify_and_repair_markets():
    if not DOME_API_KEY:
        print("[ERROR] Missing DOME_API_KEY environment variable.")
        sys.exit(1)
        
    index_path = os.path.join(DATA_DIR, "discovered_markets.parquet")
    if not os.path.exists(index_path):
        print(f"[ERROR] No market index found at {index_path}. Run dome_backfill.py first.")
        sys.exit(1)
        
    print("=" * 60)
    print("  DOME API BACKFILL AUDITOR")
    print("=" * 60)
    
    # Load all markets
    df_idx = pd.read_parquet(index_path)
    markets = df_idx.to_dict("records")
    
    print(f"  Loaded {len(markets)} markets for verification.")
    
    mismatches_found = 0
    repaired = 0
    
    for i, market in enumerate(markets):
        slug = market["slug"]
        coin = market["coin"]
        tf = market.get("timeframe", "1h") # Fallback to 1h if not in index
        
        # 1. Ask Dome API for the expected total trades
        params = {"market_slug": slug, "limit": 1}
        data = dome_get("/polymarket/orders", params)
        
        if not data:
            # Market might have zero trades, or API error. Skip for now.
            continue
            
        expected_total = data.get("pagination", {}).get("total", 0)
        
        # 2. Count local trades across all daily files for this market
        market_dir = Path(DATA_DIR) / coin / tf / slug
        if not market_dir.exists():
            local_total = 0
            local_files = []
        else:
            local_files = list(market_dir.rglob("backfill_trades.parquet"))
            local_total = 0
            for f in local_files:
                try:
                    df = pd.read_parquet(f)
                    local_total += len(df)
                except Exception:
                    pass
        
        # 3. Compare and repair
        # We allow a small tolerance of 1-3 trades in case new trades trickle in 
        # while the script is running for recently closed markets.
        if local_total >= expected_total - 5:
            if i % 50 == 0:
                print(f"  [{i+1}/{len(markets)}] {slug} -> OK ({local_total:,} trades)")
        else:
            print(f"\n  [MISMATCH DETECTED] [{i+1}/{len(markets)}] {slug}")
            print(f"    Expected: {expected_total:,} | Found: {local_total:,}")
            
            mismatches_found += 1
            
            # Delete incomplete files
            print("    [1] Deleting incomplete local files...")
            for f in local_files:
                try:
                    f.unlink()
                except Exception as e:
                    print(f"      Failed to delete {f}: {e}")
            
            # Re-run the backfill for this market
            print("    [2] Re-downloading complete trade history...")
            try:
                # We need to give it the full market dict format expected by backfill_trades
                # Make sure timeframe is explicitly set
                market["timeframe"] = tf
                new_total = backfill_trades(market)
                print(f"    [3] Repair complete! Downloaded {new_total:,} trades.")
                repaired += 1
            except Exception as e:
                print(f"    [!] Repair failed: {e}")
                
            print("-" * 60)
            
    print("\n=" * 60)
    print("  AUDIT COMPLETE")
    print(f"  Total markets checked: {len(markets)}")
    print(f"  Mismatches found: {mismatches_found}")
    print(f"  Successfully repaired: {repaired}")
    print("=" * 60)

if __name__ == "__main__":
    verify_and_repair_markets()
