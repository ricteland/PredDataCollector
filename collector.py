import asyncio
import os
import time
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich import box

import shared_state
from ws_client import main_daemon
from binance_logger import binance_ws_loop

console = Console()

def get_dir_size(path='data'):
    total = 0
    if not os.path.exists(path):
        return 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total += os.path.getsize(fp)
    # Return size in Megabytes
    return total / (1024 * 1024)

def generate_dashboard() -> Layout:
    # 1. Background Metrics Updates
    mb_saved = get_dir_size('data')
    shared_state.state['mb_saved'] = mb_saved
    
    now = time.time()
    flush_time = int(max(0, shared_state.state['next_flush_time'] - now))
    slug_time = int(max(0, shared_state.state['next_slug_update'] - now))
    
    elapsed = int(now - shared_state.state['start_time'])
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_fmt = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    # 2. Rich Layout Scaffolding
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body")
    )

    # 3. Header Segment
    header_table = Table(show_header=False, expand=True, box=None)
    header_table.add_column("1", justify="left")
    header_table.add_column("2", justify="center")
    header_table.add_column("3", justify="right")
    
    header_table.add_row(
        f"[bold cyan]Uptime:[/bold cyan] {uptime_fmt}",
        "[bold white on blue] PolyTrading Collection Engine [/bold white on blue]",
        f"[bold yellow]Vol Saved:[/bold yellow] {mb_saved:.2f} MB"
    )
    layout["header"].update(Panel(header_table))
    
    # 4. Body Metrics Table
    body_table = Table(expand=True, box=box.ROUNDED)
    body_table.add_column("Pipeline Metric", style="cyan", no_wrap=True)
    body_table.add_column("Buffer Yield", justify="right", style="green")
    
    body_table.add_row("Markets Synchronized (BTC & ETH)", str(shared_state.state['slugs_active']))
    body_table.add_row("Polymarket CLOB Trades", f"{shared_state.state['polymarket_trades']:,}")
    body_table.add_row("Polymarket CLOB Tick Updates", f"{shared_state.state['polymarket_ticks']:,}")
    body_table.add_row("Polymarket Book Snapshots", f"{shared_state.state['polymarket_snapshots']:,}")
    body_table.add_row("Binance Oracle Trades", f"{shared_state.state['binance_trades']:,}")
    body_table.add_row("Binance Oracle Best Bid/Ask", f"{shared_state.state['binance_ticks']:,}")
    
    body_table.add_row("", "")
    body_table.add_row("[yellow]Next Disk Flush In[/yellow]", f"[yellow]{flush_time}s[/yellow]")
    body_table.add_row("[magenta]Next Node.js API Slugs Rotation In[/magenta]", f"[magenta]{slug_time}s[/magenta]")

    layout["body"].update(Panel(body_table, title="[bold]Concurrent Telemetry Mappings[/bold]"))
    
    return layout

async def ui_loop():
    # Renders the exact UI overlay twice a second indefinitely
    with Live(generate_dashboard(), console=console, refresh_per_second=2, screen=True) as live:
        while True:
            await asyncio.sleep(0.5)
            live.update(generate_dashboard())

async def run_orchestration():
    # 1. Background daemon pulling active CLOB levels
    task_clob = asyncio.create_task(main_daemon())
    # 2. Background daemon pulling Spot executions
    task_spot = asyncio.create_task(binance_ws_loop())
    # 3. Foreground visualization rendering shared telemetry
    task_ui = asyncio.create_task(ui_loop())
    
    try:
        await asyncio.gather(task_clob, task_spot, task_ui)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(run_orchestration())
    except KeyboardInterrupt:
        pass
