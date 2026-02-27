import asyncio
import os
import sys
import time
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import Header, Footer, Static, DataTable, Label, Digits
from textual import work

# ── Allow running directly as `python weather/weather_collector.py` ──────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _HERE)

import weather_shared_state
from weather_ws_client import main_daemon

WEATHER_DATA_DIR = os.path.join(_ROOT, "data", "weather")

def get_dir_size():
    total = 0
    if not os.path.exists(WEATHER_DATA_DIR):
        return 0
    for dirpath, dirnames, filenames in os.walk(WEATHER_DATA_DIR):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                if not os.path.islink(fp):
                    total += os.path.getsize(fp)
            except OSError:
                pass
    return total / (1024 * 1024)

class MetricBox(Static):
    def __init__(self, title: str, id: str):
        super().__init__(id=id)
        self.title_str = title
        
    def compose(self) -> ComposeResult:
        yield Label(self.title_str, classes="metric-title")
        yield Digits("0", id=f"{self.id}-value", classes="metric-value")
        
    def update_val(self, new_val):
        self.query_one(f"#{self.id}-value", Digits).update(str(new_val))


class WeatherDashboard(App):
    CSS = """
    Screen {
        background: $surface;
    }

    #metrics-container {
        layout: horizontal;
        height: 8;
        dock: top;
        margin-bottom: 1;
    }

    MetricBox {
        width: 1fr;
        height: 100%;
        border: solid $accent;
        background: $panel;
        padding: 1;
        content-align: center middle;
    }
    
    .metric-title {
        text-align: center;
        width: 100%;
        color: $text-muted;
    }
    
    .metric-value {
        text-align: center;
        width: 100%;
        color: $text;
    }

    DataTable {
        height: 1fr;
        border: solid $primary;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="metrics-container"):
            yield MetricBox("Volume Saved (MB)", "mb-saved")
            yield MetricBox("Active Tokens", "active-tokens")
            yield MetricBox("CLOB Trades", "clob-trades")
            yield MetricBox("CLOB Ticks", "clob-ticks")
            yield MetricBox("Book Snapshots", "clob-snapshots")
            
        self.table = DataTable(cursor_type="row")
        yield self.table
        yield Footer()

    def on_mount(self) -> None:
        self.title = "Polymarket Weather Collector Engine"
        self.table.add_columns("City", "Question", "End Date", "Live Trades")
        self.set_interval(1.0, self.update_dashboard)
        self.run_daemon()

    @work(thread=True)
    def run_daemon(self):
        asyncio.run(main_daemon())

    def update_dashboard(self) -> None:
        mb_saved = get_dir_size()
        weather_shared_state.state['mb_saved'] = mb_saved
        
        self.query_one("#mb-saved").update_val(f"{mb_saved:.2f}")
        self.query_one("#active-tokens").update_val(f"{weather_shared_state.state['slugs_active']}")
        self.query_one("#clob-trades").update_val(f"{weather_shared_state.state['polymarket_trades']:,}")
        self.query_one("#clob-ticks").update_val(f"{weather_shared_state.state['polymarket_ticks']:,}")
        self.query_one("#clob-snapshots").update_val(f"{weather_shared_state.state['polymarket_snapshots']:,}")
        
        markets = weather_shared_state.state.get('markets', {})
        existing_keys = [row_key.value for row_key in self.table.rows]
        active_keys = []
        
        for cid, info in markets.items():
            active_keys.append(cid)
            row_data = (
                info['city'].upper(),
                info['question'],
                str(info['end_date']),
                f"{info['trades']:,}"
            )
            
            if cid in existing_keys:
                for col_idx, val in enumerate(row_data):
                    self.table.update_cell(cid, col_idx, val)
            else:
                self.table.add_row(*row_data, key=cid)
                
        for key in list(existing_keys):
            if key not in active_keys:
                try:
                    self.table.remove_row(key)
                except Exception:
                    pass

if __name__ == "__main__":
    app = WeatherDashboard()
    app.run()
