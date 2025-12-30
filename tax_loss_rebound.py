"""
Tax Loss Rebound - BASKET RUNNER (Queue-Based Logger)
Revised Features:
- Auto-Reconnection (Survives TWS Restarts)
- Telegram Alerts (Entries, Exits, Disconnects, Scan Results)
- ISOLATED STATE: Uses JSON to remember the specific basket of stocks selected.
- QUEUE LOGGER: Thread-safe CSV writing for high performance.
"""

import os
import sys
import json
import time
import threading
import queue
import math
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

import pandas as pd
import datetime as dt

from ib_insync import (
    IB, Stock, MarketOrder, Trade, Contract, Ticker
)

# ─────────────────────────────────────────────────────────────
# PATH & CLIENT ID MANAGER
# ─────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.client_id_manager import get_or_allocate_client_id, bump_client_id
# [NEW] Telegram Import
from utils.telegram_alert import send_alert

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
APP_NAME = "Tax_Loss_Rebound"

# Connection
HOST = "127.0.0.1"
PORT = 7497         
ACCOUNT_ID = "DU3188670"

# Strategy Dates
ENTRY_START_DATE = "2025-12-20"
EXIT_START_DATE = "2026-01-15"

# Capital & Risk
PERCENTAGE_PER_STOCK = 0.02      
MAX_CAPITAL_PER_TRADE_USD = 2000.0 
MIN_QTY = 1
LOSERS_THRESHOLD = -0.20          
TOP_N_TARGETS = 5

# Logs
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_ROOT = os.path.join(BASE_DIR, "logs", APP_NAME)
os.makedirs(LOG_ROOT, exist_ok=True)

TRADE_LOG_PATH = os.path.join(LOG_ROOT, "trade_log.csv")
HEARTBEAT_PATH = os.path.join(LOG_ROOT, "heartbeat.json")
STATUS_LOG_PATH = os.path.join(LOG_ROOT, "status.log")
# [NEW] Basket State File
STATE_FILE = os.path.join(LOG_ROOT, "state_Tax_Loss_Basket.json")

CLIENT_ID = get_or_allocate_client_id(name=APP_NAME, role="strategy", preferred=None)


# ─────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────
@dataclass
class TradeRow:
    timestamp: dt.datetime
    symbol: str
    action: str
    price: float
    quantity: int
    pnl: float
    duration: float
    position: str
    status: str
    ib_order_id: int
    extra: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PositionState:
    symbol: str
    status: str = "NONE"      # NONE | LONG
    qty: int = 0
    entry_price: Optional[float] = None
    entry_time: Optional[dt.datetime] = None


# ─────────────────────────────────────────────────────────────
# HELPER LOGGER
# ─────────────────────────────────────────────────────────────
def log_status(msg: str) -> None:
    # Use timezone-aware UTC
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}][{APP_NAME}] {msg}"
    print(line)
    try:
        with open(STATUS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────
# STATE MANAGEMENT (JSON)
# ─────────────────────────────────────────────────────────────
def load_basket_state() -> Dict[str, Any]:
    """Loads the specific basket of stocks and their positions."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log_status(f"⚠️ Failed to load basket state: {e}")
        return {}

def save_basket_state(positions: Dict[str, PositionState]):
    """Saves the entire basket state to disk."""
    data = {}
    for sym, state in positions.items():
        data[sym] = {
            "status": state.status,
            "qty": state.qty,
            "entry_price": state.entry_price,
            "entry_time": state.entry_time.isoformat() if state.entry_time else None
        }
    
    wrapper = {
        "last_updated": dt.datetime.now().isoformat(),
        "positions": data
    }
    
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(wrapper, f, indent=2)
    except Exception as e:
        log_status(f"⚠️ Failed to save basket state: {e}")


# ─────────────────────────────────────────────────────────────
# SCANNER LOGIC
# ─────────────────────────────────────────────────────────────
def scan_for_basket(ib_conn: IB, top_n: int) -> List[str]:
    log_status(f"Scanning for Top {top_n} candidates using IBKR Data...")
    
    # Load Tickers
    csv_filename = os.path.join(BASE_DIR, "russell2000_tickers.csv")
    ticker_list = ["AMC", "GME", "PLUG", "DKNG", "MSTR", "COIN", "CVNA", "UPST", "AFRM", "SOFI", "NVDA", "TSLA"]
    
    if os.path.exists(csv_filename):
        try:
            df = pd.read_csv(csv_filename)
            col_name = next((c for c in ['Ticker', 'Symbol'] if c in df.columns), df.columns[0])
            ticker_list = df[col_name].astype(str).str.strip().tolist()
        except: pass

    basket = []
    
    # Simple Progress tracking
    total = len(ticker_list)
    for i, t in enumerate(ticker_list):
        if i % 50 == 0: log_status(f"Scanning... {i}/{total}")
        
        contract = Stock(t, "SMART", "USD")
        try:
            bars = ib_conn.reqHistoricalData(
                contract, endDateTime='', durationStr='1 Y', barSizeSetting='1 day',
                whatToShow='TRADES', useRTH=True, formatDate=1, keepUpToDate=False
            )
            if not bars: continue

            start_price = bars[0].close
            end_price = bars[-1].close
            if start_price == 0: continue
            
            perf = (end_price - start_price) / start_price
            
            if perf < LOSERS_THRESHOLD:
                log_status(f"Found Candidate: {t} ({perf:.2%})")
                basket.append(t)
            
            if len(basket) >= top_n: break
            time.sleep(0.1) # Throttle
        except Exception:
            pass

    if not basket:
        log_status("No tickers met the loser criteria.")
        return []

    msg = f"🎯 Basket Selected: {basket}"
    log_status(msg)
    send_alert(msg, APP_NAME)
    return basket


# ─────────────────────────────────────────────────────────────
# STRATEGY RUNNER
# ─────────────────────────────────────────────────────────────
class StrategyRunner:
    def __init__(self) -> None:
        self.ib = IB()
        
        # BASKET MANAGEMENT
        self.symbols: List[str] = []
        self.contracts: Dict[str, Contract] = {}
        self.tickers: Dict[str, Ticker] = {}
        self.positions: Dict[str, PositionState] = {}

        # CAPITAL MANAGEMENT
        self.account_cash = 0.0
        
        # DATES
        self.entry_start = dt.datetime.strptime(ENTRY_START_DATE, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
        self.exit_start = dt.datetime.strptime(EXIT_START_DATE, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)

        # LOGGING SYSTEM (Queue)
        self.log_queue = queue.Queue()
        self._stop_requested = False
        self._logged_order_ids: Dict[int, bool] = {}

        # Start the background logger thread
        self.logger_thread = threading.Thread(target=self._logger_worker, daemon=True)
        self.logger_thread.start()
        
        # Restore Basket State immediately
        self._restore_state()

    # ─────────────────────────────
    # UTILS
    # ─────────────────────────────
    def _now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)

    def _logger_worker(self):
        """Background thread for non-blocking CSV writing."""
        while not self._stop_requested:
            try:
                record = self.log_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                row_dict = {
                    "timestamp": record.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": record.symbol, "action": record.action,
                    "price": record.price, "quantity": record.quantity,
                    "pnl": record.pnl, "duration": record.duration,
                    "position": record.position, "status": record.status,
                    "ib_order_id": record.ib_order_id,
                    "extra": json.dumps(record.extra) if record.extra else None,
                }
                df_new = pd.DataFrame([row_dict])

                if os.path.exists(TRADE_LOG_PATH):
                    try: df_old = pd.read_csv(TRADE_LOG_PATH)
                    except: df_old = pd.DataFrame()
                    df = pd.concat([df_old, df_new], ignore_index=True)
                else:
                    df = df_new

                if not df.empty:
                    df["dedup_key"] = (
                        df["timestamp"].astype(str) + "|" + df["symbol"].astype(str)
                        + "|" + df["action"].astype(str) + "|" + df["ib_order_id"].astype(str)
                    )
                    df = df.drop_duplicates(subset=["dedup_key"]).drop(columns=["dedup_key"])

                df.to_csv(TRADE_LOG_PATH, index=False)
                self.log_queue.task_done()
            except Exception as e:
                print(f"CRITICAL LOGGER ERROR: {e}")

    def _write_heartbeat(self, status: str = "running") -> None:
        pos_summary = {sym: s.qty for sym, s in self.positions.items() if s.qty > 0}
        data = {
            "app_name": APP_NAME,
            "status": status,
            "last_update": self._now().isoformat(),
            "cash_base": self.account_cash,
            "active_positions": pos_summary
        }
        try:
            with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception: pass

    # ─────────────────────────────
    # STATE MANAGEMENT
    # ─────────────────────────────
    def _restore_state(self):
        saved_data = load_basket_state()
        if not saved_data or "positions" not in saved_data:
            log_status("No previous basket state found.")
            return

        positions_data = saved_data["positions"]
        self.symbols = list(positions_data.keys())
        
        log_status("♻️ Restoring Basket State...")
        log_status(f"Last updated: {saved_data.get('last_updated', 'Unknown')}")
        
        has_positions = False
        for sym, p_data in positions_data.items():
            state = PositionState(sym)
            state.status = p_data.get("status", "NONE")
            state.qty = int(p_data.get("qty", 0))
            state.entry_price = p_data.get("entry_price")
            
            t_str = p_data.get("entry_time")
            if t_str:
                try: state.entry_time = dt.datetime.fromisoformat(t_str)
                except: pass
            
            self.positions[sym] = state
            self.contracts[sym] = Stock(sym, "SMART", "USD")
            
            if state.status == "LONG" and state.qty > 0:
                has_positions = True
                entry_info = f" @ ${state.entry_price:.2f}" if state.entry_price else ""
                log_status(f"   -> {sym}: LONG {state.qty} shares{entry_info}")
        
        if not has_positions:
            log_status("   -> No active positions found in state.")
        
        log_status(f"♻️ Restored basket with {len(self.symbols)} symbols: {self.symbols}")

    # ─────────────────────────────
    # TRADING LOGIC
    # ─────────────────────────────
    def on_account_summary(self, val: Any):
        if val.tag == "TotalCashBalance" and val.currency == "BASE":
            try:
                self.account_cash = float(val.value)
            except: pass

    def _qty_for_symbol(self, price: float) -> int:
        if price <= 0 or self.account_cash <= 0: return 0
        alloc_amt = self.account_cash * PERCENTAGE_PER_STOCK
        if alloc_amt > MAX_CAPITAL_PER_TRADE_USD:
            alloc_amt = MAX_CAPITAL_PER_TRADE_USD
        qty = int(alloc_amt // price)
        return qty if qty >= MIN_QTY else 0

    def check_signal(self, symbol: str) -> Optional[str]:
        state = self.positions.get(symbol)
        if not state: return None
        now = self._now()
        
        if now >= self.exit_start:
            if state.status == "LONG":
                log_status(f"[{symbol}] Exit Date Reached. Selling.")
                return "SELL"
            return None 

        if state.status == "NONE":
            # Only buy if we are in the entry window (Same Year as start)
            if (now >= self.entry_start) and (now.year == self.entry_start.year):
               return "BUY"
        return None

    def _place_order(self, symbol: str, action: str, price: float):
        state = self.positions[symbol]
        
        if action == "BUY" and state.status != "NONE": return
        if action == "SELL" and state.status == "NONE": return

        qty = 0
        if action == "BUY":
            qty = self._qty_for_symbol(price)
            if qty == 0: 
                log_status(f"[{symbol}] Insufficient capital. Skipping.")
                return
        elif action == "SELL":
            qty = state.qty

        contract = self.contracts[symbol]
        order = MarketOrder(action, qty)
        if ACCOUNT_ID: order.account = ACCOUNT_ID
        order.orderRef = APP_NAME

        trade = self.ib.placeOrder(contract, order)
         
        # Optimistic Update
        if action == "BUY": state.status = "LONG"
        elif action == "SELL": state.status = "NONE"

        trade.fillEvent += self._on_trade_update
        trade.statusEvent += self._on_order_status
        
        msg = f"🚀 <b>[{APP_NAME}]</b> {action} {qty} {symbol} @ ~{price:.2f}"
        log_status(msg)
        send_alert(msg, APP_NAME)

    # ─────────────────────────────
    # EVENTS
    # ─────────────────────────────
    def _on_tick(self, ticker: Ticker):
        if self._stop_requested: return
        symbol = ticker.contract.symbol
        price = ticker.last or ticker.close
        if not price or price <= 0: return

        action = self.check_signal(symbol)
        if action:
            self._place_order(symbol, action, float(price))

        self._write_heartbeat()

    def _on_order_status(self, trade: Trade):
        if trade.orderStatus.status in ('Cancelled', 'Inactive', 'ApiCancelled'):
            symbol = trade.contract.symbol
            state = self.positions.get(symbol)
            if state and state.status != "NONE":
                log_status(f"⚠️ Order {trade.order.orderId} Cancelled. Resetting state.")
                state.status = "NONE"
                state.qty = 0
                state.entry_price = None
                save_basket_state(self.positions)

    def _on_trade_update(self, trade: Trade, *args):
        fill = args[0] if args else None
        if fill:
            fill_price = fill.execution.price
            filled_qty = fill.execution.shares
        else:
            if not trade.orderStatus.filled: return
            fill_price = trade.orderStatus.avgFillPrice
            filled_qty = trade.orderStatus.filled

        if filled_qty <= 0 or fill_price <= 0: return

        oid = trade.order.orderId
        if self._logged_order_ids.get(oid): return
        self._logged_order_ids[oid] = True
        
        symbol = trade.contract.symbol
        action = trade.order.action.upper()
        state = self.positions.get(symbol)
        if not state: return 

        now = self._now()
        pnl = 0.0
        duration = 0.0

        if action == "BUY":
            state.qty = int(filled_qty)
            state.entry_price = float(fill_price)
            state.entry_time = now
            state.status = "LONG"
        elif action == "SELL":
            if state.entry_price:
                pnl = (fill_price - state.entry_price) * filled_qty
            if state.entry_time:
                duration = (now - state.entry_time).total_seconds()
            state.qty = 0
            state.entry_price = None
            state.status = "NONE"

        # SAVE BASKET STATE
        save_basket_state(self.positions)

        row = TradeRow(
            timestamp=now.replace(tzinfo=None), symbol=symbol, action=action,
            price=fill_price, quantity=int(filled_qty), pnl=pnl, duration=duration,
            position=state.status, status=trade.orderStatus.status, ib_order_id=oid
        )
        self.log_queue.put(row)
        
        msg = f"✅ <b>[{APP_NAME}]</b> FILL: {symbol} {action} {filled_qty} @ {fill_price:.2f} (PnL=${pnl:.2f})"
        log_status(msg)
        send_alert(msg, APP_NAME)

    # ─────────────────────────────
    # MAIN EXECUTION
    # ─────────────────────────────
    def _setup_basket(self):
        # If we already have symbols from state, skip scanning
        if self.symbols:
            log_status(f"♻️ Using restored basket: {self.symbols}")
        else:
            self.symbols = scan_for_basket(self.ib, TOP_N_TARGETS)
        
        if not self.symbols:
            log_status("Scan returned no candidates. Stopping.")
            self.stop()
            return

        for s in self.symbols:
            if s not in self.positions:
                self.positions[s] = PositionState(s)
            self.contracts[s] = Stock(s, "SMART", "USD")

        log_status("Qualifying contracts...")
        for sym, contract in self.contracts.items():
            try:
                self.ib.qualifyContracts(contract)
                t = self.ib.reqMktData(contract, "", False, False)
                t.updateEvent += self._on_tick
                self.tickers[sym] = t
            except Exception as e:
                log_status(f"Failed to setup {sym}: {e}")

        log_status(f"✅ Setup Complete. Monitoring: {self.symbols}")

    def run(self) -> None:
        global CLIENT_ID
        send_alert(f"🚀 <b>[{APP_NAME}]</b> Started.", APP_NAME)
        
        while not self._stop_requested:
            try:
                # 1. CONNECT
                if not self.ib.isConnected():
                    log_status(f"Connecting to {HOST}:{PORT} (ID: {CLIENT_ID})...")
                    try:
                        self.ib.connect(HOST, PORT, clientId=CLIENT_ID, readonly=False)
                        log_status("✅ Connected to Interactive Brokers")
                        send_alert(f"✅ <b>[{APP_NAME}]</b> Connected (ID: {CLIENT_ID})", APP_NAME)
                    except Exception as e:
                        if "already in use" in str(e).lower():
                            CLIENT_ID = bump_client_id(APP_NAME, "strategy")
                        else:
                            log_status(f"❌ Connection failed: {e}")
                        time.sleep(10)
                        continue

                # 2. INITIALIZATION (Once per session)
                if not self.symbols: # If not yet setup
                    self.ib.reqAccountSummary()
                    self.ib.accountSummaryEvent += self.on_account_summary
                    
                    # Wait for Entry Date
                    now = self._now()
                    if now < self.entry_start:
                        log_status(f"⏳ Waiting for Entry Date: {ENTRY_START_DATE}")
                        self.ib.sleep(60) # Sleep and check connection
                        continue
                    
                    self._setup_basket()
                
                # Resubscribe if we reconnected
                for sym, contract in self.contracts.items():
                    self.ib.reqMktData(contract, "", False, False)

                # 3. MONITOR LOOP
                while self.ib.isConnected():
                    if self._stop_requested: break
                    self.ib.waitOnUpdate(timeout=1.0)
                    self._write_heartbeat("running")

            except Exception as e:
                err_msg = f"⚠️ <b>[{APP_NAME}]</b> CRITICAL DISCONNECT!\nError: {str(e)}"
                log_status(err_msg)
                send_alert(err_msg, APP_NAME)
                self._write_heartbeat("disconnected")

            finally:
                if self.ib.isConnected(): self.ib.disconnect()
                if not self._stop_requested:
                    log_status("🔄 Reconnecting in 10s...")
                    time.sleep(10)

        log_status("🛑 Bot Stopped.")
        send_alert(f"🛑 <b>[{APP_NAME}]</b> Bot Stopped.", APP_NAME)

    def stop(self) -> None:
        self._stop_requested = True

if __name__ == "__main__":
    StrategyRunner().run()