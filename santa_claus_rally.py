import os
import sys
import json
import time
import threading
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

import pandas as pd
import numpy as np
import datetime as dt

# [Requirement] Do NOT import AccountSummary explicitly to avoid conflicts
from ib_insync import IB, Stock, MarketOrder, Trade

# ─────────────────────────────────────────────────────────────
# PATH & CLIENT ID MANAGER WIRES
# ─────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.client_id_manager import get_or_allocate_client_id, bump_client_id
from utils.telegram_alert import send_alert

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
APP_NAME = "Santa_Claus_Rally"

HOST = "127.0.0.1"
PORT = 7497     
ACCOUNT_ID = "DU3188670"    

SYMBOL = "SPY"
EXCHANGE = "SMART"
CURRENCY = "USD"

# ─── CAPITAL SIZING SETTINGS ───
CAPITAL_MODE = "PCT"        # "FIXED" or "PCT"
FIXED_CAPITAL_AMOUNT = 100000.0
CAPITAL_PCT = 0.02          
MIN_QTY = 1

COOLDOWN_SEC = 60
MIN_SAME_ACTION_REPRICE = 0.003

# Logs:
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_ROOT = os.path.join(BASE_DIR, "logs", APP_NAME)
os.makedirs(LOG_ROOT, exist_ok=True)

TRADE_LOG_PATH = os.path.join(LOG_ROOT, "trade_log.csv")
HEARTBEAT_PATH = os.path.join(LOG_ROOT, "heartbeat.json")
STATUS_LOG_PATH = os.path.join(LOG_ROOT, "status.log")
STATE_FILE = os.path.join(LOG_ROOT, "state_Santa_Claus.json") 

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


# ─────────────────────────────────────────────────────────────
# HELPER LOGGER
# ─────────────────────────────────────────────────────────────
def log_status(msg: str) -> None:
    # Uses UTC for logs to match server time standards
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts} UTC][{APP_NAME}] {msg}"
    print(line)
    try:
        with open(STATUS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────
# STATE MANAGEMENT (JSON)
# ─────────────────────────────────────────────────────────────
def load_state() -> Dict[str, Any]:
    """Loads the isolated position state for THIS bot only."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        log_status(f"⚠️ Failed to load state file: {e}")
        return {}

def save_state(position: str, qty: int, entry_price: float, entry_time: Optional[dt.datetime]):
    """Saves the current position to disk."""
    data = {
        "current_position": position,
        "current_qty": qty,
        "entry_price": entry_price,
        "entry_time": entry_time.isoformat() if entry_time else None,
        "last_updated": dt.datetime.now().isoformat()
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log_status(f"⚠️ Failed to save state: {e}")

# ─────────────────────────────────────────────────────────────
# STRATEGY RUNNER
# ─────────────────────────────────────────────────────────────
class StrategyRunner:
    def __init__(self) -> None:
        self.ib = IB()
        self.contract = Stock(SYMBOL, EXCHANGE, CURRENCY)

        # Internal State
        self.current_position: str = "NONE"
        self.current_qty: int = 0
        self.entry_price: Optional[float] = None
        self.entry_time: Optional[dt.datetime] = None

        # [NEW] LIVE CASH CACHE (Initialized to 0.0)
        self.cached_cash_balance: float = 0.0

        # Pending Order Lock
        self.pending_order: bool = False
        self.pending_action: Optional[str] = None

        # Throttling
        self.last_trade_time: Optional[dt.datetime] = None
        self.last_action: Optional[str] = None
        
        # Date Tracking
        self.last_buy_date: Optional[str] = None
        self.last_sell_date: Optional[str] = None

        # Tick Debouncing
        self.last_tick_check: Optional[dt.datetime] = None
        self.tick_throttle_sec = 1.0 

        self.trade_log_buffer: List[TradeRow] = []
        self.lock = threading.Lock()

        self._ticker = None
        self._stop_requested = False
        self._logged_order_ids: Dict[int, bool] = {}
        self.prices: List[float] = []
        
        # Load State immediately on Init
        self._restore_state()

    def _restore_state(self):
        state = load_state()
        if state and state.get("current_position") == "LONG":
            self.current_position = "LONG"
            self.current_qty = int(state.get("current_qty", 0))
            self.entry_price = float(state.get("entry_price", 0.0))
            
            t_str = state.get("entry_time")
            if t_str:
                try:
                    self.entry_time = dt.datetime.fromisoformat(t_str)
                except: pass
            
            log_status(f"♻️ RESTORED STATE: LONG {self.current_qty} {SYMBOL} @ {self.entry_price}")
        else:
            log_status("ℹ️ No active position in State File.")

    # ─────────────────────────────────────────────────────────
    # FILE IO
    # ─────────────────────────────────────────────────────────
    def _flush_trade_log_buffer(self) -> None:
        with self.lock:
            if not self.trade_log_buffer: return
            rows = []
            for r in self.trade_log_buffer:
                rows.append({
                    "timestamp": r.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    "symbol": r.symbol, "action": r.action, "price": r.price,
                    "quantity": r.quantity, "pnl": r.pnl, "duration": r.duration,
                    "position": r.position, "status": r.status, "ib_order_id": r.ib_order_id,
                    "extra": json.dumps(r.extra) if r.extra else None,
                })
            df_new = pd.DataFrame(rows)
            self.trade_log_buffer.clear()

        if os.path.exists(TRADE_LOG_PATH):
            try:
                df_old = pd.read_csv(TRADE_LOG_PATH)
            except Exception:
                df_old = pd.DataFrame()
            df = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df = df_new

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        df["dedup_key"] = (
            df["timestamp"].astype(str) + "|" + df["symbol"].astype(str)
            + "|" + df["action"].astype(str) + "|" + df["ib_order_id"].astype(str)
        )
        df = df.drop_duplicates(subset=["dedup_key"]).drop(columns=["dedup_key"])
        if "timestamp" in df.columns:
            df = df.sort_values("timestamp")
        df.to_csv(TRADE_LOG_PATH, index=False)

    def _write_heartbeat(self, status: str = "running", last_price: Optional[float] = None) -> None:
        data = {
            "app_name": APP_NAME,
            "symbol": SYMBOL,
            "status": status,
            "last_update": dt.datetime.now(dt.timezone.utc).isoformat(),
            "position": self.current_position,
            "position_qty": self.current_qty,
            "entry_price": self.entry_price,
            "last_price": last_price,
            "account_cash_base": self.cached_cash_balance,
            "pending_order": self.pending_order
        }
        try:
            with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────
    # SAFETY & SIZING
    # ─────────────────────────────────────────────────────────
    def _now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    def _can_trade(self, action: str, price: float) -> bool:
        now = self._now()
        
        if self.pending_order:
            log_status(f"BLOCKED: Pending {self.pending_action} order exists")
            return False
        
        if self.last_trade_time is not None:
            elapsed = (now - self.last_trade_time).total_seconds()
            if elapsed < COOLDOWN_SEC:
                log_status(f"BLOCKED: Cooldown active ({elapsed:.1f}s < {COOLDOWN_SEC}s)")
                return False
        
        if action == "BUY" and self.current_position == "LONG":
            log_status(f"BLOCKED: Already LONG with {self.current_qty} shares")
            return False
        elif action == "SELL" and self.current_position == "NONE":
            log_status(f"BLOCKED: No position to SELL")
            return False
        
        return True

    def _qty_for_price(self, price: float) -> int:
        if price <= 0: return 0
        
        capital_to_use = 0.0
        if CAPITAL_MODE == "FIXED":
            capital_to_use = FIXED_CAPITAL_AMOUNT
        elif CAPITAL_MODE == "PCT":
            # [FIX] Use the cached stream variable
            if self.cached_cash_balance <= 0: 
                log_status("⚠️ Cash Balance is 0 or not loaded. Qty = 0.")
                return 0
            capital_to_use = self.cached_cash_balance * CAPITAL_PCT

        max_qty = int(capital_to_use // price)
        
        if max_qty < MIN_QTY: 
            return 0
            
        return max_qty

    # ─────────────────────────────────────────────────────────
    # DATE UTILITIES
    # ─────────────────────────────────────────────────────────
    def _get_trading_days_remaining_in_year(self, current_date: dt.datetime) -> int:
        end_of_year = dt.datetime(current_date.year, 12, 31)
        days = pd.bdate_range(start=current_date, end=end_of_year)
        return len(days)

    def _get_trading_days_elapsed_in_jan(self, current_date: dt.datetime) -> int:
        start_of_year = dt.datetime(current_date.year, 1, 1)
        days = pd.bdate_range(start=start_of_year, end=current_date)
        return len(days)

    # ─────────────────────────────────────────────────────────
    # STRATEGY LOGIC
    # ─────────────────────────────────────────────────────────
    def compute_signal(self, price: float) -> Optional[str]:
        now = dt.datetime.now()
        today_key = now.strftime("%Y-%m-%d")
        
        # ─────────────────────────────────────────────────────────────
        # [ACTIVE] TEST MODE: Specific Dates (Jan 2 Entry / Jan 5 Exit)
        # ─────────────────────────────────────────────────────────────
        """
        # 1. TEST ENTRY: Force Buy on Jan 2
        if now.month == 1 and now.day == 2:
            if self.current_position == "NONE":
                if self.last_buy_date == today_key: return None
                log_status(f"SIGNAL: TEST BUY triggered (Date is Jan 2)")
                return "BUY"

        # 2. TEST EXIT: Force Sell on Jan 5
        elif now.month == 1 and now.day == 5:
            if self.current_position == "LONG":
                if self.last_sell_date == today_key: return None
                log_status(f"SIGNAL: TEST SELL triggered (Date is Jan 5)")
                return "SELL"
        """
        # ─────────────────────────────────────────────────────────────
        # ORIGINAL LOGIC (Commented out for testing)
        # ─────────────────────────────────────────────────────────────
        # # 1. Check Entry (December)
        if now.month == 12:
            if now.day >= 15: 
                days_left = self._get_trading_days_remaining_in_year(now)
                ENTRY_DAYS_LEFT = 5
                if days_left == ENTRY_DAYS_LEFT and self.current_position == "NONE":
                    if self.last_buy_date == today_key: return None
                    return "BUY"

        # # 2. Check Exit (January)
        elif now.month == 1:
            if now.day <= 10:
                jan_days = self._get_trading_days_elapsed_in_jan(now)
                if jan_days >= 2 and self.current_position == "LONG":
                    if self.last_sell_date == today_key: return None
                    return "SELL"
        
        return None

    # ─────────────────────────────────────────────────────────
    # ORDER & EXECUTION
    # ─────────────────────────────────────────────────────────
    def _place_order(self, action: str, qty: int, price: float) -> None:
        with self.lock:
            if self.pending_order: return
            self.pending_order = True
            self.pending_action = action
            
            today = dt.datetime.now().strftime("%Y-%m-%d")
            if action == "BUY": self.last_buy_date = today
            elif action == "SELL": self.last_sell_date = today
        
        order = MarketOrder(action, int(qty))
        if ACCOUNT_ID: order.account = ACCOUNT_ID
        order.orderRef = APP_NAME

        trade: Trade = self.ib.placeOrder(self.contract, order)
        
        trade.fillEvent += lambda *args: self._on_trade_update(trade)
        trade.statusEvent += lambda *args: self._on_trade_update(trade)

        self.last_trade_time = self._now()
        self.last_action = action
        
        msg = f"🚀 <b>[{APP_NAME}]</b> {action} x{qty} sent @ ~{price:.2f}"
        log_status(msg)
        send_alert(msg, APP_NAME)

    def _on_trade_update(self, trade: Trade) -> None:
        status = getattr(trade.orderStatus, "status", None)
        avg_price = getattr(trade.orderStatus, "avgFillPrice", None)
        filled = getattr(trade.orderStatus, "filled", None)
        oid = getattr(trade.order, "orderId", None)

        if not oid or not status: return
        if self._logged_order_ids.get(oid): return
        if status.lower() not in ("filled", "partiallyfilled"): return
        if not avg_price or avg_price <= 0 or not filled: return

        action = trade.order.action.upper()
        qty = int(filled)
        price = float(avg_price)
        now = self._now()
        pnl = 0.0
        duration = 0.0
        pos_after = self.current_position

        if action == "BUY":
            self.current_position = "LONG"
            self.current_qty = qty
            self.entry_price = price
            self.entry_time = now
            pos_after = "LONG"
            save_state("LONG", qty, price, now)
            
        elif action == "SELL":
            if self.current_position == "LONG" and self.entry_price:
                pnl = (price - float(self.entry_price)) * qty
            if self.entry_time:
                duration = (now - self.entry_time).total_seconds()
            self.current_position = "NONE"
            self.current_qty = 0
            self.entry_price = None
            self.entry_time = None
            pos_after = "NONE"
            save_state("NONE", 0, 0.0, None)
        
        with self.lock:
            self.pending_order = False
            self.pending_action = None

        row = TradeRow(now, SYMBOL, action, price, qty, pnl, duration, pos_after, status, oid, {})
        with self.lock: self.trade_log_buffer.append(row)
        self._flush_trade_log_buffer()
        self._logged_order_ids[oid] = True
        
        msg = f"✅ <b>[{APP_NAME}]</b> FILLED: {action} x{qty} @ {price:.2f} (PnL=${pnl:.2f})"
        log_status(msg)
        send_alert(msg, APP_NAME)

    # ─────────────────────────────────────────────────────────
    # ACCOUNT STREAM HANDLER (NEW PATTERN)
    # ─────────────────────────────────────────────────────────
    def _on_account_summary(self, val):
        if val.tag == "TotalCashBalance":
            if val.currency == "BASE" or val.currency == CURRENCY:
                try:
                    self.cached_cash_balance = float(val.value)
                except ValueError: 
                    pass

    # ─────────────────────────────────────────────────────────
    # MARKET DATA & MAIN TICK LOGIC
    # ─────────────────────────────────────────────────────────
    def _on_tick(self, _=None) -> None:
        if self._stop_requested or not self._ticker: return

        now = self._now()
        if self.last_tick_check and (now - self.last_tick_check).total_seconds() < self.tick_throttle_sec:
            return 
        self.last_tick_check = now

        price = (self._ticker.last or self._ticker.marketPrice() or self._ticker.close or 0.0)
        
        if (price != price) or price <= 0: 
            return

        self.prices.append(price)
        if len(self.prices) > 100: self.prices = self.prices[-100:]

        action = self.compute_signal(price)
        if not action:
            self._write_heartbeat(status="running", last_price=price)
            return

        if not self._can_trade(action, price):
            self._write_heartbeat(status="waiting", last_price=price)
            return

        # ─────────────────────────────────────────────────────────────
        # [FIX] USE CORRECT QUANTITY LOGIC FOR ENTRIES VS EXITS
        # ─────────────────────────────────────────────────────────────
        qty = 0
        if action == "BUY":
            qty = self._qty_for_price(price)   # Calculate size based on Capital
        elif action == "SELL":
            qty = self.current_qty             # Sell EVERYTHING we own (from JSON state)
        
        if qty > 0:
            self._place_order(action, qty, price)
        else:
            log_status(f"⚠️ Signal {action} ignored because Qty is 0 (Check Balance or Position State).")

    # ─────────────────────────────────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────────────────────────────────
    def run(self) -> None:
        global CLIENT_ID
        
        send_alert(f"🚀 <b>[{APP_NAME}]</b> Started.\nMode: {CAPITAL_MODE}", APP_NAME)
        
        while not self._stop_requested:
            try:
                # 1. CONNECT
                if not self.ib.isConnected():
                    log_status(f"Connecting to {HOST}:{PORT} (ID: {CLIENT_ID})...")
                    try:
                        self.ib.connect(HOST, PORT, clientId=CLIENT_ID, readonly=False)
                        self.ib.qualifyContracts(self.contract)
                        log_status("✅ Connected")
                        send_alert(f"✅ <b>[{APP_NAME}]</b> Connected (ID: {CLIENT_ID})", APP_NAME)
                    except Exception as e:
                        if "already in use" in str(e).lower():
                            CLIENT_ID = bump_client_id(APP_NAME, "strategy")
                            log_status(f"⚠️ Bumped Client ID to {CLIENT_ID}")
                        else:
                            log_status(f"❌ Connection failed: {e}")
                            time.sleep(10)
                            continue

                # 2. SUBSCRIBE TO ACCOUNT UPDATES (NEW PATTERN)
                # We subscribe ONCE using the Zero-Argument method
                self.ib.accountSummaryEvent += self._on_account_summary
                self.ib.reqAccountSummary() 
                log_status("✅ Account Summary Stream Subscribed")

                # 3. WARM-UP LOOP (Crucial!)
                # Waits for cash balance data to arrive. No fallback.
                log_status("⏳ Waiting for Cash Balance data...")
                wait_count = 0
                while self.cached_cash_balance <= 0 and wait_count < 50:
                    self.ib.sleep(0.1)  # Keep connection alive while waiting
                    wait_count += 1
                
                log_status(f"💰 Cash Balance Loaded: {self.cached_cash_balance:,.2f}")
                
                # 4. SUBSCRIBE TO MARKET DATA
                if self._ticker: self.ib.cancelMktData(self.contract)
                self._ticker = self.ib.reqMktData(self.contract, "", False, False)
                self._ticker.updateEvent += self._on_tick
                
                log_status("✅ Data Subscribed. Monitoring...")
                self._write_heartbeat("running")

                # 5. MONITOR LOOP
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
                if self.ib.isConnected():
                    self.ib.disconnect()
                
                if not self._stop_requested:
                    log_status("🔄 Reconnecting in 10s...")
                    time.sleep(10)

        log_status("🛑 Bot Stopped.")
        send_alert(f"🛑 <b>[{APP_NAME}]</b> Bot Stopped.", APP_NAME)

    def stop(self) -> None:
        self._stop_requested = True

if __name__ == "__main__":
    runner = StrategyRunner()
    try:
        runner.run()
    except KeyboardInterrupt:
        runner.stop()