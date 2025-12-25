"""
IBKR-Confirmed Strategy Runner: Santa Claus Rally
Adapted from QuantConnect template to ib_insync production runner.

Strategy:
- Enter LONG SPY when there are exactly 5 trading days left in the year (Market Open).
- Exit (Liquidate) SPY on the 2nd trading day of the new year.
- Capital: Can use Fixed Amount OR Percentage of Live Cash Balance.
- Persistence: Uses 'state.json' to remember position across restarts.
"""

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

from ib_insync import (
    IB,
    Stock,
    Contract as IBContract,
    MarketOrder,
    Trade
)

# ─────────────────────────────────────────────────────────────
# PATH & CLIENT ID MANAGER WIRES
# ─────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.client_id_manager import get_or_allocate_client_id, bump_client_id  # type: ignore


# ─────────────────────────────────────────────────────────────
# CONFIG — SANTA CLAUS RALLY
# ─────────────────────────────────────────────────────────────
APP_NAME = "Santa_Claus_Rally"

HOST = "127.0.0.1"
PORT = 7497                 # 7497 paper, 7496 live, or your Gateway port
ACCOUNT_ID = "DU3188670"         # Optional: Specific Account ID

SYMBOL = "SPY"
SEC_TYPE = "STK"
EXCHANGE = "SMART"
CURRENCY = "USD"

# ─── CAPITAL SIZING SETTINGS ───
CAPITAL_MODE = "PCT"        # "FIXED" or "PCT"
FIXED_CAPITAL_AMOUNT = 100000.0
CAPITAL_PCT = 0.95          
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
STATE_FILE_PATH = os.path.join(LOG_ROOT, "state.json") # <--- NEW STATE FILE

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
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}][{APP_NAME}] {msg}"
    print(line)
    try:
        with open(STATUS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# STRATEGY RUNNER
# ─────────────────────────────────────────────────────────────
class StrategyRunner:
    def __init__(self) -> None:
        self.ib = IB()
        self.contract = self._build_contract()

        # Internal State
        self.current_position: str = "NONE"
        self.current_qty: int = 0
        self.entry_price: Optional[float] = None
        self.entry_time: Optional[dt.datetime] = None

        # Cash Balance Tracker
        self.account_cash_balance: float = 0.0

        # Throttling
        self.last_trade_time: Optional[dt.datetime] = None
        self.last_action: Optional[str] = None
        self.last_action_price: Optional[float] = None

        self.trade_log_buffer: List[TradeRow] = []
        self.lock = threading.Lock()

        self._ticker = None
        self._stop_requested = False
        self._logged_order_ids: Dict[int, bool] = {}
        self.prices: List[float] = []
        
        # Load State on Init
        self._load_state()

    # ─────────────────────────────
    # STATE MANAGEMENT (JSON) - NEW
    # ─────────────────────────────
    def _load_state(self):
        """Loads position state from JSON file on startup."""
        if not os.path.exists(STATE_FILE_PATH):
            return

        try:
            with open(STATE_FILE_PATH, "r") as f:
                data = json.load(f)
            
            # Restore variables
            self.current_position = data.get("current_position", "NONE")
            self.current_qty = data.get("current_qty", 0)
            self.entry_price = data.get("entry_price")
            
            # Restore entry time (handle string -> datetime)
            et_str = data.get("entry_time")
            if et_str:
                try:
                    self.entry_time = dt.datetime.fromisoformat(et_str)
                except:
                    self.entry_time = None
            
            log_status(f"STATE LOADED: Position={self.current_position}, Qty={self.current_qty}")

        except Exception as e:
            log_status(f"ERROR loading state file: {e}")

    def _save_state(self):
        """Saves current variables to JSON file."""
        data = {
            "current_position": self.current_position,
            "current_qty": self.current_qty,
            "entry_price": self.entry_price,
            "entry_time": self.entry_time.isoformat() if self.entry_time else None,
            "last_update": dt.datetime.now(dt.timezone.utc).isoformat()
        }
        try:
            with open(STATE_FILE_PATH, "w") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            log_status(f"ERROR saving state file: {e}")

    # ─────────────────────────────
    # CONTRACT BUILDING
    # ─────────────────────────────
    def _build_contract(self):
        return Stock(SYMBOL, EXCHANGE, CURRENCY)

    # ─────────────────────────────
    # FILE IO
    # ─────────────────────────────
    def _flush_trade_log_buffer(self) -> None:
        with self.lock:
            if not self.trade_log_buffer:
                return
            rows = []
            for r in self.trade_log_buffer:
                rows.append({
                    "timestamp": r.timestamp.isoformat(),
                    "symbol": r.symbol,
                    "action": r.action,
                    "price": r.price,
                    "quantity": r.quantity,
                    "pnl": r.pnl,
                    "duration": r.duration,
                    "position": r.position,
                    "status": r.status,
                    "ib_order_id": r.ib_order_id,
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
            df["timestamp"].astype(str)
            + "|" + df["symbol"].astype(str)
            + "|" + df["action"].astype(str)
            + "|" + df["ib_order_id"].astype(str)
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
            "account_cash_base": self.account_cash_balance
        }
        try:
            with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # ─────────────────────────────
    # SAFETY & SIZING
    # ─────────────────────────────
    def _now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    def _can_trade(self, action: str, price: float) -> bool:
        now = self._now()
        if self.last_trade_time is not None:
            if (now - self.last_trade_time).total_seconds() < COOLDOWN_SEC:
                return False
        
        # Simple long-only gating
        if action == "BUY" and self.current_position == "LONG":
            return False
        if action == "SELL" and self.current_position == "NONE":
            return False
        return True

    def _qty_for_price(self, price: float) -> int:
        if price <= 0: return 0
        capital_to_use = 0.0

        if CAPITAL_MODE == "FIXED":
            capital_to_use = FIXED_CAPITAL_AMOUNT
            log_status(f"Capital Check: Using FIXED amount: ${capital_to_use:,.2f}")

        elif CAPITAL_MODE == "PCT":
            if self.account_cash_balance <= 0:
                log_status(f"WARNING: No valid cash balance found yet ({self.account_cash_balance}). Cannot trade.")
                return 0
            capital_to_use = self.account_cash_balance * CAPITAL_PCT
            log_status(f"Capital Check: Using {CAPITAL_PCT*100}% of Cash (${self.account_cash_balance:,.2f}) = ${capital_to_use:,.2f}")

        max_qty = int(capital_to_use // price)
        if max_qty < MIN_QTY: return 0
        return max_qty

    # ─────────────────────────────
    # DATE UTILITIES
    # ─────────────────────────────
    def _get_trading_days_remaining_in_year(self, current_date: dt.datetime) -> int:
        end_of_year = dt.datetime(current_date.year, 12, 31)
        days = pd.bdate_range(start=current_date, end=end_of_year)
        return len(days)

    def _get_trading_days_elapsed_in_jan(self, current_date: dt.datetime) -> int:
        start_of_year = dt.datetime(current_date.year, 1, 1)
        days = pd.bdate_range(start=start_of_year, end=current_date)
        return len(days)

    # ─────────────────────────────
    # STRATEGY LOGIC
    # ─────────────────────────────
    def compute_signal(self, price: float) -> Optional[str]:
        now = dt.datetime.now()
        
        # 1. Check Entry (December)
        if now.month == 12:
            if now.day >= 15: 
                days_left = self._get_trading_days_remaining_in_year(now)
                # Enter when exactly 5 trading days are left
                if days_left == 5:
                    return "BUY"

        # 2. Check Exit (January)
        elif now.month == 1:
            if now.day <= 10:
                jan_days = self._get_trading_days_elapsed_in_jan(now)
                # Exit on 2nd trading day
                if jan_days >= 2:
                    return "SELL"
        
        return None

    # ─────────────────────────────
    # ORDER & EXECUTION
    # ─────────────────────────────
    def _place_order(self, action: str, qty: int, price: float) -> None:
        order = MarketOrder(action, int(qty))
        if ACCOUNT_ID:
            order.account = ACCOUNT_ID

        trade: Trade = self.ib.placeOrder(self.contract, order)
        oid = trade.order.orderId
        log_status(f"Placed {action} MKT x{qty} @ ~{price:.4f} (orderId={oid})")
        trade.updateEvent += lambda t=trade: self._on_trade_update(t)

        self.last_trade_time = self._now()
        self.last_action = action
        self.last_action_price = price

    def _on_trade_update(self, trade: Trade) -> None:
        status = getattr(trade.orderStatus, "status", None)
        avg_price = getattr(trade.orderStatus, "avgFillPrice", None)
        filled = getattr(trade.orderStatus, "filled", None)
        oid = getattr(trade.order, "orderId", None)

        if oid is None: return
        if self._logged_order_ids.get(oid, False): return
        if status is None: return
        if status.lower() not in ("filled", "partiallyfilled"): return
        if avg_price is None or avg_price <= 0 or filled is None or filled <= 0: return

        action = trade.order.action.upper()
        qty = int(filled)
        price = float(avg_price)
        now = self._now()

        pnl = 0.0
        duration = 0.0
        position_after = self.current_position

        # UPDATE STATE BASED ON FILL
        if action == "BUY":
            self.current_position = "LONG"
            self.current_qty = qty
            self.entry_price = price
            self.entry_time = now
            position_after = self.current_position
            self._save_state() # <--- SAVE STATE

        elif action == "SELL":
            if self.current_position == "LONG" and self.entry_price is not None:
                pnl = (price - float(self.entry_price)) * qty
            if self.entry_time is not None:
                duration = (now - self.entry_time).total_seconds()
            self.current_position = "NONE"
            self.current_qty = 0
            self.entry_price = None
            self.entry_time = None
            position_after = "NONE"
            self._save_state() # <--- SAVE STATE

        if action not in ("BUY", "SELL"): return

        row = TradeRow(
            timestamp=now,
            symbol=SYMBOL,
            action=action,
            price=price,
            quantity=qty,
            pnl=pnl,
            duration=duration,
            position=position_after,
            status=status,
            ib_order_id=oid,
            extra={
                "avgFillPrice": avg_price,
                "filled": filled,
                "account": getattr(trade.order, "account", None),
            },
        )
        with self.lock:
            self.trade_log_buffer.append(row)
        self._flush_trade_log_buffer()
        self._logged_order_ids[oid] = True
        log_status(f"Logged fill: {action} x{qty} @ {price:.4f}, status={status}")

    # ─────────────────────────────
    # MARKET DATA HANDLER
    # ─────────────────────────────
    def _on_tick(self, _=None) -> None:
        if self._stop_requested: return
        if self._ticker is None: return

        price = (self._ticker.last or self._ticker.marketPrice() or self._ticker.close or 0.0)
        if price <= 0: return
        price = float(price)

        self.prices.append(price)
        if len(self.prices) > 100: self.prices = self.prices[-100:]

        action = self.compute_signal(price)
        
        if action not in ("BUY", "SELL"):
            self._write_heartbeat(status="running", last_price=price)
            return

        if not self._can_trade(action, price):
            self._write_heartbeat(status="running", last_price=price)
            return

        qty = self._qty_for_price(price)
        if qty <= 0:
            self._write_heartbeat(status="running", last_price=price)
            return

        self._place_order(action, qty, price)
        self._write_heartbeat(status="order_submitted", last_price=price)

    # ─────────────────────────────
    # ACCOUNT SUMMARY HANDLER
    # ─────────────────────────────
    def _on_account_summary(self, val):
        if val.tag == "TotalCashBalance" and val.currency == "BASE":
            try:
                new_balance = float(val.value)
                if abs(new_balance - self.account_cash_balance) > 1.0:
                    log_status(f"Cash Balance Updated: ${new_balance:,.2f} (BASE)")
                self.account_cash_balance = new_balance
            except Exception:
                pass

    # ─────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────
    def run(self) -> None:
        global CLIENT_ID
        while True:
            try:
                log_status(f"Connecting to IBKR at {HOST}:{PORT} (clientId={CLIENT_ID})")
                self.ib.connect(HOST, PORT, clientId=CLIENT_ID, readonly=False)
                self.ib.qualifyContracts(self.contract)
                log_status(f"Connected. Qualified contract: {self.contract}")
                break
            except Exception as e:
                msg = str(e).lower()
                if "client id already in use" in msg:
                    new_id = bump_client_id(name=APP_NAME, role="strategy")
                    CLIENT_ID = new_id
                    log_status(f"Client ID in use. Bumped to {new_id}, retrying...")
                    time.sleep(2)
                    continue
                else:
                    log_status(f"Fatal connection error: {e}")
                    self._write_heartbeat(status="error_connect")
                    return

        # 1. SUBSCRIBE TO ACCOUNT DATA
        self.ib.accountSummaryEvent += self._on_account_summary
        self.ib.reqAccountSummary()
        log_status("Subscribed to Account Summary.")

        # 2. SUBSCRIBE TO MARKET DATA
        self._ticker = self.ib.reqMktData(self.contract, "", False, False)
        self._ticker.updateEvent += self._on_tick
        log_status("Subscribed to live market data.")
        
        self._write_heartbeat(status="running")

        try:
            while not self._stop_requested and self.ib.isConnected():
                self.ib.waitOnUpdate(timeout=0.5)
        except KeyboardInterrupt:
            log_status("KeyboardInterrupt. Stopping.")
        except Exception as e:
            log_status(f"Exception: {e}")
        finally:
            if self.ib.isConnected(): self.ib.disconnect()

    def stop(self) -> None:
        self._stop_requested = True


if __name__ == "__main__":
    StrategyRunner().run()