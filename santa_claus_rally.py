"""
IBKR-Confirmed Strategy Runner: Santa Claus Rally (Template-Compliant)
Surgical fixes: Remove state persistence, fix race conditions, align with template.

Strategy:
- Enter LONG SPY when there are exactly 5 trading days left in the year (Market Open).
- Exit (Liquidate) SPY on the 2nd trading day of the new year.
- Capital: Can use Fixed Amount OR Percentage of Live Cash Balance.
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
# CONFIG – SANTA CLAUS RALLY
# ─────────────────────────────────────────────────────────────
APP_NAME = "Santa_Claus_Rally"

HOST = "127.0.0.1"
PORT = 7497                 # 7497 paper, 7496 live, or your Gateway port
ACCOUNT_ID = "DU3188670"    # Optional: Specific Account ID

SYMBOL = "SPY"
SEC_TYPE = "STK"
EXCHANGE = "SMART"
CURRENCY = "USD"

# ─── CAPITAL SIZING SETTINGS ───
CAPITAL_MODE = "PCT"        # "FIXED" or "PCT"
FIXED_CAPITAL_AMOUNT = 100000.0
CAPITAL_PCT = 0.02          # 95% of cash balance (NOT 0.02!)
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

        # Internal State (ephemeral - rebuilt each session from fills)
        self.current_position: str = "NONE"
        self.current_qty: int = 0
        self.entry_price: Optional[float] = None
        self.entry_time: Optional[dt.datetime] = None

        # Cash Balance Tracker
        self.account_cash_balance: float = 0.0

        # *** FIX 1: Pending order tracking (with lock for thread safety) ***
        self.pending_order: bool = False
        self.pending_action: Optional[str] = None

        # Throttling
        self.last_trade_time: Optional[dt.datetime] = None
        self.last_action: Optional[str] = None
        self.last_action_price: Optional[float] = None

        # *** FIX 2: Track signal dates by type (BUY/SELL separately) ***
        self.last_buy_date: Optional[str] = None
        self.last_sell_date: Optional[str] = None

        # *** FIX 3: Tick debouncing ***
        self.last_tick_check: Optional[dt.datetime] = None
        self.tick_throttle_sec = 1.0  # Process ticks max once per second

        self.trade_log_buffer: List[TradeRow] = []
        self.lock = threading.Lock()

        self._ticker = None
        self._stop_requested = False
        self._logged_order_ids: Dict[int, bool] = {}
        self.prices: List[float] = []

    # ─────────────────────────────────────────────────────────
    # CONTRACT BUILDING
    # ─────────────────────────────────────────────────────────
    def _build_contract(self):
        return Stock(SYMBOL, EXCHANGE, CURRENCY)

    # ─────────────────────────────────────────────────────────
    # FILE IO (DO NOT MODIFY - Dashboard Integration)
    # ─────────────────────────────────────────────────────────
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
            "account_cash_base": self.account_cash_balance,
            "pending_order": self.pending_order
        }
        try:
            with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────
    # SAFETY & SIZING (DO NOT MODIFY - Template Compliance)
    # ─────────────────────────────────────────────────────────
    def _now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    def _can_trade(self, action: str, price: float) -> bool:
        """Enhanced trade gating with multiple safeguards."""
        now = self._now()
        
        # *** Check pending orders first (no lock needed for read) ***
        if self.pending_order:
            log_status(f"BLOCKED: Pending {self.pending_action} order exists")
            return False
        
        # *** Cooldown check ***
        if self.last_trade_time is not None:
            elapsed = (now - self.last_trade_time).total_seconds()
            if elapsed < COOLDOWN_SEC:
                log_status(f"BLOCKED: Cooldown active ({elapsed:.1f}s < {COOLDOWN_SEC}s)")
                return False
        
        # *** Position-based gating ***
        if action == "BUY":
            if self.current_position == "LONG":
                log_status(f"BLOCKED: Already LONG with {self.current_qty} shares")
                return False
        elif action == "SELL":
            if self.current_position == "NONE":
                log_status(f"BLOCKED: No position to SELL")
                return False
        
        return True

    def _qty_for_price(self, price: float) -> int:
        if price <= 0: 
            return 0
        
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
    # STRATEGY LOGIC (FIX 2: Signal-type tracking)
    # ─────────────────────────────────────────────────────────
    def compute_signal(self, price: float) -> Optional[str]:
        """Returns BUY/SELL signal with duplicate prevention per signal type."""
        now = dt.datetime.now()
        today_key = now.strftime("%Y-%m-%d")
        
        # 1. Check Entry (December)
        if now.month == 12:
            if now.day >= 15: 
                days_left = self._get_trading_days_remaining_in_year(now)
                
                # *** CHANGE THIS NUMBER TO ADJUST ENTRY TIMING ***
                ENTRY_DAYS_LEFT = 5  # Enter when exactly this many trading days remain
                
                if days_left == ENTRY_DAYS_LEFT and self.current_position == "NONE":
                    # Prevent duplicate BUY on same day
                    if self.last_buy_date == today_key:
                        return None
                    log_status(f"SIGNAL: BUY triggered ({ENTRY_DAYS_LEFT} trading days left in year)")
                    return "BUY"

        # 2. Check Exit (January)
        elif now.month == 1:
            if now.day <= 10:
                jan_days = self._get_trading_days_elapsed_in_jan(now)
                if jan_days >= 2 and self.current_position == "LONG":
                    # Prevent duplicate SELL on same day
                    if self.last_sell_date == today_key:
                        return None
                    log_status(f"SIGNAL: SELL triggered (2nd trading day of January)")
                    return "SELL"
        
        return None

    # ─────────────────────────────────────────────────────────
    # ORDER & EXECUTION (FIX 3: Race condition fix)
    # ─────────────────────────────────────────────────────────
    def _place_order(self, action: str, qty: int, price: float) -> None:
        """Place order with atomic pending flag (prevents race conditions)."""
        
        # *** FIX 3: Set pending flag under lock BEFORE API call ***
        with self.lock:
            # Double-check under lock (defense in depth)
            if self.pending_order:
                log_status(f"BLOCKED: Order already pending (race condition caught)")
                return
            
            self.pending_order = True
            self.pending_action = action
            
            # *** FIX 2: Mark signal date by action type ***
            today = dt.datetime.now().strftime("%Y-%m-%d")
            if action == "BUY":
                self.last_buy_date = today
            elif action == "SELL":
                self.last_sell_date = today
        
        # Now safe to proceed with order placement
        order = MarketOrder(action, int(qty))
        if ACCOUNT_ID:
            order.account = ACCOUNT_ID
        
        # *** ADD ORDER REFERENCE (visible in TWS) ***
        order.orderRef = APP_NAME  # "Santa_Claus_Rally"

        trade: Trade = self.ib.placeOrder(self.contract, order)
        oid = trade.order.orderId
        log_status(f"Placed {action} MKT x{qty} @ ~{price:.4f} (orderId={oid}) [PENDING]")
        
        # Register callback for order updates (version-safe)
        trade.fillEvent += lambda t=trade: self._on_trade_update(t)
        trade.statusEvent += lambda t=trade: self._on_trade_update(t)

        self.last_trade_time = self._now()
        self.last_action = action
        self.last_action_price = price

    def _on_trade_update(self, trade: Trade) -> None:
        """Handle order fill and update state (DO NOT MODIFY - Dashboard P/L calculation)."""
        status = getattr(trade.orderStatus, "status", None)
        avg_price = getattr(trade.orderStatus, "avgFillPrice", None)
        filled = getattr(trade.orderStatus, "filled", None)
        oid = getattr(trade.order, "orderId", None)

        if oid is None: 
            return
        if self._logged_order_ids.get(oid, False): 
            return
        if status is None: 
            return
        
        # Log any status change for debugging
        log_status(f"Order {oid} status: {status}, filled: {filled}")
        
        if status.lower() not in ("filled", "partiallyfilled"): 
            return
        if avg_price is None or avg_price <= 0 or filled is None or filled <= 0: 
            return

        action = trade.order.action.upper()
        qty = int(filled)
        price = float(avg_price)
        now = self._now()

        pnl = 0.0
        duration = 0.0
        position_after = self.current_position

        # *** UPDATE STATE (Dashboard relies on these values) ***
        if action == "BUY":
            self.current_position = "LONG"
            self.current_qty = qty
            self.entry_price = price
            self.entry_time = now
            position_after = "LONG"
            
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
        
        # *** Clear pending flag after fill ***
        with self.lock:
            self.pending_order = False
            self.pending_action = None

        if action not in ("BUY", "SELL"): 
            return

        # *** LOG TRADE (Dashboard CSV schema - DO NOT MODIFY) ***
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
        log_status(f"FILLED: {action} x{qty} @ {price:.4f}, PnL=${pnl:.2f}, Position={position_after}")

    # ─────────────────────────────────────────────────────────
    # MARKET DATA HANDLER (FIX 4: Debounced tick processing)
    # ─────────────────────────────────────────────────────────
    def _on_tick(self, _=None) -> None:
        """Handle price ticks with debouncing and comprehensive duplicate prevention."""
        if self._stop_requested: 
            return
        if self._ticker is None: 
            return

        # *** FIX 4: Debounce tick processing (max once per second) ***
        now = self._now()
        if self.last_tick_check is not None:
            elapsed = (now - self.last_tick_check).total_seconds()
            if elapsed < self.tick_throttle_sec:
                return  # Skip this tick (too soon)
        
        self.last_tick_check = now

        price = (self._ticker.last or self._ticker.marketPrice() or self._ticker.close or 0.0)
        if price <= 0: 
            return
        price = float(price)

        self.prices.append(price)
        if len(self.prices) > 100: 
            self.prices = self.prices[-100:]

        # *** Check signal with all safeguards ***
        action = self.compute_signal(price)
        
        if action not in ("BUY", "SELL"):
            self._write_heartbeat(status="running", last_price=price)
            return

        # Triple-check can trade (includes pending check)
        if not self._can_trade(action, price):
            self._write_heartbeat(status="waiting", last_price=price)
            return

        qty = self._qty_for_price(price)
        if qty <= 0:
            log_status(f"BLOCKED: Insufficient capital for {action}")
            self._write_heartbeat(status="insufficient_capital", last_price=price)
            return

        # All checks passed - place order
        self._place_order(action, qty, price)
        self._write_heartbeat(status="order_submitted", last_price=price)

    # ─────────────────────────────────────────────────────────
    # ACCOUNT SUMMARY HANDLER
    # ─────────────────────────────────────────────────────────
    def _on_account_summary(self, val):
        if val.tag == "TotalCashBalance" and val.currency == "BASE":
            try:
                new_balance = float(val.value)
                if abs(new_balance - self.account_cash_balance) > 1.0:
                    log_status(f"Cash Balance Updated: ${new_balance:,.2f} (BASE)")
                self.account_cash_balance = new_balance
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────────────────────────────────
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

        # Subscribe to account data
        self.ib.accountSummaryEvent += self._on_account_summary
        self.ib.reqAccountSummary()
        log_status("Subscribed to Account Summary.")

        # Subscribe to market data
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
            if self.ib.isConnected(): 
                self.ib.disconnect()

    def stop(self) -> None:
        self._stop_requested = True


if __name__ == "__main__":
    StrategyRunner().run()