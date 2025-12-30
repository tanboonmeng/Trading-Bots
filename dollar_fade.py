"""
Santa Claus Rally / USD Fade Strategy Runner
Features:
- Auto-Reconnection (Survives TWS Restarts)
- Telegram Alerts (Entries, Exits, Disconnects)
- ISOLATED STATE: Uses a JSON file to track positions (Safe for shared accounts)
- Sustainable Logic (Throttled Ticks)
"""

import os
import sys
import json
import time
import math
import threading
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

import pandas as pd
import numpy as np
import datetime as dt
from zoneinfo import ZoneInfo

from ib_insync import (
    IB, Stock, Forex, Index, Future, Option, Contract as IBContract,
    MarketOrder, Trade,
)

# ─────────────────────────────────────────────────────────────
# PATH & CLIENT ID MANAGER WIRES
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
APP_NAME = "Dollar_Fade"

HOST = "127.0.0.1"
PORT = 7497                 
ACCOUNT_ID = "DU3188670"    

# Symbol Config
SYMBOL = "EURUSD"
SEC_TYPE = "FX"              
EXCHANGE = "IDEALPRO"       
CURRENCY = "USD"

# ─── STRATEGY DATES (Fixed Entry/Exit) ───
# Entry: December 23rd
ENTRY_MONTH = 12
ENTRY_DAY = 23

# Exit: January 2nd
EXIT_MONTH = 1
EXIT_DAY = 2

# ─── CAPITAL SIZING SETTINGS ───
CAPITAL_MODE = "FIXED"      # "FIXED" or "PERCENTAGE"
CAPITAL_FIXED_AMOUNT = 2000.0 
CAPITAL_PCT = 0.02

MIN_QTY = 1000              # FX minimum lot size

# Execution Safeguards
COOLDOWN_SEC = 300           
MIN_SAME_ACTION_REPRICE = 0.0005 

# Logs:
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_ROOT = os.path.join(BASE_DIR, "logs", APP_NAME)
os.makedirs(LOG_ROOT, exist_ok=True)

TRADE_LOG_PATH = os.path.join(LOG_ROOT, "trade_log.csv")
HEARTBEAT_PATH = os.path.join(LOG_ROOT, "heartbeat.json")
STATUS_LOG_PATH = os.path.join(LOG_ROOT, "status.log")
# [NEW] Isolated State File
STATE_FILE = os.path.join(LOG_ROOT, "state_Dollar_Fade.json")

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
    # Use Eastern Time for logging
    ts = dt.datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts} ET][{APP_NAME}] {msg}"
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
        self.contract = self._build_contract()

        # STATE MANAGEMENT
        self.current_position: str = "NONE"
        self.current_qty: int = 0
        self.entry_price: Optional[float] = None
        self.entry_time: Optional[dt.datetime] = None

        self.last_trade_time: Optional[dt.datetime] = None
        self.last_action: Optional[str] = None
        self.last_action_price: Optional[float] = None

        self.trade_log_buffer: List[TradeRow] = []
        self.lock = threading.Lock()

        self._ticker = None
        self._stop_requested = False
        self._logged_order_ids: Dict[int, bool] = {}
        self.prices: List[float] = []
        
        # SUSTAINABILITY: Tick Throttling
        self.last_tick_check: Optional[dt.datetime] = None
        self.tick_throttle_sec = 1.0 
        
        # Restore State on Init
        self._restore_state()

    def _build_contract(self):
        if SEC_TYPE == "FX":
            return Forex(SYMBOL, exchange=EXCHANGE)
        stype = SEC_TYPE.upper()
        if stype == "STK":
            return Stock(SYMBOL, EXCHANGE, CURRENCY)
        return IBContract(conId=0, symbol=SYMBOL, secType=SEC_TYPE, exchange=EXCHANGE, currency=CURRENCY)

    def _restore_state(self):
        state = load_state()
        pos = state.get("current_position", "NONE")
        
        if pos in ["LONG", "SHORT"]:
            self.current_position = pos
            self.current_qty = int(state.get("current_qty", 0))
            self.entry_price = float(state.get("entry_price", 0.0))
            
            t_str = state.get("entry_time")
            if t_str:
                try:
                    self.entry_time = dt.datetime.fromisoformat(t_str)
                except: pass
            
            log_status(f"♻️ RESTORED STATE: {self.current_position} {self.current_qty} {SYMBOL} @ {self.entry_price}")
        else:
            log_status("ℹ️ No active position in State File.")

    # ─────────────────────────────
    # FILE IO
    # ─────────────────────────────
    def _flush_trade_log_buffer(self) -> None:
        with self.lock:
            if not self.trade_log_buffer: return
            rows = []
            for r in self.trade_log_buffer:
                rows.append({
                    "timestamp": r.timestamp.isoformat(),
                    "symbol": r.symbol, "action": r.action, "price": r.price,
                    "quantity": r.quantity, "pnl": r.pnl, "duration": r.duration,
                    "position": r.position, "status": r.status,
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
            df["timestamp"].astype(str) + "|" + df["symbol"].astype(str) + 
            "|" + df["action"].astype(str) + "|" + df["ib_order_id"].astype(str)
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
            "last_update": dt.datetime.now(ZoneInfo("America/New_York")).isoformat(),
            "position": self.current_position,
            "position_qty": self.current_qty,
            "entry_price": self.entry_price,
            "last_price": last_price,
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
        return dt.datetime.now(ZoneInfo("America/New_York"))

    def _can_trade(self, action: str, price: float) -> bool:
        now = self._now()
        
        if "PENDING" in self.current_position:
            return False

        if self.last_trade_time is not None:
            if (now - self.last_trade_time).total_seconds() < COOLDOWN_SEC:
                return False

        if self.last_action == action and self.last_action_price:
            last_px = self.last_action_price
            if last_px > 0:
                if abs(price - last_px) / last_px < MIN_SAME_ACTION_REPRICE:
                    return False

        if action == "BUY" and self.current_position != "NONE":
            return False
        if action == "SELL" and self.current_position != "LONG":
            return False
            
        return True

    def _get_capital_allocation(self) -> float:
        if CAPITAL_MODE == "FIXED":
            return CAPITAL_FIXED_AMOUNT

        elif CAPITAL_MODE == "PERCENTAGE":
            try:
                summary = self.ib.accountSummary(ACCOUNT_ID if ACCOUNT_ID else "All")
                cash_tag = next(
                    (v for v in summary if v.tag == "TotalCashBalance" and v.currency == "BASE"), 
                    None
                )
                if cash_tag:
                    total_cash = float(cash_tag.value)
                    alloc = total_cash * CAPITAL_PCT
                    return alloc
                else:
                    return 0.0
            except Exception as e:
                log_status(f"Error fetching account summary: {e}")
                return 0.0
        return 0.0

    def _qty_for_price(self, price: float) -> int:
        if price <= 0: return 0
        capital_to_use = self._get_capital_allocation()
        if capital_to_use <= 0: return 0
        units = int(capital_to_use / price)
        if units < MIN_QTY: return 0
        return units

    # ─────────────────────────────
    # STRATEGY LOGIC: FIXED DATES
    # ─────────────────────────────
    def compute_signal(self, price: float) -> Optional[str]:
        now_et = self._now()
        today_month = now_et.month
        today_day = now_et.day
        
        # 1. EXIT LOGIC - January 2nd OR LATER
        if self.current_position == "LONG":
            # Primary Exit: Jan 2nd+
            if today_month == EXIT_MONTH and today_day >= EXIT_DAY:
                log_status(f"Exit signal triggered on {now_et.strftime('%Y-%m-%d')} (Jan {EXIT_DAY}+)")
                return "SELL"
            
            # Safety Exit: Late months (Feb, Mar, etc.)
            # CRITICAL FIX: Explicitly ignore December (Month 12) so we don't sell early
            if today_month > EXIT_MONTH and today_month != 12:
                log_status(f"Late exit signal triggered on {now_et.strftime('%Y-%m-%d')}")
                return "SELL"
        
        # 2. ENTRY LOGIC - December 23rd ONLY
        elif self.current_position == "NONE":
            if today_month == ENTRY_MONTH and today_day == ENTRY_DAY:
                log_status(f"Entry signal triggered on {now_et.strftime('%Y-%m-%d')} (Dec {ENTRY_DAY})")
                return "BUY"

        return None

    # ─────────────────────────────
    # ORDER & EXECUTION HANDLING
    # ─────────────────────────────
    def _place_order(self, action: str, qty: int, price: float) -> None:
        order = MarketOrder(action, int(qty))
        if ACCOUNT_ID: order.account = ACCOUNT_ID
        order.orderRef = APP_NAME 

        if action == "BUY": self.current_position = "PENDING_BUY"
        elif action == "SELL": self.current_position = "PENDING_SELL"

        trade: Trade = self.ib.placeOrder(self.contract, order)
        
        # Matches original logic: statusEvent for updates
        trade.statusEvent += self._on_trade_update
        
        self.last_trade_time = self._now()
        self.last_action = action
        self.last_action_price = price
        
        msg = f"🚀 <b>[{APP_NAME}]</b> {action} MKT x{qty} @ ~{price:.4f}"
        log_status(msg)
        send_alert(msg, APP_NAME)

    def _on_trade_update(self, trade: Trade) -> None:
        status = getattr(trade.orderStatus, "status", None)
        avg_price = getattr(trade.orderStatus, "avgFillPrice", None)
        filled = getattr(trade.orderStatus, "filled", None)
        oid = getattr(trade.order, "orderId", None)

        if oid is None or self._logged_order_ids.get(oid, False): return
        if status is None: return
        
        status_lower = status.lower()
        if status_lower not in ("filled", "partiallyfilled"): return
        if filled is None or filled <= 0: return

        action = trade.order.action.upper()
        qty = int(filled)
        price = float(avg_price) if avg_price else 0.0
        now = self._now()

        pnl = 0.0
        duration = 0.0
        position_after = "UNKNOWN"

        if action == "BUY":
            self.current_position = "LONG"
            self.current_qty = qty
            self.entry_price = price
            self.entry_time = now
            position_after = "LONG"
            # [NEW] SAVE STATE
            save_state("LONG", qty, price, now)

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
            # [NEW] CLEAR STATE
            save_state("NONE", 0, 0.0, None)

        row = TradeRow(
            timestamp=now.replace(tzinfo=None), symbol=SYMBOL, action=action, price=price, quantity=qty,
            pnl=pnl, duration=duration, position=position_after, status=status,
            ib_order_id=oid, extra={"avgFillPrice": avg_price, "filled": filled}
        )

        with self.lock:
            self.trade_log_buffer.append(row)
        
        self._flush_trade_log_buffer()
        self._logged_order_ids[oid] = True
        
        msg = f"✅ <b>[{APP_NAME}]</b> FILL: {action} x{qty} @ {price:.4f} (PnL={pnl:.2f})"
        log_status(msg)
        send_alert(msg, APP_NAME)

    # ─────────────────────────────
    # MARKET DATA TICK HANDLER
    # ─────────────────────────────
    def _on_tick(self, _=None) -> None:
        if self._stop_requested or self._ticker is None: return
        
        # SUSTAINABILITY: Tick Throttling
        now = self._now()
        if self.last_tick_check and (now - self.last_tick_check).total_seconds() < self.tick_throttle_sec:
            return 
        self.last_tick_check = now

        # Handle NaN in Forex
        price = 0.0
        if self._ticker.last and not math.isnan(self._ticker.last):
            price = self._ticker.last
        elif self._ticker.bid and self._ticker.ask:
            price = (self._ticker.bid + self._ticker.ask) / 2
        else:
            price = self._ticker.close

        if price <= 0 or math.isnan(price): return
        price = float(price)

        self.prices.append(price)
        if len(self.prices) > 500: self.prices = self.prices[-500:]

        action = self.compute_signal(price)
        
        if action not in ("BUY", "SELL"):
            self._write_heartbeat(status="running", last_price=price)
            return

        if not self._can_trade(action, price):
            self._write_heartbeat(status="running_no_trade", last_price=price)
            return

        qty = self._qty_for_price(price)
        if qty <= 0:
            log_status(f"Signal {action} ignored: Qty calculated is 0")
            return

        self._place_order(action, qty, price)
        self._write_heartbeat(status="order_submitted", last_price=price)

    # ─────────────────────────────
    # MAIN LOOP (AUTO-RECONNECT)
    # ─────────────────────────────
    def run(self) -> None:
        global CLIENT_ID
        
        send_alert(f"🚀 <b>[{APP_NAME}]</b> Started.\nTarget: {ENTRY_MONTH}/{ENTRY_DAY} -> {EXIT_MONTH}/{EXIT_DAY}", APP_NAME)
        
        while not self._stop_requested:
            try:
                # 1. CONNECT
                if not self.ib.isConnected():
                    log_status(f"Connecting to {HOST}:{PORT} (ID: {CLIENT_ID})...")
                    try:
                        self.ib.connect(HOST, PORT, clientId=CLIENT_ID, readonly=False)
                        self.ib.qualifyContracts(self.contract)
                        log_status(f"✅ Connected. Contract: {self.contract}")
                        
                        # [NEW] Position Status Check after Reconnection
                        log_status(f"📊 Current Position: {self.current_position} | Qty: {self.current_qty} | Entry: {self.entry_price}")
                        
                        send_alert(f"✅ <b>[{APP_NAME}]</b> Connected (ID: {CLIENT_ID})", APP_NAME)
                    except Exception as e:
                        if "already in use" in str(e).lower():
                            CLIENT_ID = bump_client_id(APP_NAME, "strategy")
                            log_status(f"⚠️ Bumped Client ID to {CLIENT_ID}")
                        else:
                            log_status(f"❌ Connection failed: {e}")
                        time.sleep(10)
                        continue

                # 2. SETUP & SUBSCRIBE
                self._ticker = self.ib.reqMktData(self.contract, "", False, False)
                self._ticker.updateEvent += self._on_tick
                log_status("✅ Data Subscribed. Monitoring...")
                self._write_heartbeat("running")

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
                if self.ib.isConnected():
                    self.ib.disconnect()
                
                if not self._stop_requested:
                    log_status("🔄 Reconnecting in 10s...")
                    time.sleep(10)

        log_status("🛑 Bot Stopped.")
        send_alert(f"🛑 <b>[{APP_NAME}]</b> Bot Stopped.", APP_NAME)

    def stop(self) -> None:
        self._stop_requested = True

def main() -> None:
    runner = StrategyRunner()
    try:
        runner.run()
    except KeyboardInterrupt:
        runner.stop()

if __name__ == "__main__":
    main()