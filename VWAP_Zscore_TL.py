"""
VWAP Z-score Trend Long Strategy (Regime-Aware) - PERSISTENT & AUTO-RECONNECT
Features:
- Scanner-Style Heartbeat: Reports live Z-scores for all stocks.
- Efficient Weather Check: Only checks regime when a signal is detected.
- Unified Persistence: Saves positions and history in one state file.
"""

import os
import sys
import json
import time
import threading
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from collections import deque

import pandas as pd
import numpy as np
import datetime as dt

from ib_insync import IB, Stock, MarketOrder, Trade

# ────────────────────────────────────────────────────────────────
# PATH & CLIENT ID MANAGER
# ────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.client_id_manager import get_or_allocate_client_id, bump_client_id
from utils.telegram_alert import send_alert

# ────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────
APP_NAME = "VWAP_Zscore"
HOST = "127.0.0.1"
PORT = 7497
ACCOUNT_ID = "DU3188670"

STOCKS_TO_TRADE = [
    {'symbol': 'MSFT', 'n_vwap': 90, 'params': {'z_enter': 1.60, 'z_exit': 0.10}},
    {'symbol': 'NVDA', 'n_vwap': 130, 'params': {'z_enter': 0.60, 'z_exit': 0.50}},
    {'symbol': 'TSM', 'n_vwap': 60, 'params': {'z_enter': 0.40, 'z_exit': 0.10}},
    {'symbol': 'GOOGL', 'n_vwap': 90, 'params': {'z_enter': 0.40, 'z_exit': 0.10}},
    {'symbol': 'AAPL', 'n_vwap': 70, 'params': {'z_enter': 0.90, 'z_exit': 0.90}},
    {'symbol': 'META', 'n_vwap': 110, 'params': {'z_enter': 0.30, 'z_exit': 0.30}},
    {'symbol': 'TSLA', 'n_vwap': 60, 'params': {'z_enter': 1.00, 'z_exit': 1.00}},
    {'symbol': 'PLTR', 'n_vwap': 170, 'params': {'z_enter': 1.70, 'z_exit': 0.20}},
]

# --- CAPITAL ALLOCATION SETTINGS ---
CAPITAL_MODE = "PERCENTAGE" 
FIXED_CAPITAL_AMOUNT = 10000.0 
CASH_ALLOCATION_PCT = 0.02

MIN_QTY = 1
DAILY_SINGLE_ENTRY = True
REENTRY_COOLDOWN_MIN = 60

# [CRITICAL] Keep this as "BOT". We handle persistence manually now.
POSITION_SCOPE = "BOT"

UPDATE_INTERVAL_MIN = 5
ALIGN_TO_CLOCK = True

WEATHER_FILE = "market_weather.json"
REGIME_CHECK_ENABLED = True
ALLOW_EXITS_IN_BAD_REGIME = True
REQUIRED_STRATEGIES = ["trend", "breakout"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_ROOT = os.path.join(BASE_DIR, "logs", APP_NAME)
os.makedirs(LOG_ROOT, exist_ok=True)

TRADE_LOG_PATH = os.path.join(LOG_ROOT, "trade_log.csv")
HEARTBEAT_PATH = os.path.join(LOG_ROOT, "heartbeat.json")
STATUS_LOG_PATH = os.path.join(LOG_ROOT, "status.log")
STATE_FILE = os.path.join(LOG_ROOT, f"state_{APP_NAME}.json")

CLIENT_ID = get_or_allocate_client_id(name=APP_NAME, role="strategy", preferred=None)

# ────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ────────────────────────────────────────────────────────────────
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
class SymbolState:
    contract: Any
    ticker: Any = None
    bars: deque = field(default_factory=lambda: deque(maxlen=400))
    position: str = "NONE"
    quantity: int = 0
    entry_price: Optional[float] = None
    entry_time: Optional[dt.datetime] = None
    next_eval_at: Optional[dt.datetime] = None
    pending_order: bool = False

# ────────────────────────────────────────────────────────────────
# UTILITIES
# ────────────────────────────────────────────────────────────────
def log_status(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}][{APP_NAME}] {msg}"
    print(line)
    try:
        with open(STATUS_LOG_PATH, "a") as f:
            f.write(line + "\n")
    except: pass

def load_state() -> Dict:
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except: return {}

def save_state(state: Dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except: pass

def load_weather_report() -> Optional[Dict]:
    try:
        with open(WEATHER_FILE, "r") as f:
            return json.load(f)
    except:
        return None

def check_regime_permission(action: str) -> tuple:
    if not REGIME_CHECK_ENABLED:
        return True, {"status": "disabled"}
    
    weather = load_weather_report()
    if not weather:
        return False, {"status": "no_data"}
    
    try:
        regime = weather["market_condition"]["regime_code"]
        regime_desc = weather["market_condition"]["regime_desc"]
        commands = weather["strategy_commands"]
        
        if action == "SELL" and ALLOW_EXITS_IN_BAD_REGIME:
            return True, {"status": "exit_override", "regime": regime, "regime_desc": regime_desc}
        
        if action == "BUY":
            perms = [(s, commands.get(s, "STOP")) for s in REQUIRED_STRATEGIES]
            all_go = all(p == "GO" for _, p in perms)
            return all_go, {"status": "go" if all_go else "blocked", "regime": regime, 
                             "regime_desc": regime_desc, "permissions": dict(perms)}
        
        return False, {"status": "unknown"}
    except:
        return False, {"status": "error"}

# ────────────────────────────────────────────────────────────────
# MAIN RUNNER
# ────────────────────────────────────────────────────────────────
class VWAPZscoreRunner:
    def __init__(self):
        self.ib = IB()
        self.symbols = {}
        self.config = {c["symbol"]: c for c in STOCKS_TO_TRADE}
        self.state = load_state()
        self.trade_log_buffer = []
        self.lock = threading.Lock()
        self._logged_orders = set()
        self._stop = False
        
        # Capital Management Variables
        self.net_liq = 0.0
        self.total_cash = 0.0
        
        self.ib.accountSummaryEvent += self._on_account_summary
        
        # Initialize Symbols and Restore State
        for cfg in STOCKS_TO_TRADE:
            sym = cfg["symbol"]
            self.symbols[sym] = SymbolState(contract=Stock(sym, "SMART", "USD"))
            
            # Restore Persistence Logic
            pos_key = f"{sym}:POS"
            if pos_key in self.state:
                saved = self.state[pos_key]
                qty = saved.get("quantity", 0)
                if qty > 0:
                    self.symbols[sym].quantity = qty
                    self.symbols[sym].position = saved.get("position", "LONG")
                    self.symbols[sym].entry_price = saved.get("entry_price")
                    t_str = saved.get("entry_time")
                    if t_str:
                        try:
                            self.symbols[sym].entry_time = dt.datetime.fromisoformat(t_str)
                        except: pass
                    
                    log_status(f"♻️ Restored {sym}: LONG {qty} shares @ {self.symbols[sym].entry_price}")

    def _now(self):
        return dt.datetime.now()
    
    def _on_account_summary(self, val):
        if val.tag == "TotalCashBalance" and val.currency == "BASE":
            try:
                self.total_cash = float(val.value)
            except: pass
        if val.tag == "NetLiquidation" and val.currency == "USD":
            try:
                self.net_liq = float(val.value)
            except: pass

    def _compute_qty(self, price: float):
        if price <= 0: return MIN_QTY
        
        capital_to_use = 0.0
        if CAPITAL_MODE == "FIXED":
            capital_to_use = FIXED_CAPITAL_AMOUNT
        else:
            if self.total_cash <= 0:
                log_status("⚠️ Warning: Cash balance is 0 or not yet retrieved. Defaulting to MIN_QTY.")
                return MIN_QTY
            capital_to_use = self.total_cash * CASH_ALLOCATION_PCT

        qty = int(capital_to_use / price)
        return max(MIN_QTY, qty)

    def _schedule_next_eval(self, symbol: str, first: bool = False):
        now = self._now()
        interval = dt.timedelta(minutes=UPDATE_INTERVAL_MIN)
        
        if ALIGN_TO_CLOCK:
            mins = (now.minute // UPDATE_INTERVAL_MIN) * UPDATE_INTERVAL_MIN
            next_t = now.replace(second=0, microsecond=0, minute=mins) + interval
        else:
            next_t = now + interval
        
        if first and next_t <= now:
            next_t = now + interval
        
        self.symbols[symbol].next_eval_at = next_t

    def _should_eval_now(self, symbol: str):
        state = self.symbols[symbol]
        now = self._now()
        
        if not state.next_eval_at:
            self._schedule_next_eval(symbol, True)
            return False
        
        if now >= state.next_eval_at:
            self._schedule_next_eval(symbol, False)
            return True
        return False

    def _record_action_dedupe(self, symbol: str, side: str):
        """Records only the timestamp for daily dedupe logic."""
        key = f"{symbol}:{side}"
        self.state[key] = {"ts": self._now().isoformat()}
        save_state(self.state)

    def _save_symbol_state(self, symbol: str):
        """Saves the actual position details (Qty, Price) to JSON."""
        state = self.symbols[symbol]
        key = f"{symbol}:POS"
        
        data = {
            "quantity": state.quantity,
            "position": state.position,
            "entry_price": state.entry_price,
            "entry_time": state.entry_time.isoformat() if state.entry_time else None
        }
        self.state[key] = data
        save_state(self.state)

    def _blocked_by_dedupe(self, symbol: str, side: str):
        key = f"{symbol}:{side}"
        rec = self.state.get(key)
        if not rec: return False
        
        try:
            t = dt.datetime.fromisoformat(rec["ts"]).replace(tzinfo=None)
        except:
            return False
        
        now = self._now()
        
        if REENTRY_COOLDOWN_MIN > 0:
            if (now - t) < dt.timedelta(minutes=REENTRY_COOLDOWN_MIN):
                return True
        
        if DAILY_SINGLE_ENTRY and now.date() == t.date():
            return True
        
        return False

    def _flush_trade_log(self):
        with self.lock:
            if not self.trade_log_buffer: return
            
            rows = [{
                "timestamp": r.timestamp.isoformat(),
                "symbol": r.symbol, "action": r.action, "price": r.price,
                "quantity": r.quantity, "pnl": r.pnl, "duration": r.duration,
                "position": r.position, "status": r.status,
                "ib_order_id": r.ib_order_id,
                "extra": json.dumps(r.extra) if r.extra else None
            } for r in self.trade_log_buffer]
            
            df_new = pd.DataFrame(rows)
            self.trade_log_buffer.clear()
        
        if os.path.exists(TRADE_LOG_PATH):
            try:
                df_old = pd.read_csv(TRADE_LOG_PATH)
                df = pd.concat([df_old, df_new], ignore_index=True)
            except: df = df_new
        else:
            df = df_new
        
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        # Dedup by ID + Action
        df["key"] = df["ib_order_id"].astype(str) + "|" + df["action"]
        df = df.drop_duplicates("key").drop(columns=["key"]).sort_values("timestamp")
        df.to_csv(TRADE_LOG_PATH, index=False)

    def _write_heartbeat(self, status="running"):
        # --- NEW: Scanner-Style Heartbeat ---
        # Instead of only showing active positions, we show data for ALL tracked stocks.
        tickers_status = {}
        
        for sym, state in self.symbols.items():
            # Calculate the live Z-score for the dashboard (Scanner Mode)
            result = self._compute_vwap_zscore(sym)
            current_z = result[1] if result else None
            current_vwap = result[2] if result else None
            
            tickers_status[sym] = {
                "position": state.position,
                "qty": state.quantity,
                "z_score": round(current_z, 3) if current_z is not None else None,
                "vwap": round(current_vwap, 2) if current_vwap is not None else None
            }

        data = {
            "app_name": APP_NAME,
            "status": status,
            "last_update": self._now().isoformat(),
            "tickers": tickers_status,  # Shows ALL stocks + Z-scores
            "net_liq": self.net_liq,
            "total_cash_base": self.total_cash,
            "capital_mode": CAPITAL_MODE
        }
        
        try:
            with open(HEARTBEAT_PATH, "w") as f:
                json.dump(data, f, indent=2)
        except: pass

    def _compute_vwap_zscore(self, symbol: str):
        state = self.symbols[symbol]
        n = self.config[symbol]["n_vwap"]
        data = list(state.bars)
        
        if len(data) < n + 5: return None
        
        close = np.array([x[1] for x in data], dtype=float)
        vol = np.array([x[2] for x in data], dtype=float)
        
        pv = close * vol
        vwap = pd.Series(pv).rolling(n).sum() / pd.Series(vol).rolling(n).sum()
        
        diff = close - vwap.to_numpy()
        mu = pd.Series(diff).rolling(n).mean().to_numpy()
        sd = pd.Series(diff).rolling(n).std(ddof=0).to_numpy()
        
        if np.isnan(sd[-1]) or sd[-1] == 0: return None
        
        z = (diff - mu) / sd
        return (float(close[-1]), float(z[-1]), float(vwap.iloc[-1]))

    def compute_signal(self, symbol: str, price: float, z: float):
        state = self.symbols[symbol]
        params = self.config[symbol]["params"]
        
        # 1. Check INTERNAL Persistent Position
        bot_pos = state.quantity
        
        # Handle Exit
        if bot_pos > 0 and z <= params["z_exit"] and not state.pending_order:
            return "SELL"
            
        # Handle Entry
        if z >= params["z_enter"] and not state.pending_order:
            if bot_pos == 0:
                if self._blocked_by_dedupe(symbol, "BUY"):
                    return None
                return "BUY"
        
        return None

    def _place_order(self, symbol: str, action: str, price: float):
        state = self.symbols[symbol]
        qty = self._compute_qty(price)
        
        if qty < MIN_QTY: return
        
        order = MarketOrder(action, qty)
        if ACCOUNT_ID: order.account = ACCOUNT_ID
        
        trade = self.ib.placeOrder(state.contract, order)
        oid = trade.order.orderId
        
        msg = f"[{symbol}] 🚀 {action} x{qty} @ ~{price:.2f} (oid={oid}) | Mode: {CAPITAL_MODE}"
        log_status(msg)
        send_alert(msg, APP_NAME) # [ALERT] Order Placed
        
        state.pending_order = True
        trade.updateEvent += lambda t=trade, s=symbol: self._on_trade_update(t, s)

    def _on_trade_update(self, trade, symbol: str):
        status = getattr(trade.orderStatus, "status", None)
        avg_price = getattr(trade.orderStatus, "avgFillPrice", None)
        filled = getattr(trade.orderStatus, "filled", None)
        oid = getattr(trade.order, "orderId", None)
        
        if not oid or oid in self._logged_orders: return
        if not status: return
        
        s = status.lower()
        if s in ("filled", "cancelled", "inactive"):
            self.symbols[symbol].pending_order = False
        
        if s not in ("filled", "partiallyfilled"): return
        if not avg_price or avg_price <= 0 or not filled or filled <= 0: return
        
        action = trade.order.action.upper()
        qty = int(filled)
        price = float(avg_price)
        now = self._now()
        state = self.symbols[symbol]
        
        pnl = 0.0
        duration = 0.0
        pos = "NONE"
        
        # Update State & Save Persistence
        if action == "BUY":
            state.position = "LONG"
            state.quantity = qty
            state.entry_price = price
            state.entry_time = now
            pos = "LONG"
            self._save_symbol_state(symbol) 

        elif action == "SELL":
            if state.entry_price:
                pnl = (price - state.entry_price) * qty
            if state.entry_time:
                duration = (now - state.entry_time).total_seconds()
            state.position = "NONE"
            state.quantity = 0
            state.entry_price = None
            state.entry_time = None
            pos = "NONE"
            self._save_symbol_state(symbol) 
        else:
            return
        
        row = TradeRow(now, symbol, action, price, qty, pnl, duration, pos, status, oid,
                       {"avgFillPrice": avg_price, "filled": filled})
        
        with self.lock:
            self.trade_log_buffer.append(row)
        
        self._flush_trade_log()
        self._logged_orders.add(oid)
        
        msg = f"[{symbol}] ✅ FILL: {action} x{qty} @ {price:.2f}, pnl=${pnl:.2f}, pos={pos}"
        log_status(msg)
        send_alert(msg, APP_NAME) # [ALERT] Fill confirmed
        
        if action == "BUY":
            self._record_action_dedupe(symbol, "BUY")

    def _on_tick(self, ticker, symbol: str):
        if self._stop or not self._should_eval_now(symbol): return
        
        price = ticker.last or ticker.marketPrice() or ticker.close or 0.0
        if price <= 0: return
        
        result = self._compute_vwap_zscore(symbol)
        if not result: return
        
        _, z, vwap = result
        
        action = self.compute_signal(symbol, price, z)
        if not action: return
        
        permitted, info = check_regime_permission(action)
        
        if info.get("status") == "disabled":
            log_status(f"[{symbol}] 🔓 Regime filter disabled")
        elif permitted:
            log_status(f"[{symbol}] ✅ {action} PERMITTED | {info.get('regime_desc', '?')}")
        else:
            log_status(f"[{symbol}] 🚫 {action} BLOCKED | {info.get('regime_desc', '?')}")
            return
        
        self._place_order(symbol, action, price)

    def run(self):
        global CLIENT_ID
        
        log_status("=" * 60)
        log_status("🌡️ REGIME FILTER STATUS")
        if REGIME_CHECK_ENABLED:
            log_status(f"✅ ENABLED | File: {WEATHER_FILE}")
            log_status(f"📊 Required: {REQUIRED_STRATEGIES}")
        else:
            log_status("🔓 DISABLED")
        log_status("=" * 60)

        # Alert on Startup
        send_alert("🚀 Started. Waiting for IBKR connection...", APP_NAME)

        # [OUTER LOOP] Keeps the bot alive forever
        while not self._stop:
            try:
                # --- 1. CONNECT PHASE ---
                if not self.ib.isConnected():
                    log_status(f"Connecting to {HOST}:{PORT} (client={CLIENT_ID})")
                    try:
                        self.ib.connect(HOST, PORT, clientId=CLIENT_ID, readonly=False)
                        log_status("✅ Connected")
                        
                        # [ALERT] Connected
                        send_alert(f"✅ Connected to IBKR (Client {CLIENT_ID})", APP_NAME)
                        
                    except Exception as e:
                        if "already in use" in str(e).lower():
                            CLIENT_ID = bump_client_id(APP_NAME, "strategy")
                            log_status(f"⚠️ Client ID conflict. Bumped to {CLIENT_ID}")
                        else:
                            log_status(f"❌ Connection failed: {e}")
                        
                        log_status("⏳ Retrying in 10 seconds...")
                        time.sleep(10)
                        continue 

                # --- 2. SETUP/SUBSCRIBE PHASE ---
                self.ib.reqAccountSummary()
                
                for sym, state in self.symbols.items():
                    self.ib.qualifyContracts(state.contract)
                    
                    # Backfill History if empty
                    if len(state.bars) == 0:
                        try:
                            bars = self.ib.reqHistoricalData(
                                state.contract, "", "365 D", "1 day", "TRADES", 1, 1, True
                            )
                            for bar in bars:
                                state.bars.append((bar.date, float(bar.close), int(bar.volume)))
                            log_status(f"✅ [{sym}] Loaded {len(state.bars)} bars")
                        except Exception as e:
                            log_status(f"⚠️ [{sym}] Historical data failed: {e}")
                    
                    # Subscribe Live Data
                    try:
                        if state.ticker: self.ib.cancelMktData(state.contract)
                        state.ticker = self.ib.reqMktData(state.contract, "", False, False)
                        state.ticker.updateEvent += lambda t=state.ticker, s=sym: self._on_tick(t, s)
                        log_status(f"✅ [{sym}] Live data subscribed")
                    except Exception as e:
                        log_status(f"⚠️ [{sym}] Live data failed: {e}")
                        
                    self._schedule_next_eval(sym, True)
                    time.sleep(0.2)

                log_status("🚦 Monitoring live signals...")
                self._write_heartbeat("running")

                # --- 3. MONITOR LOOP ---
                while self.ib.isConnected():
                    if self._stop: break
                    self.ib.waitOnUpdate(timeout=2.0)
                    self._write_heartbeat("running")

            except Exception as e:
                # [ALERT] CRITICAL DISCONNECT
                err_msg = f"⚠️ CRITICAL DISCONNECT!\nError: {str(e)}"
                log_status(err_msg)
                send_alert(err_msg, APP_NAME)
                
                self._write_heartbeat("disconnected")
            
            finally:
                if self.ib.isConnected(): self.ib.disconnect()
                
                if not self._stop:
                    log_status("🔄 Attempting Reconnect in 10s...")
                    time.sleep(10)
        
        log_status("🛑 Bot explicitly stopped.")
        send_alert("🛑 Bot stopped manually.", APP_NAME)

# ────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ────────────────────────────────────────────────────────────────
def main():
    runner = VWAPZscoreRunner()
    try:
        runner.run()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()