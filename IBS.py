"""
IBS (Internal Bar Strength) BASKET Strategy
Live Trading Runner using ib_insync (Dashboard Compatible + Order Ref)

Features:
- Trades a basket of stocks simultaneously (AAPL, MSFT, NVDA, etc.)
- Sets Order Ref to "IBS_BASKET" for easy tracking in TWS
- 100% Dashboard Compatible (TradeRow & CSV logic matches template)
- Checks Weather Station (market_weather.json) for "mean_reversion" permission
- Manages capital per-position (Percentage or Fixed)
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
from datetime import timezone

from ib_insync import (
    IB,
    Stock,
    MarketOrder,
    Trade,
)

# ────────────────────────────────────────────────────────────────────
# PATH & CLIENT ID MANAGER
# ────────────────────────────────────────────────────────────────────
try:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if PROJECT_ROOT not in sys.path:
        sys.path.append(PROJECT_ROOT)
    from utils.client_id_manager import get_or_allocate_client_id, bump_client_id
except ImportError:
    # Fallback if utils missing
    def get_or_allocate_client_id(name, role, preferred=None): return 9001
    def bump_client_id(name, role): return 9002

# ────────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────────
APP_NAME = "IBS"     # <--- This will now appear in TWS Order Ref

HOST = "127.0.0.1"
PORT = 7497                 # 7497 paper, 7496 live
ACCOUNT_ID = "DU3188670"           # Optional: Specify account ID if needed

# ---------------- STOCK BASKET CONFIGURATION ----------------
IBS_STOCKS = [
    {'symbol': 'AAPL',  'buy_thr': 0.08, 'sell_thr': 0.98},
    {'symbol': 'MSFT',  'buy_thr': 0.15, 'sell_thr': 0.97},
    {'symbol': 'NVDA',  'buy_thr': 0.15, 'sell_thr': 0.99},
    {'symbol': 'TSM',   'buy_thr': 0.15, 'sell_thr': 0.99},
    {'symbol': 'META',  'buy_thr': 0.09, 'sell_thr': 0.87},
    {'symbol': 'GOOGL', 'buy_thr': 0.07, 'sell_thr': 0.83},
]

EXCHANGE = "SMART"
CURRENCY = "USD"

# Capital Allocation
CAPITAL_MODE = "PERCENTAGE"      # "FIXED" or "PERCENTAGE"
CAPITAL_FIXED_AMOUNT = 2000.0    # Used if mode is FIXED
CAPITAL_PERCENTAGE = 0.02        # 2% of TotalCashBalance per position
CASH_TAG = "TotalCashBalance" 
MIN_QTY = 1

# Trading Controls
COOLDOWN_SEC = 300               # 5 min between trades (global throttle)
MIN_SAME_ACTION_REPRICE = 0.003
DAILY_SINGLE_ENTRY = True        # Only one entry per symbol per day
REENTRY_COOLDOWN_MIN = 60        # Cooldown after exit before re-entry

# Weather Station Integration
WEATHER_FILE = os.environ.get("WEATHER_FILE", "market_weather.json")
WEATHER_CHECK_INTERVAL = 300     
WEATHER_BYPASS_MODE = os.environ.get("WEATHER_BYPASS_MODE", "0") == "1"
WEATHER_CHECK_TIME = "08:05"     
SLEEP_WHEN_BLOCKED = True        
USE_LOCAL_TIME = True            

# Historical Data
USE_RTH = True
HIST_DURATION = "1 Y"            
HIST_BAR_SIZE = "1 day"
HIST_WHAT = "TRADES"

# Logs
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_ROOT = os.path.join(BASE_DIR, "logs", APP_NAME)
os.makedirs(LOG_ROOT, exist_ok=True)

TRADE_LOG_PATH = os.path.join(LOG_ROOT, "trade_log.csv")
HEARTBEAT_PATH = os.path.join(LOG_ROOT, "heartbeat.json")
STATUS_LOG_PATH = os.path.join(LOG_ROOT, "status.log")
STATE_FILE = os.path.join(LOG_ROOT, "last_actions.json")

CLIENT_ID = get_or_allocate_client_id(name=APP_NAME, role="strategy", preferred=None)


# ────────────────────────────────────────────────────────────────────
# DATA STRUCTURES (EXACT TEMPLATE COPY)
# ────────────────────────────────────────────────────────────────────
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


# ────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ────────────────────────────────────────────────────────────────────
def log_status(msg: str) -> None:
    if USE_LOCAL_TIME:
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tz_info = time.strftime("%Z")
    else:
        ts = dt.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        tz_info = "UTC"
    
    line = f"[{ts} {tz_info}][{APP_NAME}] {msg}"
    print(line)
    try:
        with open(STATUS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def load_weather_report():
    try:
        with open(WEATHER_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        log_status(f"⚠️  Weather file not found: {WEATHER_FILE}")
        return None
    except Exception as e:
        log_status(f"⚠️  Error reading weather: {e}")
        return None

def check_trading_permission(weather_data, strategy_type="mean_reversion"):
    if weather_data is None:
        return False, "UNKNOWN", "STOP"
    try:
        regime_desc = weather_data.get("market_condition", {}).get("regime_desc", "UNKNOWN")
        permissions = weather_data.get("strategy_commands", {})
        permission = permissions.get(strategy_type, "STOP")
        allowed = (permission == "GO")
        return allowed, regime_desc, permission
    except Exception:
        return False, "ERROR", "STOP"

def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


# ────────────────────────────────────────────────────────────────────
# BASKET STRATEGY RUNNER
# ────────────────────────────────────────────────────────────────────
class IBSBasketRunner:
    def __init__(self) -> None:
        self.ib = IB()
        
        # --- Multi-Asset State Management ---
        # Maps Symbol -> Object/Value
        self.contracts: Dict[str, Stock] = {}
        
        # Position State per symbol
        self.positions: Dict[str, str] = {s['symbol']: "NONE" for s in IBS_STOCKS}
        self.quantities: Dict[str, int] = {s['symbol']: 0 for s in IBS_STOCKS}
        self.entry_prices: Dict[str, Optional[float]] = {s['symbol']: None for s in IBS_STOCKS}
        self.entry_times: Dict[str, Optional[dt.datetime]] = {s['symbol']: None for s in IBS_STOCKS}
        
        # Data storage per symbol
        self.daily_bars_map: Dict[str, deque] = {s['symbol']: deque(maxlen=400) for s in IBS_STOCKS}
        self.bars_ready_map: Dict[str, bool] = {s['symbol']: False for s in IBS_STOCKS}
        
        # Config map for easy lookup
        self.config_map = {s['symbol']: s for s in IBS_STOCKS}

        # Capital tracking
        self.account_cash: float = 0.0

        # Global throttles
        self.last_trade_time: Optional[dt.datetime] = None
        self.last_action_map: Dict[str, str] = {} 
        self.last_price_map: Dict[str, float] = {}

        # Logging & IO
        self.trade_log_buffer: List[TradeRow] = []
        self.lock = threading.Lock()
        self._logged_order_ids: Dict[int, bool] = {}

        # Weather
        self.current_weather = None
        self.last_weather_check = None
        self.weather_lock = threading.Lock()
        self.is_sleeping = False
        self.next_weather_check = None

        self.state = load_state()
        
        # Monitoring
        self.last_log_time = None
        self.log_interval_sec = 60
        self.tick_counts: Dict[str, int] = {s['symbol']: 0 for s in IBS_STOCKS}
        self._stop_requested = False

    # ────────────────────────────────────────────────────────────────
    # ACCOUNT
    # ────────────────────────────────────────────────────────────────
    def _on_account_summary(self, val):
        if val.tag == CASH_TAG and val.currency == "BASE":
            try:
                old_cash = self.account_cash
                self.account_cash = float(val.value)
                if old_cash != self.account_cash:
                    log_status(f"💰 Account Cash Updated: ${self.account_cash:,.2f} (BASE)")
            except Exception:
                pass

    # ────────────────────────────────────────────────────────────────
    # FILE IO (DASHBOARD SAFE)
    # ────────────────────────────────────────────────────────────────
    def _flush_trade_log_buffer(self) -> None:
        """
        Append new rows to CSV; deduplicate by (timestamp, symbol, action, ib_order_id).
        Ensures the file is sorted by timestamp ascending.
        MATCHES TEMPLATE EXACTLY.
        """
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

        # Ensure sorted by time for clean metrics & charts
        if "timestamp" in df.columns:
            df = df.sort_values("timestamp")

        df.to_csv(TRADE_LOG_PATH, index=False)

    def _write_heartbeat(self, status="running") -> None:
        # Build status for ALL symbols
        tickers_status = {}
        for sym in self.config_map.keys():
            tickers_status[sym] = {
                "position": self.positions[sym],
                "qty": self.quantities[sym],
                "ibs": self._calculate_ibs(sym)
            }
        
        with self.weather_lock:
             _, regime, perm = check_trading_permission(self.current_weather)

        data = {
            "app_name": APP_NAME,
            "status": status,
            "updated": self._now().isoformat(),
            "cash": self.account_cash,
            "weather": {"regime": regime, "perm": perm},
            "tickers": tickers_status
        }
        try:
            with open(HEARTBEAT_PATH, "w") as f:
                json.dump(data, f, indent=2)
        except: pass

    # ────────────────────────────────────────────────────────────────
    # WEATHER & TIME
    # ────────────────────────────────────────────────────────────────
    def _now(self) -> dt.datetime:
        if USE_LOCAL_TIME:
            return dt.datetime.now().replace(tzinfo=None)
        return dt.datetime.now(timezone.utc).replace(tzinfo=None)

    def _calculate_next_weather_check(self) -> dt.datetime:
        now = self._now()
        target_hour, target_min = map(int, WEATHER_CHECK_TIME.split(':'))
        next_check = now.replace(hour=target_hour, minute=target_min, second=0, microsecond=0)
        if now >= next_check:
            next_check = next_check + dt.timedelta(days=1)
        return next_check

    def _should_check_weather_now(self) -> bool:
        if self.next_weather_check is None: return True
        return self._now() >= self.next_weather_check

    def _update_weather(self):
        with self.weather_lock:
            if WEATHER_BYPASS_MODE:
                log_status("⚠️  WEATHER BYPASS MODE ENABLED")
                self.current_weather = {
                    "market_condition": {"regime_desc": "BYPASS MODE"},
                    "strategy_commands": {"mean_reversion": "GO"}
                }
                self.last_weather_check = self._now()
                self.is_sleeping = False
                return
            
            self.current_weather = load_weather_report()
            self.last_weather_check = self._now()
            self.next_weather_check = self._calculate_next_weather_check()
            
            if self.current_weather:
                regime = self.current_weather.get("market_condition", {}).get("regime_desc", "UNKNOWN")
                perms = self.current_weather.get("strategy_commands", {})
                mr_perm = perms.get("mean_reversion", "STOP")
                
                log_status(f"{'='*60}\n🌡️  WEATHER UPDATE\n   Regime: {regime}\n   MR Permission: {mr_perm}\n   Next Check: {self.next_weather_check}\n{'='*60}")
                
                if SLEEP_WHEN_BLOCKED and mr_perm == "STOP":
                    self.is_sleeping = True
                    log_status("💤 ENTERING SLEEP MODE - Mean Reversion Blocked")
                else:
                    self.is_sleeping = False
                    log_status("✅ ACTIVE MODE - Mean Reversion Allowed")
            else:
                log_status("⚠️  Weather unavailable - BLOCKED")
                self.is_sleeping = True

    def _check_weather_permission(self) -> tuple:
        if self.last_weather_check is None or (not self.is_sleeping and (self._now() - self.last_weather_check).total_seconds() >= WEATHER_CHECK_INTERVAL):
             self._update_weather()
        elif self.is_sleeping and self._should_check_weather_now():
             self._update_weather()
             
        if WEATHER_BYPASS_MODE:
            return True, "BYPASS", "GO"
        
        with self.weather_lock:
            return check_trading_permission(self.current_weather, "mean_reversion")

    # ────────────────────────────────────────────────────────────────
    # TRADING LOGIC
    # ────────────────────────────────────────────────────────────────
    def _record_action(self, symbol: str, side: str):
        key = f"{symbol}:{side}"
        self.state[key] = {"ts": self._now().isoformat()}
        save_state(self.state)

    def _blocked_by_dedupe(self, symbol: str, side: str) -> bool:
        key = f"{symbol}:{side}"
        rec = self.state.get(key)
        if not rec: return False
        try:
            last_ts = dt.datetime.fromisoformat(rec["ts"])
            now = self._now()
            if REENTRY_COOLDOWN_MIN > 0 and (now - last_ts).total_seconds() < (REENTRY_COOLDOWN_MIN * 60):
                return True
            if DAILY_SINGLE_ENTRY and now.date() == last_ts.date():
                return True
        except:
            return False
        return False

    def _qty_for_price(self, price: float) -> int:
        if price <= 0: return 0
        allocated = 0.0
        if CAPITAL_MODE == "PERCENTAGE":
            if self.account_cash > 0: allocated = self.account_cash * CAPITAL_PERCENTAGE
        else:
            allocated = CAPITAL_FIXED_AMOUNT
        
        if allocated <= 0: return 0
        qty = int(allocated // price)
        return max(MIN_QTY, qty) if qty >= MIN_QTY else 0

    def _calculate_ibs(self, symbol: str) -> Optional[float]:
        bars = self.daily_bars_map[symbol]
        if len(bars) < 2: return None
        latest = bars[-1]
        o, h, l, c = latest.open, latest.high, latest.low, latest.close
        if h == l: return 0.5
        return (c - l) / (h - l)

    def _can_trade_symbol(self, symbol: str, action: str, price: float) -> bool:
        now = self._now()
        if self.last_trade_time and (now - self.last_trade_time).total_seconds() < 5:
            return False
            
        last_act = self.last_action_map.get(symbol)
        last_prc = self.last_price_map.get(symbol)
        
        if last_act == action and last_prc:
             if abs(price - last_prc) / last_prc < MIN_SAME_ACTION_REPRICE:
                 return False

        current_pos = self.positions[symbol]
        if action == "BUY" and current_pos == "LONG": return False
        if action == "SELL" and current_pos == "NONE": return False
        return True

    def _process_symbol(self, symbol: str, price: float):
        if not self.bars_ready_map[symbol]: return

        self.tick_counts[symbol] += 1
        now = self._now()
        if self.last_log_time is None or (now - self.last_log_time).total_seconds() > self.log_interval_sec:
             ibs = self._calculate_ibs(symbol)
             log_status(f"[{symbol}] Prc={price:.2f} IBS={ibs if ibs else 'N/A'} Pos={self.positions[symbol]}")
             self.last_log_time = now
             self._write_heartbeat()

        ibs = self._calculate_ibs(symbol)
        if ibs is None: return

        config = self.config_map[symbol]
        action = None
        
        # SELL Logic
        if self.positions[symbol] == "LONG" and ibs > config['sell_thr']:
            log_status(f"📈 [{symbol}] SELL SIG: IBS {ibs:.3f} > {config['sell_thr']}")
            action = "SELL"
            
        # BUY Logic
        elif self.positions[symbol] == "NONE" and ibs < config['buy_thr']:
            allowed, _, _ = self._check_weather_permission()
            if allowed:
                if not self._blocked_by_dedupe(symbol, "BUY"):
                     log_status(f"📉 [{symbol}] BUY SIG: IBS {ibs:.3f} < {config['buy_thr']}")
                     action = "BUY"

        if action:
            if self._can_trade_symbol(symbol, action, price):
                qty = self.quantities[symbol] if action == "SELL" else self._qty_for_price(price)
                if qty > 0:
                    self._place_order(symbol, action, qty, price)

    # ────────────────────────────────────────────────────────────────
    # ORDER EXECUTION
    # ────────────────────────────────────────────────────────────────
    def _place_order(self, symbol: str, action: str, qty: int, price: float):
        contract = self.contracts[symbol]
        order = MarketOrder(action, qty)
        
        # SET ORDER REF FOR TWS VISIBILITY
        order.orderRef = APP_NAME
        
        if ACCOUNT_ID: order.account = ACCOUNT_ID
        
        trade = self.ib.placeOrder(contract, order)
        
        # Use updateEvent for reliable fills (matches template)
        trade.updateEvent += lambda t=trade: self._on_trade_update(t)
        
        self.last_trade_time = self._now()
        self.last_action_map[symbol] = action
        self.last_price_map[symbol] = price
        log_status(f"🚀 [{symbol}] {action} x{qty} sent (Ref: {APP_NAME})")

    def _on_trade_update(self, trade: Trade):
        if not trade.contract: return
        symbol = trade.contract.symbol
        status = trade.orderStatus.status
        filled = trade.orderStatus.filled
        oid = trade.order.orderId
        
        if status.lower() not in ("filled", "partiallyfilled"): return
        if self._logged_order_ids.get(oid): return
        
        if filled > 0:
            avg_price = trade.orderStatus.avgFillPrice
            action = trade.order.action.upper()
            
            # Update Internal State
            if action == "BUY":
                self.positions[symbol] = "LONG"
                self.quantities[symbol] = int(filled)
                self.entry_prices[symbol] = avg_price
                self.entry_times[symbol] = self._now()
                self._record_action(symbol, "BUY")
            elif action == "SELL" and self.positions[symbol] == "LONG":
                if filled >= self.quantities[symbol]:
                     self.positions[symbol] = "NONE"
                     self.quantities[symbol] = 0
            
            self._logged_order_ids[oid] = True
            log_status(f"✅ [{symbol}] FILLED: {action} {filled} @ {avg_price}")
            
            # Dashboard Compatible TradeRow
            row = TradeRow(
                timestamp=self._now(), symbol=symbol, action=action,
                price=avg_price, quantity=int(filled), pnl=0.0, duration=0.0,
                position=self.positions[symbol], status=status, ib_order_id=oid,
                extra={"avgFillPrice": avg_price, "filled": filled}
            )
            with self.lock: self.trade_log_buffer.append(row)
            self._flush_trade_log_buffer()
            self._write_heartbeat()

    # ────────────────────────────────────────────────────────────────
    # MARKET DATA HANDLERS
    # ────────────────────────────────────────────────────────────────
    def _on_bar_update(self, bars, hasNewBar, symbol):
        if hasNewBar:
            self.daily_bars_map[symbol].clear()
            self.daily_bars_map[symbol].extend(bars)
            self.bars_ready_map[symbol] = True

    def _on_tick(self, ticker):
        if self._stop_requested: return
        
        if self.is_sleeping:
             if self._should_check_weather_now():
                 self._update_weather()
             return

        if not ticker.contract: return
        symbol = ticker.contract.symbol
        price = ticker.last or ticker.marketPrice() or ticker.close
        if price and price > 0:
            self._process_symbol(symbol, float(price))

    # ────────────────────────────────────────────────────────────────
    # MAIN
    # ────────────────────────────────────────────────────────────────
    def run(self):
        global CLIENT_ID
        self._update_weather()
        
        while True:
            try:
                log_status(f"Connecting to {HOST}:{PORT} (ID: {CLIENT_ID})...")
                self.ib.connect(HOST, PORT, clientId=CLIENT_ID)
                break
            except Exception as e:
                log_status(f"Conn Error: {e}")
                CLIENT_ID = bump_client_id(APP_NAME, "strategy")
                time.sleep(2)

        log_status("Setting up Basket Contracts...")
        contracts_list = []
        for s in IBS_STOCKS:
            c = Stock(s['symbol'], EXCHANGE, CURRENCY)
            self.contracts[s['symbol']] = c
            contracts_list.append(c)
        
        self.ib.qualifyContracts(*contracts_list)
        log_status(f"✅ Qualified {len(contracts_list)} tickers")

        self.ib.accountSummaryEvent += self._on_account_summary
        self.ib.reqAccountSummary()

        for s in IBS_STOCKS:
            sym = s['symbol']
            c = self.contracts[sym]
            
            bars = self.ib.reqHistoricalData(
                c, endDateTime="", durationStr=HIST_DURATION,
                barSizeSetting=HIST_BAR_SIZE, whatToShow=HIST_WHAT,
                useRTH=int(USE_RTH), formatDate=1, keepUpToDate=True
            )
            bars.updateEvent += lambda b, n, sy=sym: self._on_bar_update(b, n, sy)
            self.daily_bars_map[sym].extend(bars)
            if bars: self.bars_ready_map[sym] = True
            
            tick = self.ib.reqMktData(c, "", False, False)
            tick.updateEvent += self._on_tick
            
        log_status("✅ Market Data Subscribed. Running...")
        
        try:
            self.ib.run()
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            log_status(f"CRASH: {e}")
        finally:
            self.ib.disconnect()

    def stop(self):
        self._stop_requested = True
        self.ib.disconnect()

if __name__ == "__main__":
    runner = IBSBasketRunner()
    runner.run()