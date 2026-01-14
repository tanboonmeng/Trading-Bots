"""
IBS (Internal Bar Strength) Strategy - Continuous/Live Execution
Integrated with WEATHER STATION (market_weather.json).

Strategy:
- Calculates Live IBS = (Current_Price - Today_Low) / (Today_High - Today_Low)
- Enters LONG IMMEDIATELY if Live IBS < Buy Threshold
- Exits LONG IMMEDIATELY if Live IBS > Sell Threshold
- Manages a BASKET of stocks (AAPL, MSFT, etc.)

Features:
- Auto-Reconnection (Infinity Loop)
- TWS Trade Tracking (OrderRef)
- Regime Integration (Reads market_weather.json)
- Detailed Telegram Alerts
- CRASH FIX: Uses correct 'statusEvent' for trades
- LIVE EXECUTION: Trades on every tick, does not wait for bar close
- FOLDER FIX: Auto-creates log directory if deleted
"""

import os
import sys
import json
import time
import threading
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Deque
from collections import deque

import pandas as pd
import numpy as np
import datetime as dt
import requests

from ib_insync import (
    IB, Stock, MarketOrder, Trade, Contract, BarData
)

# ────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURATION
# ────────────────────────────────────────────────────────────────────────────
APP_NAME = "IBS"
HOST = "127.0.0.1"
PORT = 7497
ACCOUNT_ID = "DU3188670" 

# --- BASKET CONFIGURATION ---
IBS_STOCKS = [
    {'symbol': 'AAPL',  'buy_thr': 0.41, 'sell_thr': 0.61},
    {'symbol': 'MSFT',  'buy_thr': 0.42, 'sell_thr': 0.62},
    {'symbol': 'NVDA',  'buy_thr': 0.43, 'sell_thr': 0.63},
    {'symbol': 'TSM',   'buy_thr': 0.44, 'sell_thr': 0.64},
    {'symbol': 'META',  'buy_thr': 0.45, 'sell_thr': 0.65},
    {'symbol': 'GOOGL', 'buy_thr': 0.46, 'sell_thr': 0.66},
    {'symbol': 'AMZN', 'buy_thr': 0.40, 'sell_thr': 0.66},
]

# --- STRATEGY SETTINGS ---
# [UPDATED] Set to DAILY to calculate "Today's" IBS
TIMEFRAME = "DAILY" 
BAR_SIZE = '1 day' 
HIST_DURATION = '2 D' 

CAPITAL_MODE = "PERCENTAGE"       
CAPITAL_FIXED_AMOUNT = 2000.0     
CAPITAL_PERCENTAGE = 0.02         
MIN_QTY = 1

CASH_TAG = "TotalCashBalance"
# Replace with your credentials
TELEGRAM_BOT_TOKEN = "8555045217:AAFViBEXHbnrNEBvXnGeiGu4v4KsSze32DQ"
TELEGRAM_CHAT_ID = "472283039"

# --- WEATHER STATION INTEGRATION ---
REGIME_CHECK_ENABLED = True
WEATHER_FILE = "market_weather.json"
REQUIRED_STRATEGIES = ["mean_reversion"] 

# ────────────────────────────────────────────────────────────────────────────
# 2. PATHS & LOGGING
# ────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_ROOT = os.path.join(PROJECT_ROOT, "logs", APP_NAME)
os.makedirs(LOG_ROOT, exist_ok=True)

TRADE_LOG_PATH = os.path.join(LOG_ROOT, "trade_log.csv")
HEARTBEAT_PATH = os.path.join(LOG_ROOT, "heartbeat.json")
STATUS_LOG_PATH = os.path.join(LOG_ROOT, "status.log")
STATE_FILE_PATH = os.path.join(LOG_ROOT, f"state_{APP_NAME}.json")

# Try to import client_id_manager
try:
    sys.path.append(os.path.dirname(PROJECT_ROOT))
    from utils.client_id_manager import get_or_allocate_client_id, bump_client_id
    CLIENT_ID = get_or_allocate_client_id(name=APP_NAME, role="strategy")
except ImportError:
    CLIENT_ID = 9029 

def log_status(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}][{APP_NAME}] {msg}"
    print(line)
    try:
        os.makedirs(LOG_ROOT, exist_ok=True)
        with open(STATUS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except: pass

# ────────────────────────────────────────────────────────────────────────────
# 3. DATA STRUCTURES
# ────────────────────────────────────────────────────────────────────────────
@dataclass
class SymbolState:
    symbol: str
    contract: Contract
    bars: Deque[BarData] = field(default_factory=lambda: deque(maxlen=100))
    position: str = "NONE"
    quantity: int = 0
    entry_price: float = 0.0
    buy_thr: float = 0.5
    sell_thr: float = 0.5
    latest_ibs: float = 0.0
    last_update: dt.datetime = field(default_factory=dt.datetime.now)

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
    extra: str = "{}"

# ────────────────────────────────────────────────────────────────────────────
# 4. MAIN RUNNER CLASS
# ────────────────────────────────────────────────────────────────────────────
class IBSRunner:
    def __init__(self):
        self.ib = IB()
        self.lock = threading.Lock()
        self._stop_requested = False
        
        self.cash_balance = 0.0
        self.trade_log_buffer = []
        self._logged_order_ids = {}
        
        self.symbols: Dict[str, SymbolState] = {}
        for cfg in IBS_STOCKS:
            s = cfg['symbol']
            contract = Stock(s, "SMART", "USD")
            self.symbols[s] = SymbolState(
                symbol=s,
                contract=contract,
                buy_thr=cfg['buy_thr'],
                sell_thr=cfg['sell_thr']
            )
            
        self.load_persistence()

    # --- TELEGRAM ---
    def _send_telegram(self, message: str, blocking: bool = False) -> None:
        if "YOUR_" in TELEGRAM_BOT_TOKEN: return 
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        
        def _req():
            try: requests.post(url, json=payload, timeout=5)
            except: pass
        if blocking: _req()
        else: threading.Thread(target=_req, daemon=True).start()

    # --- PERSISTENCE ---
    def load_persistence(self):
        if os.path.exists(STATE_FILE_PATH):
            try:
                with open(STATE_FILE_PATH, 'r') as f:
                    data = json.load(f)
                    for sym, state_data in data.items():
                        if sym in self.symbols:
                            self.symbols[sym].position = state_data.get('position', "NONE")
                            self.symbols[sym].quantity = state_data.get('quantity', 0)
                            self.symbols[sym].entry_price = state_data.get('entry_price', 0.0)
                log_status("♻️ State restored from file.")
            except Exception as e:
                log_status(f"⚠️ Load state failed: {e}")

    def save_persistence(self):
        os.makedirs(LOG_ROOT, exist_ok=True)
        data = {}
        for sym, state in self.symbols.items():
            data[sym] = {
                "position": state.position,
                "quantity": state.quantity,
                "entry_price": state.entry_price
            }
        try:
            with open(STATE_FILE_PATH, 'w') as f:
                json.dump(data, f, indent=2)
        except: pass

    # --- REGIME FILTER (INTEGRATED) ---
    def check_regime_permission(self, action: str) -> tuple:
        if not REGIME_CHECK_ENABLED:
            return True, {"status": "disabled"}
        
        weather_path = os.path.join(PROJECT_ROOT, WEATHER_FILE)
        try:
            if not os.path.exists(weather_path):
                return False, {"status": "no_file"}
            with open(weather_path, "r") as f:
                weather = json.load(f)
                
            regime_desc = weather["market_condition"].get("regime_desc", "?")
            commands = weather.get("strategy_commands", {})
            
            if action == "SELL":
                return True, {"status": "exit_override", "regime": regime_desc}
            
            if action == "BUY":
                perms = [(s, commands.get(s, "STOP")) for s in REQUIRED_STRATEGIES]
                all_go = all(p == "GO" for _, p in perms)
                return all_go, {"status": "go" if all_go else "blocked", "regime": regime_desc}
            
            return False, {"status": "unknown"}
        except:
            return False, {"status": "error"}

    # --- ACCOUNT ---
    def _on_account_summary(self, val):
        if val.tag == CASH_TAG and (val.currency == "USD" or val.currency == "BASE"):
            try: self.cash_balance = float(val.value)
            except: pass

    # --- CALCULATION ---
    def calculate_ibs(self, bar: BarData) -> float:
        rng = bar.high - bar.low
        if rng == 0: return 0.5
        return (bar.close - bar.low) / rng

    def _qty_for_price(self, price: float) -> int:
        if price <= 0: return 0
        capital = 0.0
        
        if CAPITAL_MODE == "FIXED":
            capital = CAPITAL_FIXED_AMOUNT
            log_status(f"💰 Algo: Fixed Capital ${capital:.2f}")
        else:
            if self.cash_balance <= 0:
                log_status(f"⚠️ Algo: Cash Balance unknown or 0. Skipping.")
                return 0
                
            capital = self.cash_balance * CAPITAL_PERCENTAGE
            log_status(f"💰 Algo: Cash ${self.cash_balance:,.2f} * {CAPITAL_PERCENTAGE*100}% = Allocating ${capital:,.2f}")
        
        qty = int(capital // price)
        return max(MIN_QTY, qty)

    # --- EXECUTION ---
    def _place_order(self, state: SymbolState, action: str, price: float):
        qty = 0
        if action == "BUY":
            qty = self._qty_for_price(price)
        elif action == "SELL":
            qty = state.quantity
            
        if qty < MIN_QTY: return

        order = MarketOrder(action, qty)
        if ACCOUNT_ID: order.account = ACCOUNT_ID
        order.orderRef = APP_NAME
        
        trade = self.ib.placeOrder(state.contract, order)
        trade.statusEvent += self._on_trade_update
        
        # ─── NOTIFICATION: ORDER PLACED ───
        msg = (
            f"📊 <b>{APP_NAME}</b>\n"
            f"🔔 Order Placed: <b>{action}</b>\n\n"
            f"📈 Symbol: <code>{state.symbol}</code>\n"
            f"💰 Qty: <code>{qty}</code>\n"
            f"💵 Price: <code>${price:.2f}</code>\n"
            f"💳 Cash: <code>${self.cash_balance:,.2f}</code>\n\n"
            f"⚙️ <b>Capital Settings:</b>\n"
            f"• Mode: <code>{CAPITAL_MODE}</code>\n"
            f"• Fixed: <code>{CAPITAL_FIXED_AMOUNT}</code>\n"
            f"• Pct: <code>{CAPITAL_PERCENTAGE}</code>"
        )
        log_status(f"📤 [{state.symbol}] Placed {action} {qty} @ ~{price:.2f} (Ref: {APP_NAME})")
        self._send_telegram(msg, blocking=False)

    def _on_trade_update(self, trade: Trade):
        if not trade.orderStatus.status in ('Filled', 'PartiallyFilled'): return
        
        fill = trade.orderStatus
        oid = trade.order.orderId
        if oid in self._logged_order_ids: return
        
        sym = trade.contract.symbol
        if sym not in self.symbols: return
        state = self.symbols[sym]
        
        action = trade.order.action
        qty = int(fill.filled)
        price = fill.avgFillPrice
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        
        pnl = 0.0
        if action == "BUY":
            state.position = "LONG"
            state.quantity = qty
            state.entry_price = price
        elif action == "SELL":
            if state.entry_price > 0:
                pnl = (price - state.entry_price) * qty
            state.position = "NONE"
            state.quantity = 0
            state.entry_price = 0.0
            
        self.save_persistence()
        self._logged_order_ids[oid] = True
        
        row = TradeRow(
            timestamp=now, symbol=sym, action=action, price=price,
            quantity=qty, pnl=pnl, duration=0, position=state.position,
            status="Filled", ib_order_id=oid
        )
        self._flush_log(row)
        
        # ─── NOTIFICATION: TRADE CLOSED ───
        msg = (
            f"✅ <b>{APP_NAME}</b>\n"
            f"💰 Trade Closed: <b>{action}</b>\n\n"
            f"📈 Symbol: <code>{sym}</code>\n"
            f"💵 Fill Price: <code>${price:.2f}</code>\n"
            f"📊 Quantity: <code>{qty}</code>\n"
            f"💸 PnL: <code>${pnl:.2f}</code>\n"
            f"💳 Cash: <code>${self.cash_balance:,.2f}</code>"
        )
        log_status(f"✅ [{sym}] FILLED {action} {qty} @ {price:.2f} | PnL: ${pnl:.2f}")
        self._send_telegram(msg, blocking=False)

    def _flush_log(self, row: TradeRow):
        os.makedirs(os.path.dirname(TRADE_LOG_PATH), exist_ok=True)
        file_exists = os.path.exists(TRADE_LOG_PATH)
        df = pd.DataFrame([{
            "timestamp": row.timestamp, "symbol": row.symbol, "action": row.action,
            "price": row.price, "quantity": row.quantity, "pnl": row.pnl,
            "position": row.position, "status": row.status, "ib_order_id": row.ib_order_id
        }])
        df.to_csv(TRADE_LOG_PATH, mode='a', header=not file_exists, index=False)

    def _write_heartbeat(self, status="running"):
        tickers_data = {}
        for s, state in self.symbols.items():
            tickers_data[s] = {
                "pos": state.position,
                "qty": state.quantity,
                "ibs": round(state.latest_ibs, 4),
                "thr": f"{state.buy_thr}/{state.sell_thr}"
            }
            
        data = {
            "app_name": APP_NAME,
            "status": status,
            "last_update": dt.datetime.now(dt.timezone.utc).isoformat(),
            "cash": self.cash_balance,
            "tickers": tickers_data
        }
        try:
            os.makedirs(LOG_ROOT, exist_ok=True)
            with open(HEARTBEAT_PATH, 'w') as f: json.dump(data, f, indent=2)
        except: pass

    # --- MARKET DATA HANDLER (LIVE EXECUTION) ---
    def _on_bar_update(self, bars: list, has_new_bar: bool, symbol: str):
        if not bars: return
        
        state = self.symbols[symbol]
        bar = bars[-1]
        
        # 1. Update IBS Indicator Live
        # Calculates IBS based on "Today's" High/Low and Current Close (Price)
        ibs = self.calculate_ibs(bar)
        state.latest_ibs = ibs
        state.last_update = dt.datetime.now()
        
        # 2. EXECUTE IMMEDIATELY (No "has_new_bar" check)
        # The bot checks every tick. "state.position" prevents duplicate trades.
        
        # BUY LOGIC
        if state.position == "NONE":
            if ibs < state.buy_thr:
                allowed, info = self.check_regime_permission("BUY")
                if not allowed:
                    # Note: We do not log blockages on every tick to avoid log spam
                    return

                log_status(f"🚀 [{symbol}] BUY SIGNAL (IBS {ibs:.3f} < {state.buy_thr})")
                self._place_order(state, "BUY", bar.close)
                
        # SELL LOGIC
        elif state.position == "LONG":
            if ibs > state.sell_thr:
                log_status(f"📉 [{symbol}] SELL SIGNAL (IBS {ibs:.3f} > {state.sell_thr})")
                self._place_order(state, "SELL", bar.close)

    # ────────────────────────────────────────────────────────────────────────────
    # 5. MAIN LOOP (INFINITY)
    # ────────────────────────────────────────────────────────────────────────────
    def run(self):
        global CLIENT_ID
        log_status("="*60)
        log_status(f"🤖 {APP_NAME} STARTING (IBS Strategy - Live Execution)")
        log_status("="*60)
        
        while not self._stop_requested:
            # 1. CONNECT
            while not self._stop_requested:
                try:
                    log_status(f"🔌 Connecting to {HOST}:{PORT}...")
                    self.ib.connect(HOST, PORT, clientId=CLIENT_ID)
                    log_status("✅ Connected.")
                    
                    # ─── NOTIFICATION: STARTUP ───
                    current_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    msg = (
                        f"🤖 <b>{APP_NAME}</b>\n"
                        f"✅ <b>Connected Successfully</b>\n\n"
                        f"📡 Host: <code>{HOST}:{PORT}</code>\n"
                        f"⏰ Time: <code>{current_time}</code>\n\n"
                        f"⚙️ <b>Capital Settings:</b>\n"
                        f"• Mode: <code>{CAPITAL_MODE}</code>\n"
                        f"• Fixed: <code>{CAPITAL_FIXED_AMOUNT}</code>\n"
                        f"• Pct: <code>{CAPITAL_PERCENTAGE}</code>"
                    )
                    self._send_telegram(msg, blocking=False)
                
                    break
                except Exception as e:
                    if "already in use" in str(e):
                        CLIENT_ID = bump_client_id(APP_NAME, "strategy")
                    log_status(f"❌ Connect failed: {e}. Retry 10s...")
                    time.sleep(10)

            if self._stop_requested: break

            # 2. SUBSCRIBE
            self.ib.accountSummaryEvent += self._on_account_summary
            self.ib.reqAccountSummary()
            
            log_status(f"📡 Subscribing to {len(self.symbols)} stocks...")
            for s, state in self.symbols.items():
                self.ib.qualifyContracts(state.contract)
                
                # Use KeepUpToDate for real-time bars
                bars = self.ib.reqHistoricalData(
                    state.contract, endDateTime='', durationStr=HIST_DURATION,
                    barSizeSetting=BAR_SIZE, whatToShow='TRADES', useRTH=True,
                    keepUpToDate=True
                )
                bars.updateEvent += lambda b, n=False, sym=s: self._on_bar_update(b, n, sym)
                log_status(f"   + {s} subscribed.")

            # 3. RUN LOOP
            try:
                while self.ib.isConnected() and not self._stop_requested:
                    self.ib.waitOnUpdate(timeout=1.0)
                    self._write_heartbeat("running")
            except Exception as e:
                log_status(f"⚠️ Crash: {e}")
            finally:
                if self.ib.isConnected(): self.ib.disconnect()
                
                # ─── NOTIFICATION: DISCONNECT ───
                disc_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                exit_reason = "Manual Stop" if self._stop_requested else "Disconnected/Error"

                msg_disc = (
                    f"🛑 <b>{APP_NAME}</b>\n"
                    f"❌ <b>Disconnected / Stopped</b>\n\n"
                    f"⏰ Time: <code>{disc_time}</code>\n"
                    f"❓ Reason: <code>{exit_reason}</code>"
                )

                if self._stop_requested:
                    log_status("🛑 Bot Stopped Manually.")
                    self._send_telegram(msg_disc, blocking=True)
                else:
                    log_status("⚠️ Disconnected. Retrying in 10s...")
                    self._send_telegram(msg_disc, blocking=False)
                    time.sleep(10)

        log_status("🛑 Bot Execution Ended.")

if __name__ == "__main__":
    IBSRunner().run()