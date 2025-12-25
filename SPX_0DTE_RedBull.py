"""
SPX 0DTE Bull Put Spread Runner (Auto-Sleep/Wake Edition)

Strategy:
- Instrument: SPX (S&P 500 Index)
- Direction: Bullish Credit Spread (Bull Put)
- Entry Condition: 
    1. Current Time is between 11:30 AM and 4:00 PM ET.
    2. Live Price > 150-Day SMA (Calculated statically once per day).
    3. No trade executed yet today.
- Execution:
    - Finds Short Put with Delta ~ -0.20.
    - Finds Long Put $5 lower.
    - Places a Market Order (BUY) on the Combo (BAG) contract.
- Schedule:
    - Runs daily. 
    - Automatically sleeps overnight and on holidays/weekends.
    - Wakes up at 09:30 AM ET the next valid trading day.
"""

import os
import sys
import json
import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime, time as dtime, timedelta, timezone
import pytz

import pandas as pd
import numpy as np
import datetime as dt

from ib_insync import (
    IB, Stock, Index, Option, Future, Contract, 
    MarketOrder, Trade, ComboLeg, util
)

# ─────────────────────────────────────────────────────────────
# PATH & CLIENT ID MANAGER WIRES
# ─────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Use existing client_id_manager if available, else fallback to 1
try:
    from utils.client_id_manager import get_or_allocate_client_id
except ImportError:
    def get_or_allocate_client_id(name, role, preferred=None): return 1

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
APP_NAME = "0DTE_Red_Bull"

HOST = "127.0.0.1"
PORT = 7497                 # 7496 for Live, 7497 for Paper
ACCOUNT_ID = 'DU3188670'    # <--- UPDATE THIS

SYMBOL = "SPX"
SEC_TYPE = "IND"
EXCHANGE = "CBOE"
CURRENCY = "USD"

# Strategy Specifics
SMA_PERIOD = 110
TARGET_DELTA = -0.25
TRADE_WINDOW_START = dtime(11, 30)
TRADE_WINDOW_END = dtime(15, 59)

# Capital Management
# Note: SPX spreads require margin. A $5 wide spread usually requires ~$500 margin.
MAX_CAPITAL_PER_TRADE_USD = 5000.0 
MIN_QTY = 1

# Days to Skip
MARKET_HOLIDAYS = [
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26", 
    "2025-06-19", "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25"
]
FOMC_DAYS = [
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18", 
    "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10"
]
SKIP_FOMC_DAYS = False
ALLOWED_WEEKDAYS = [0, 1, 2, 3, 4]  # Mon-Fri

# Logging setup
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

def log_status(msg: str) -> None:
    # UPDATED: Use timezone-aware now() but convert to naive UTC for log consistency
    ts = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
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
        self.contract = self._build_contract() # This is SPX Index (for data)
        
        # Strategy State
        self.daily_sma_value = 0.0
        self.has_traded_today = False
        
        # We hold the calculated spread contract here to pass it from compute->execution
        self.pending_spread_contract: Optional[Contract] = None 

        # Standard Template State
        self.current_position: str = "NONE"
        self.current_qty: int = 0
        self.entry_price: Optional[float] = None
        self.entry_time: Optional[dt.datetime] = None
        self.last_trade_time: Optional[dt.datetime] = None
        self.trade_log_buffer: List[TradeRow] = []
        self.lock = threading.Lock()
        self._ticker = None
        self._stop_requested = False
        self._logged_order_ids: Dict[int, bool] = {}
        self.prices: List[float] = []

    def _build_contract(self):
        # We subscribe to the Index for the signals
        return Index(SYMBOL, EXCHANGE, CURRENCY)

    # ─────────────────────────────
    # UTILITY LOGIC (SPX Specific)
    # ─────────────────────────────
    def _is_valid_trading_day(self) -> bool:
        eastern = pytz.timezone("US/Eastern")
        today_str = datetime.now(eastern).strftime("%Y-%m-%d")
        weekday = datetime.now(eastern).weekday()

        if today_str in MARKET_HOLIDAYS:
            log_status(f"Today {today_str} is a holiday.")
            return False
        if SKIP_FOMC_DAYS and today_str in FOMC_DAYS:
            log_status(f"Today {today_str} is FOMC.")
            return False
        if weekday not in ALLOWED_WEEKDAYS:
            # log_status(f"Today is not an allowed weekday.") 
            return False
        return True

    def _is_within_window(self) -> bool:
        eastern = pytz.timezone("US/Eastern")
        now = datetime.now(eastern).time()
        return TRADE_WINDOW_START <= now <= TRADE_WINDOW_END

    def _calculate_static_sma(self):
        """Fetch 150 daily bars and calculate mean. Run once at startup."""
        log_status(f"Calculating static {SMA_PERIOD}-day SMA...")
        try:
            bars = self.ib.reqHistoricalData(
                self.contract, endDateTime='', durationStr=f'{SMA_PERIOD + 20} D',
                barSizeSetting='1 day', whatToShow='TRADES', useRTH=True, formatDate=1
            )
            df = util.df(bars)
            if df is None or len(df) < SMA_PERIOD:
                log_status(f"Error: Not enough history for SMA. Got {len(df) if df is not None else 0}.")
                return

            # Ensure we don't include today's partial bar if it exists
            today_date = datetime.now().date()
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[df['date'] < today_date]
            
            # Take the last N periods
            closes = df['close'].iloc[-SMA_PERIOD:]
            self.daily_sma_value = closes.mean()
            log_status(f"Static SMA-{SMA_PERIOD} established: {self.daily_sma_value:.2f}")

        except Exception as e:
            log_status(f"Failed to calculate SMA: {e}")

    # ─────────────────────────────
    # COMPLEX OPTION CHAIN LOGIC
    # ─────────────────────────────
    def _find_spread_opportunities(self, underlying_price: float) -> Optional[Contract]:
        """
        Blocking/Synchronous version of chain lookup.
        Finds Short Put (Delta ~ -0.20) and Long Put (Strike - 5).
        Returns a BAG Contract or None.
        """
        log_status("Scanning for spread opportunities...")
        eastern = pytz.timezone("US/Eastern")
        expiry = datetime.now(eastern).strftime('%Y%m%d')

        # 1. Define Strikes (Round to nearest 5, check +/- range)
        base_strike = round(underlying_price / 5) * 5
        strikes = [base_strike + i * 5 for i in range(-12, 5)]

        # 2. Build Option Contracts for Qualification
        contracts = []
        for strike in strikes:
            c = Option(SYMBOL, expiry, strike, 'P', 'SMART', tradingClass='SPXW') # SPXW for dailies
            contracts.append(c)

        # 3. Qualify (Blocking)
        qualified = self.ib.qualifyContracts(*contracts)
        if not qualified:
            log_status("No qualified SPXW puts found.")
            return None

        # 4. Request Market Data to get Deltas (Blocking wait)
        tickers = [self.ib.reqMktData(c, '', False, False) for c in qualified]
        
        # Wait up to 8 seconds for data to populate
        start_wait = time.time()
        while time.time() - start_wait < 8:
            self.ib.sleep(0.2) # Allow IB loop to process messages
            if all(t.modelGreeks is not None for t in tickers):
                break
        
        # 5. Filter by Delta
        candidates = {}
        for c, t in zip(qualified, tickers):
            # Cancel mkt data subscription to save bandwidth
            self.ib.cancelMktData(c)
            if t.modelGreeks and t.modelGreeks.delta:
                candidates[c] = t.modelGreeks.delta

        if not candidates:
            log_status("Could not retrieve Greeks for options.")
            return None

        # 6. Select Short Leg (Closest to TARGET_DELTA)
        # We want delta ~ -0.20. Note puts have negative delta.
        short_leg = min(candidates.items(), key=lambda x: abs(x[1] - TARGET_DELTA))[0]
        short_delta = candidates[short_leg]
        log_status(f"Selected Short Leg: {short_leg.strike} (Delta: {short_delta:.2f})")

        # 7. Select Long Leg ($5 Lower)
        long_strike = short_leg.strike - 5.0
        long_leg = next((c for c in qualified if c.strike == long_strike), None)

        if not long_leg:
            log_status(f"Could not find Long Leg at strike {long_strike}")
            return None
        
        log_status(f"Selected Long Leg: {long_leg.strike}")

        # 8. Build BAG Contract
        contract = Contract()
        contract.symbol = "SPX"
        contract.secType = "BAG"
        contract.currency = "USD"
        contract.exchange = "CBOE"
        contract.comboLegs = [
            ComboLeg(conId=short_leg.conId, ratio=1, action='SELL', exchange='CBOE'),
            ComboLeg(conId=long_leg.conId, ratio=1, action='BUY', exchange='CBOE')
        ]
        return contract

    # ─────────────────────────────
    # STRATEGY SIGNAL
    # ─────────────────────────────
    def compute_signal(self, price: float) -> Optional[str]:
        # 1. Basic Checks
        if self.has_traded_today:
            return None
        
        if not self._is_within_window():
            return None

        # 2. SMA Check
        if self.daily_sma_value == 0.0:
            return None # SMA not calculated yet or failed

        above_sma = price > self.daily_sma_value
        
        # Log periodically (every ~5 mins) to avoid spam
        if len(self.prices) % 300 == 0:
             log_status(f"Price: {price:.2f} | SMA: {self.daily_sma_value:.2f} | Cond: {'Pass' if above_sma else 'Fail'}")

        if not above_sma:
            return None

        # 3. Opportunity Search (Only if trend passes)
        # This is expensive, so we check if we are already in a position or just look once
        # Since has_traded_today handles the "once per day", we can proceed.
        
        log_status(f"Trend valid ({price} > {self.daily_sma_value}). Searching for options...")
        spread_contract = self._find_spread_opportunities(price)

        if spread_contract:
            self.pending_spread_contract = spread_contract
            return "BUY" # Trigger the trade execution

        return None

    # ─────────────────────────────
    # EXECUTION OVERRIDES
    # ─────────────────────────────
    def _qty_for_price(self, price: float) -> int:
        # For spreads, "price" is the credit (often < 5.00), so standard sizing logic fails.
        # We assume ~$500 margin per contract for a $5 wide SPX spread.
        spread_margin = 500.0 
        qty = int(MAX_CAPITAL_PER_TRADE_USD // spread_margin)
        return max(qty, MIN_QTY)

    def _place_order(self, action: str, qty: int, price: float) -> None:
        """
        Override to place the Complex Order instead of a simple Stock order.
        """
        if action == "BUY" and self.pending_spread_contract:
            # We are "Buying" the strategy (which is a credit spread combo)
            contract = self.pending_spread_contract
            order = MarketOrder(action, qty)
            if ACCOUNT_ID:
                order.account = ACCOUNT_ID
            
            log_status(f"Placing Combo Order: {qty}x SPX Bull Put Spread")
            trade = self.ib.placeOrder(contract, order)
            
            # Reset pending
            self.pending_spread_contract = None
            self.has_traded_today = True # Mark as done for the day

            # Hook up logging
            trade.updateEvent += lambda t=trade: self._on_trade_update(t)
            
            self.last_trade_time = self._now()
            self.last_action = action
            self.last_action_price = price
        else:
            log_status("Error: Order signal received but no pending spread contract found.")

    # ─────────────────────────────
    # STANDARD RUNNER METHODS
    # ─────────────────────────────
    def _now(self) -> dt.datetime:
        # UPDATED: Use timezone-aware now() but convert to naive UTC
        return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    def _flush_trade_log_buffer(self) -> None:
        """
        Append new rows to CSV; deduplicate by (timestamp, symbol, action, ib_order_id).
        Ensures the file is sorted by timestamp ascending.
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

        # Load existing log if available
        if os.path.exists(TRADE_LOG_PATH):
            try:
                df_old = pd.read_csv(TRADE_LOG_PATH)
            except Exception:
                df_old = pd.DataFrame()
            df = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df = df_new

        # Standardize timestamp format
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # ---------------------------------------------------------
        # CRITICAL DEDUPLICATION (Restored from Template)
        # ---------------------------------------------------------
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

    def _write_heartbeat(self, status: str = "running", last_price: Optional[float] = None) -> None:
        # UPDATED: Use timezone-aware now() but convert to naive UTC isoformat
        data = {
            "app_name": APP_NAME,
            "symbol": SYMBOL,
            "status": status,
            "last_update": dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).isoformat(),
            "sma_150": self.daily_sma_value,
            "has_traded_today": self.has_traded_today,
            "last_price": last_price,
        }
        try:
            with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception: pass

    def _on_trade_update(self, trade: Trade) -> None:
        status = getattr(trade.orderStatus, "status", None)
        filled = getattr(trade.orderStatus, "filled", None)
        oid = getattr(trade.order, "orderId", None)
        
        if oid is None or self._logged_order_ids.get(oid, False): return
        if status not in ("Filled", "PartiallyFilled"): return
        if filled is None or filled <= 0: return

        action = trade.order.action.upper()
        qty = int(filled)
        price = float(trade.orderStatus.avgFillPrice)
        now = self._now()
        
        row = TradeRow(
            timestamp=now, symbol=SYMBOL, action=action, price=price,
            quantity=qty, pnl=0.0, duration=0.0, position="FLAT",
            status=status, ib_order_id=oid,
            extra={"account": getattr(trade.order, "account", None)}
        )
        with self.lock: self.trade_log_buffer.append(row)
        self._flush_trade_log_buffer()
        self._logged_order_ids[oid] = True
        log_status(f"FILLED: {action} x{qty} @ {price}")

    def _on_tick(self, _=None) -> None:
        if self._stop_requested or self._ticker is None: return
        
        price = (self._ticker.last or self._ticker.close or 0.0)
        if price <= 0: return
        
        self.prices.append(price)
        if len(self.prices) > 1000: self.prices = self.prices[-500:]

        action = self.compute_signal(price)
        if action == "BUY":
            qty = self._qty_for_price(price)
            if qty > 0:
                self._place_order(action, qty, price)
        
        self._write_heartbeat(status="running", last_price=price)

    # ─────────────────────────────
    # MAIN ETERNAL LOOP
    # ─────────────────────────────
    def run(self) -> None:
        global CLIENT_ID
        eastern = pytz.timezone("US/Eastern")

        while True:
            # 1. TIME CHECK: Are we done for the day or is it a holiday?
            now = datetime.now(eastern)
            is_valid_day = self._is_valid_trading_day()
            is_past_window = now.time() > TRADE_WINDOW_END

            if not is_valid_day or is_past_window:
                log_status("Market closed or invalid day. Calculating sleep time...")
                
                # Find the next valid trading day
                next_date = now.date()
                if is_past_window or not is_valid_day:
                    next_date += timedelta(days=1) 

                while True:
                    d_str = next_date.strftime("%Y-%m-%d")
                    weekday = next_date.weekday()
                    
                    if (weekday in ALLOWED_WEEKDAYS and 
                        d_str not in MARKET_HOLIDAYS and 
                        (not SKIP_FOMC_DAYS or d_str not in FOMC_DAYS)):
                        break 
                    next_date += timedelta(days=1)
                
                # Sleep until 09:30 AM ET next valid day
                wake_up_dt = eastern.localize(datetime.combine(next_date, dtime(9, 30)))
                sleep_seconds = (wake_up_dt - datetime.now(eastern)).total_seconds()
                
                if sleep_seconds > 0:
                    log_status(f"Sleeping until {wake_up_dt} ({sleep_seconds/3600:.1f} hours)...")
                    
                    # FORCE HEARTBEAT BEFORE SLEEP
                    self._write_heartbeat(status="sleeping_overnight")
                    
                    # Reset Daily State
                    self.has_traded_today = False
                    self.daily_sma_value = 0.0
                    self.prices = []
                    self.pending_spread_contract = None
                    time.sleep(sleep_seconds)
                    continue 

            # 1.5 PRE-MARKET SLEEP (Wait until 11:30 AM ET)
            if is_valid_day and now.time() < TRADE_WINDOW_START:
                start_dt = eastern.localize(datetime.combine(now.date(), TRADE_WINDOW_START))
                sleep_seconds = (start_dt - datetime.now(eastern)).total_seconds()
                
                if sleep_seconds > 0:
                    log_status(f"Currently {now.time().strftime('%H:%M')} ET. Strategy starts at {TRADE_WINDOW_START}.")
                    log_status(f"Sleeping for {sleep_seconds/3600:.2f} hours until start time...")
                    
                    # FORCE HEARTBEAT BEFORE SLEEP
                    self._write_heartbeat(status="sleeping_premarket")
                    
                    time.sleep(sleep_seconds)
                    continue 

            # 2. CONNECT
            try:
                log_status(f"Connecting to IBKR ({HOST}:{PORT}, ID={CLIENT_ID})...")
                self.ib.connect(HOST, PORT, clientId=CLIENT_ID)
                self.ib.qualifyContracts(self.contract)
                log_status(f"Connected. Data Contract: {self.contract}")
            except Exception as e:
                log_status(f"Connection failed: {e}. Retrying in 60s...")
                self._write_heartbeat(status="error_connecting")
                time.sleep(60)
                continue

            # 3. DAILY SETUP
            if self.daily_sma_value == 0.0:
                self._calculate_static_sma()

            self._ticker = self.ib.reqMktData(self.contract, "", False, False)
            self._ticker.updateEvent += self._on_tick
            log_status("Subscribed to market data. Strategy running...")
            self._write_heartbeat(status="running")

            # 4. INTRADAY EXECUTION LOOP
            try:
                while not self._stop_requested and self.ib.isConnected():
                    self.ib.waitOnUpdate(timeout=1.0)
                    
                    if datetime.now(eastern).time() > TRADE_WINDOW_END:
                        log_status("Trading window ended for today. Disconnecting...")
                        break 
                        
            except KeyboardInterrupt:
                log_status("User stopped manually.")
                self._stop_requested = True
                break
            except Exception as e:
                log_status(f"Runtime error in main loop: {e}")
                time.sleep(10)
            finally:
                if self.ib.isConnected():
                    self.ib.disconnect()
                    log_status("Disconnected. Cycle complete.")
                
                if self._stop_requested:
                    break 

def main() -> None:
    runner = StrategyRunner()
    runner.run()

if __name__ == "__main__":
    main()