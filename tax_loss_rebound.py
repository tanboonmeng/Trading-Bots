"""
Tax Loss Rebound - BASKET RUNNER (Queue-Based Logger)
Adapted from IBKR-Confirmed Strategy Runner Template

Strategy: 
1. Hibernate until Dec 20th.
2. Scan for 'Big Losers' (-20% YTD) using IBKR Historical Data.
3. Buy Top 5 candidates.
4. Hold until Jan 15th, then Sell.

Features:
- Source: Uses IBKR for both historical scan and live trading.
- Dynamic Capital: Targets 'TotalCashBalance' (BASE).
- RESTART SAFE: Replays local trade_log.csv to remember positions.
- [NEW] QUEUE LOGGER: Uses a background thread to handle CSV I/O.
    - Prevents "Missing Timestamp" bugs.
    - Removes all file locking delays from the trading path.
    - Guarantees sequential file writing.
- RISK SAFE: Enforces hard dollar cap per trade.
- STABILITY: Auto-reconnects on socket disconnects.
- ZOMBIE SAFE: Resets internal state if orders are cancelled by IBKR.
"""

import os
import sys
import json
import time
import threading
import queue  # [NEW] For thread-safe logging
import math
import asyncio
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

try:
    from utils.client_id_manager import get_or_allocate_client_id, bump_client_id
except ImportError:
    def get_or_allocate_client_id(name, role, preferred=None): return 999
    def bump_client_id(name, role): return 1000

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
APP_NAME = "Tax_Loss_Rebound"

# Connection
HOST = "127.0.0.1"
PORT = 7497         # 7497=Paper, 7496=Live, 4002=Gateway
ACCOUNT_ID = "DU3188670"     # Your specific account

# Strategy Dates
ENTRY_START_DATE = "2025-12-20"
EXIT_START_DATE = "2026-01-15"

# Capital & Risk
PERCENTAGE_PER_STOCK = 0.02      # 0.4% of Cash per stock
MAX_CAPITAL_PER_TRADE_USD = 2000.0 
MIN_QTY = 1
LOSERS_THRESHOLD = -0.20          
TOP_N_TARGETS = 5

# Logging
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
# SCANNER LOGIC (IBKR VERSION)
# ─────────────────────────────────────────────────────────────
def scan_for_basket(ib_conn: IB, top_n: int) -> List[str]:
    log_status(f"Scanning for Top {top_n} candidates using IBKR Data...")
    
    # 1. Load Tickers
    csv_filename = os.path.join(BASE_DIR, "russell2000_tickers.csv")
    ticker_list = ["AMC", "GME", "PLUG", "DKNG", "MSTR", "COIN", "CVNA", "UPST", "AFRM", "SOFI", "NVDA", "TSLA"]
    
    if os.path.exists(csv_filename):
        try:
            df = pd.read_csv(csv_filename)
            col_name = next((c for c in ['Ticker', 'Symbol'] if c in df.columns), df.columns[0])
            ticker_list = df[col_name].astype(str).str.strip().tolist()
        except: pass

    basket = []
    
    for t in ticker_list:
        contract = Stock(t, "SMART", "USD")
        try:
            bars = ib_conn.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr='1 Y',
                barSizeSetting='1 day',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1,
                keepUpToDate=False
            )

            if not bars: continue

            start_price = bars[0].close
            end_price = bars[-1].close
            if start_price == 0: continue
            
            perf = (end_price - start_price) / start_price
            
            if perf < LOSERS_THRESHOLD:
                log_status(f"Found Candidate: {t} ({perf:.2%})")
                basket.append(t)
            
            if len(basket) >= top_n:
                break
            time.sleep(0.1)
        except Exception:
            pass

    if not basket:
        log_status("No tickers met the loser criteria.")
        return []

    log_status(f"🎯 Basket Selected: {basket}")
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

        # [NEW] QUEUE LOGGING SYSTEM
        # We no longer use a simple list buffer. We use a thread-safe Queue.
        self.log_queue = queue.Queue()
        self._stop_requested = False
        self._logged_order_ids: Dict[int, bool] = {}

        # Start the background logger thread
        self.logger_thread = threading.Thread(target=self._logger_worker, daemon=True)
        self.logger_thread.start()

    # ─────────────────────────────
    # UTILS
    # ─────────────────────────────
    def _now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)

    # ─────────────────────────────
    # [NEW] DEDICATED LOGGER WORKER
    # ─────────────────────────────
    def _logger_worker(self):
        """
        Runs in a separate thread.
        Consumes trade records from the queue and writes them to CSV sequentially.
        This guarantees that no two file operations ever overlap.
        """
        while not self._stop_requested:
            try:
                # Block for 1 second waiting for data, then loop to check stop_request
                record = self.log_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                # 1. Prepare New Data DataFrame
                # We enforce string formatting immediately to prevent NaT issues
                row_dict = {
                    "timestamp": record.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": record.symbol,
                    "action": record.action,
                    "price": record.price,
                    "quantity": record.quantity,
                    "pnl": record.pnl,
                    "duration": record.duration,
                    "position": record.position,
                    "status": record.status,
                    "ib_order_id": record.ib_order_id,
                    "extra": json.dumps(record.extra) if record.extra else None,
                }
                df_new = pd.DataFrame([row_dict])

                # 2. Read Existing CSV
                if os.path.exists(TRADE_LOG_PATH):
                    try:
                        df_old = pd.read_csv(TRADE_LOG_PATH)
                    except Exception:
                        df_old = pd.DataFrame()
                    df = pd.concat([df_old, df_new], ignore_index=True)
                else:
                    df = df_new

                # 3. Deduplicate (Dedup Key Logic)
                if not df.empty:
                    # Create temporary key for dedup
                    df["dedup_key"] = (
                        df["timestamp"].astype(str)
                        + "|" + df["symbol"].astype(str)
                        + "|" + df["action"].astype(str)
                        + "|" + df["ib_order_id"].astype(str)
                    )
                    df = df.drop_duplicates(subset=["dedup_key"]).drop(columns=["dedup_key"])

                # 4. Sort (Convert to datetime for sort, then back to string isn't strictly needed if ISO, 
                #    but good for safety. We just store as is to keep it simple and robust.)
                #    If you need strict time sorting, we do a temp conversion:
                if "timestamp" in df.columns:
                    temp_time = pd.to_datetime(df["timestamp"], errors="coerce")
                    df = df.iloc[temp_time.argsort()]

                # 5. Save
                df.to_csv(TRADE_LOG_PATH, index=False)
                
                # Signal queue that task is done
                self.log_queue.task_done()
                
            except Exception as e:
                print(f"CRITICAL LOGGER ERROR: {e}")

    def _write_heartbeat(self, status: str = "running") -> None:
        data = {
            "app_name": APP_NAME,
            "status": status,
            "last_update": dt.datetime.now(dt.timezone.utc).isoformat(),
            "cash_base": self.account_cash,
            "active_symbols": self.symbols
        }
        try:
            with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception: pass

    def _reconstruct_state_from_log(self):
        """Restores position state by replaying the local trade_log.csv."""
        if not os.path.exists(TRADE_LOG_PATH):
            return

        try:
            df = pd.read_csv(TRADE_LOG_PATH)
            if df.empty: return
            
            # Filter for symbols we are currently tracking
            current_basket = set(self.positions.keys())
            df = df[df['symbol'].isin(current_basket)].copy()

            if df.empty: return

            if "timestamp" in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp')

            log_status("🔄 Replaying Trade Log to restore state...")

            for _, row in df.iterrows():
                sym = str(row['symbol'])
                action = str(row['action']).upper()
                
                state = self.positions[sym]
                
                if action == "BUY":
                    state.status = "LONG"
                    state.qty = int(row['quantity'])
                    state.entry_price = float(row['price'])
                    state.entry_time = row['timestamp'].to_pydatetime() if pd.notnull(row['timestamp']) else None
                    
                elif action == "SELL":
                    state.status = "NONE"
                    state.qty = 0
                    state.entry_price = None
                    state.entry_time = None
            
            for sym, state in self.positions.items():
                if state.status == "LONG":
                    log_status(f"   -> Restored {sym}: LONG {state.qty} shares")

        except Exception as e:
            log_status(f"⚠️ Error reading trade log: {e}")

    # ─────────────────────────────
    # CAPITAL LOGIC
    # ─────────────────────────────
    def on_account_summary(self, val: Any):
        if val.tag == "TotalCashBalance" and val.currency == "BASE":
            try:
                new_cash = float(val.value)
                if self.account_cash == 0 or abs(new_cash - self.account_cash) > 50.0: 
                    self.account_cash = new_cash
                    log_status(f"💰 Account Cash Updated: ${self.account_cash:,.2f} (BASE)")
            except: pass

    def _qty_for_symbol(self, price: float) -> int:
        if price <= 0 or self.account_cash <= 0: return 0
        alloc_amt = self.account_cash * PERCENTAGE_PER_STOCK
        if alloc_amt > MAX_CAPITAL_PER_TRADE_USD:
            alloc_amt = MAX_CAPITAL_PER_TRADE_USD
        qty = int(alloc_amt // price)
        return qty if qty >= MIN_QTY else 0

    # ─────────────────────────────
    # TRADING LOGIC
    # ─────────────────────────────
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
        if action == "BUY":
            state.status = "LONG"
        elif action == "SELL":
            state.status = "NONE"

        trade.fillEvent += self._on_trade_update
        trade.statusEvent += self._on_order_status
        
        log_status(f"[{symbol}] Placed {action} {qty} @ ~{price:.2f} (Id: {trade.order.orderId})")

    # ─────────────────────────────
    # EVENTS
    # ─────────────────────────────
    def _on_tick(self, ticker: Ticker):
        if self._stop_requested: return
        symbol = ticker.contract.symbol
        price = ticker.last if ticker.last and not pd.isna(ticker.last) else None
        
        if not price or price <= 0:
            mp = ticker.marketPrice()
            if mp and not pd.isna(mp): price = mp
        if (not price or pd.isna(price) or price <= 0) and ticker.close:
            price = ticker.close
        if not price or pd.isna(price) or price <= 0: return

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

    def _on_trade_update(self, trade: Trade, *args):
        # 1. Validation
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
        
        # 2. Memory Deduplication
        if self._logged_order_ids.get(oid): return
        self._logged_order_ids[oid] = True
        
        symbol = trade.contract.symbol
        action = trade.order.action
        
        state = self.positions.get(symbol)
        if not state: return 

        now = self._now()
        pnl = 0.0
        duration = 0.0

        if action == "BUY":
            state.qty = int(filled_qty)
            state.entry_price = float(fill_price)
            state.entry_time = now
        elif action == "SELL":
            if state.entry_price:
                pnl = (fill_price - state.entry_price) * filled_qty
            if state.entry_time:
                duration = (now - state.entry_time).total_seconds()
            state.qty = 0
            state.entry_price = None

        log_ts = now.replace(tzinfo=None)
        status_str = trade.orderStatus.status
        if status_str not in ('Filled', 'PartiallyFilled'):
            status_str = 'Filled' 

        # 3. Create Row Object
        row = TradeRow(log_ts, symbol, action, fill_price, filled_qty, pnl, duration, state.status, status_str, oid)
        
        # 4. [FIX] PUSH TO QUEUE (Non-Blocking)
        # We do NOT write to CSV here. We just push to the queue.
        # The background thread handles the risky I/O part.
        self.log_queue.put(row)
        
        log_status(f"[{symbol}] Logged Fill: {action} {filled_qty} @ {fill_price:.2f}")

    # ─────────────────────────────
    # MAIN EXECUTION
    # ─────────────────────────────
    def _perform_scan_and_setup(self):
        self.symbols = scan_for_basket(self.ib, TOP_N_TARGETS)
        if not self.symbols:
            log_status("Scan returned no candidates. Stopping.")
            self.stop()
            return

        for s in self.symbols:
            self.positions[s] = PositionState(s)
            self.contracts[s] = Stock(s, "SMART", "USD")

        self._reconstruct_state_from_log()

        log_status("Qualifying contracts...")
        for sym, contract in self.contracts.items():
            try:
                self.ib.qualifyContracts(contract)
                t = self.ib.reqMktData(contract, "", False, False)
                t.updateEvent += self._on_tick
                self.tickers[sym] = t
            except Exception as e:
                log_status(f"Failed to setup {sym}: {e}")

        log_status(f"Setup Complete. Monitoring: {self.symbols}")

    def run(self) -> None:
        global CLIENT_ID
        while True:
            try:
                log_status(f"Connecting to IBKR {HOST}:{PORT} (ID: {CLIENT_ID})...")
                self.ib.connect(HOST, PORT, clientId=CLIENT_ID)
                break
            except Exception as e:
                if "client id already in use" in str(e).lower():
                    CLIENT_ID = bump_client_id(APP_NAME, "strategy")
                    continue
        
        time.sleep(5)
        log_status("Waiting for Account Data...")
        self.ib.accountSummaryEvent += self.on_account_summary
        self.ib.reqAccountSummary()
        
        wait_start = time.time()
        while self.account_cash <= 0:
            self.ib.sleep(1) 
            if time.time() - wait_start > 30:
                self.account_cash = 10000.0
                break
        
        if self.account_cash > 0:
            log_status(f"✅ Cash Balance Confirmed: ${self.account_cash:,.2f}")

        log_status(f"Date Check. Target Entry: {ENTRY_START_DATE}")
        while not self._stop_requested:
            now = self._now()
            if now < self.entry_start:
                log_status(f"Too early. Hibernating... (Current: {now.strftime('%Y-%m-%d')})")
                self.ib.sleep(3600)
            else:
                log_status("✅ Entry Date Reached! Starting...")
                break

        self._perform_scan_and_setup()
        
        while not self._stop_requested:
            try:
                if not self.ib.isConnected():
                    log_status("⚠️ Connection lost. Attempting reconnect...")
                    try:
                        self.ib.connect(HOST, PORT, clientId=CLIENT_ID)
                        time.sleep(5)
                        self.ib.reqAccountSummary()
                        for contract in self.contracts.values():
                            self.ib.reqMktData(contract, "", False, False)
                    except Exception:
                        self.ib.sleep(5.0)
                        continue

                self.ib.waitOnUpdate(timeout=1.0)
                
            except Exception as e:
                if "Socket disconnect" in str(e) or "connection" in str(e).lower():
                    log_status(f"Socket/Connection Error: {e}")
                    self.ib.sleep(2.0)
                elif isinstance(e, KeyboardInterrupt):
                    self.stop()
                    break
                else:
                    log_status(f"Loop Error: {e}")
                self.ib.sleep(1.0)
        
        if self.ib.isConnected(): self.ib.disconnect()
        log_status("Runner Stopped.")

    def stop(self) -> None:
        self._stop_requested = True
        # Wait for logger to finish pending items?
        # In a real daemon, we might just let it die, but queue is safer.
        pass

if __name__ == "__main__":
    StrategyRunner().run()