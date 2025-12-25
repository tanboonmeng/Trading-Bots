"""
SPX Bear Call 0DTE Strategy Runner
Adapted from SPX Bear Call 0DTE.py to ib_insync_runner_template.py

Strategy Logic:
- Monitors SPX index price vs 50-period SMA
- When SPX trades above SMA during trading window (9:45 AM - 3:59 PM ET)
- Sells 0DTE Bear Call Spread:
  * Short call: ~0.20 delta
  * Long call: $5 higher strike
- Trades once per day maximum
"""

import os
import sys
import json
import time
import asyncio
import threading
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, time as dtime

import pandas as pd
import numpy as np
import datetime as dt
import pytz

from ib_insync import (
    IB,
    Stock,
    Index,
    Contract as IBContract,
    MarketOrder,
    Trade,
    Option,
    Contract,
    ComboLeg,
    util,
)

# ─────────────────────────────────────────────────────────────
# PATH & CLIENT ID MANAGER WIRES
# ─────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.client_id_manager import get_or_allocate_client_id, bump_client_id


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
APP_NAME = "0DTE_Black_Bear"

HOST = "127.0.0.1"
PORT = 7497  # 7496 live, 7497 paper
ACCOUNT_ID = "DU3188670"

SYMBOL = "SPX"
SEC_TYPE = "IND"
EXCHANGE = "CBOE"
CURRENCY = "USD"

# Strategy Parameters
SMA_PERIOD = 200
TARGET_DELTA = 0.20
TRADE_WINDOW_START = dtime(10, 00)
TRADE_WINDOW_END = dtime(15, 59)
SPREAD_WIDTH = 5.0  # $5 between strikes

# Trading Rules
MAX_CAPITAL_PER_TRADE_USD = 2000.0  # Not strictly enforced for spreads
MIN_QTY = 1
COOLDOWN_SEC = 3600  # 1 hour cooldown (trades once per day anyway)

# Market Calendar
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

# Logs
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
    ts = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}][{APP_NAME}] {msg}"
    print(line)
    try:
        with open(STATUS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────
def is_trading_day(target_date: datetime) -> bool:
    """Check if target_date is a valid trading day"""
    date_str = target_date.strftime("%Y-%m-%d")
    if date_str in MARKET_HOLIDAYS:
        return False
    if SKIP_FOMC_DAYS and date_str in FOMC_DAYS:
        return False
    if target_date.weekday() not in ALLOWED_WEEKDAYS:
        return False
    return True


def is_within_trade_window() -> bool:
    """Check if current time is within trading window (ET)"""
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern).time()
    return TRADE_WINDOW_START <= now <= TRADE_WINDOW_END


def is_trade_window_ended() -> bool:
    """Check if trading window has ended"""
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern).time()
    return now > TRADE_WINDOW_END


# ─────────────────────────────────────────────────────────────
# STRATEGY RUNNER
# ─────────────────────────────────────────────────────────────
class StrategyRunner:
    def __init__(self) -> None:
        self.ib = IB()
        self.contract = self._build_contract()

        # Position state
        self.current_position: str = "NONE"  # NONE | SPREAD
        self.current_qty: int = 0
        self.entry_price: Optional[float] = None
        self.entry_time: Optional[dt.datetime] = None
        self.has_traded_today: bool = False

        # Price history for SMA
        self.price_history: List[float] = []
        self.prices: List[float] = []

        # Trade throttling
        self.last_trade_time: Optional[dt.datetime] = None
        self.last_action: Optional[str] = None
        self.last_action_price: Optional[float] = None

        # Logging
        self.trade_log_buffer: List[TradeRow] = []
        self.lock = threading.Lock()
        self._logged_order_ids: Dict[int, bool] = {}

        # Market data
        self._ticker = None
        self._stop_requested = False
        
        # Pending spread execution (async-safe)
        self.pending_spread_price: Optional[float] = None

    # ─────────────────────────────────────────────────────
    # CONTRACT BUILDING
    # ─────────────────────────────────────────────────────
    def _build_contract(self):
        """Build SPX Index contract"""
        return Index(SYMBOL, EXCHANGE, CURRENCY)

    # ─────────────────────────────────────────────────────
    # FILE IO
    # ─────────────────────────────────────────────────────
    def _flush_trade_log_buffer(self) -> None:
        """Append new rows to CSV with deduplication"""
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
        """Write heartbeat status file"""
        data = {
            "app_name": APP_NAME,
            "symbol": SYMBOL,
            "status": status,
            "last_update": dt.datetime.now(dt.UTC).isoformat(),
            "position": self.current_position,
            "position_qty": self.current_qty,
            "entry_price": self.entry_price,
            "has_traded_today": self.has_traded_today,
            "last_price": last_price,
        }
        try:
            with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # ─────────────────────────────────────────────────────
    # HELPER METHODS
    # ─────────────────────────────────────────────────────
    def _now(self) -> dt.datetime:
        """Get current time (UTC, timezone-naive)"""
        return dt.datetime.now(dt.UTC).replace(tzinfo=None)

    def _can_trade(self) -> bool:
        """Check if we can trade based on daily limit and time window"""
        if self.has_traded_today:
            return False
        if not is_within_trade_window():
            return False
        
        eastern = pytz.timezone("US/Eastern")
        today = datetime.now(eastern)
        if not is_trading_day(today):
            return False

        return True

    # ─────────────────────────────────────────────────────
    # STRATEGY LOGIC
    # ─────────────────────────────────────────────────────
    def check_above_sma(self, live_price: float) -> tuple[bool, float]:
        """
        Check if live_price is above SMA_PERIOD simple moving average
        Returns: (above_sma: bool, sma_value: float)
        """
        if len(self.price_history) < SMA_PERIOD - 1:
            return False, float('nan')

        # Combine historical prices with live price
        all_prices = self.price_history + [live_price]
        
        if len(all_prices) < SMA_PERIOD:
            return False, float('nan')

        sma_value = np.mean(all_prices[-SMA_PERIOD:])
        above_sma = live_price > sma_value

        return above_sma, float(sma_value)

    async def get_filtered_call_chain(self, underlying_price: float):
        """
        Build and filter SPXW 0DTE call chain
        Returns: (short_leg, long_leg) contracts or (None, None)
        """
        eastern = pytz.timezone("US/Eastern")
        now_et = datetime.now(eastern)
        expiry = now_et.strftime('%Y%m%d')

        # Build strike ladder: round to nearest 5, then 10 strikes OTM + 1 ITM
        base_strike = round(underlying_price / 5) * 5
        strikes = [base_strike + i * 5 for i in range(-2, 10)]
        strikes.sort()

        log_status(f"Candidate strikes: {strikes}")

        # Build option contracts
        contracts = [
            Option(
                symbol='SPX',
                lastTradeDateOrContractMonth=expiry,
                strike=strike,
                right='C',
                exchange='SMART',
                currency='USD',
                tradingClass='SPXW'
            )
            for strike in strikes
        ]

        # Qualify contracts
        qualified = await self.ib.qualifyContractsAsync(*contracts)
        if not qualified:
            log_status("No qualified SPXW call contracts found")
            return None, None

        # Request market data
        tickers = [self.ib.reqMktData(c, '', False, False) for c in qualified]
        await asyncio.sleep(6)  # Wait for modelGreeks

        # Extract deltas
        delta_map = {}
        missing_delta = False
        for contract, ticker in zip(qualified, tickers):
            if ticker.modelGreeks and ticker.modelGreeks.delta is not None:
                delta = ticker.modelGreeks.delta
                log_status(f"{contract.localSymbol}: delta={delta:.4f}")
                delta_map[contract] = delta
            else:
                log_status(f"{contract.localSymbol}: no modelGreeks")
                missing_delta = True

        if not delta_map or missing_delta:
            log_status("Insufficient delta data for chain")
            return None, None

        # Find short leg closest to TARGET_DELTA
        short_leg = min(
            delta_map.items(),
            key=lambda x: abs(x[1] - TARGET_DELTA)
        )[0]
        
        short_delta = delta_map[short_leg]
        log_status(f"Selected short leg: {short_leg.strike} (delta={short_delta:.4f})")

        # Find long leg $5 higher
        long_strike = short_leg.strike + SPREAD_WIDTH
        long_leg = next((c for c in qualified if c.strike == long_strike), None)

        if not long_leg:
            log_status(f"No long leg found at strike {long_strike}")
            return None, None

        return short_leg, long_leg

    def compute_signal(self, price: float) -> Optional[str]:
        """
        Determine if we should trade based on price vs SMA
        Returns: "BUY_SPREAD" or None
        """
        if not self._can_trade():
            return None

        above_sma, sma_value = self.check_above_sma(price)
        
        if not np.isnan(sma_value):
            log_status(
                f"SPX: {price:.2f} | SMA-{SMA_PERIOD}: {sma_value:.2f} | "
                f"Signal: {'BUY_SPREAD' if above_sma else 'HOLD'}"
            )

        if above_sma:
            return "BUY_SPREAD"
        
        return None

    # ─────────────────────────────────────────────────────
    # ORDER EXECUTION
    # ─────────────────────────────────────────────────────
    async def place_spread_order(self, short_leg, long_leg, underlying_price: float):
        """Place bear call spread order"""
        if not short_leg or not long_leg:
            log_status("Cannot place spread: missing leg(s)")
            return

        # Build combo contract
        combo = Contract()
        combo.symbol = "SPX"
        combo.secType = "BAG"
        combo.currency = "USD"
        combo.exchange = "CBOE"
        combo.comboLegs = [
            ComboLeg(conId=short_leg.conId, ratio=1, action='SELL', exchange='CBOE'),
            ComboLeg(conId=long_leg.conId, ratio=1, action='BUY', exchange='CBOE')
        ]

        # Place market order for 1 spread
        order = MarketOrder('BUY', 1)
        if ACCOUNT_ID:
            order.account = ACCOUNT_ID

        trade: Trade = self.ib.placeOrder(combo, order)
        oid = trade.order.orderId
        
        log_status(
            f"Placed Bear Call Spread: "
            f"SELL {short_leg.strike}C / BUY {long_leg.strike}C "
            f"Exp: {short_leg.lastTradeDateOrContractMonth} (orderId={oid})"
        )

        # Attach update handler
        trade.updateEvent += lambda t=trade: self._on_trade_update(t, underlying_price)

        # Mark as traded
        self.has_traded_today = True
        self.last_trade_time = self._now()
        self.last_action = "BUY_SPREAD"
        self.last_action_price = underlying_price

    def _on_trade_update(self, trade: Trade, ref_price: float) -> None:
        """Handle trade fills and log to CSV"""
        status = getattr(trade.orderStatus, "status", None)
        avg_price = getattr(trade.orderStatus, "avgFillPrice", None)
        filled = getattr(trade.orderStatus, "filled", None)
        oid = getattr(trade.order, "orderId", None)

        if oid is None or self._logged_order_ids.get(oid, False):
            return

        if status is None:
            return

        status_lower = status.lower()
        if status_lower not in ("filled", "partiallyfilled"):
            return

        if avg_price is None or filled is None or filled <= 0:
            return

        # Log the spread entry
        now = self._now()
        action = "BUY_SPREAD"
        qty = int(filled)
        price = float(avg_price) if avg_price > 0 else ref_price

        self.current_position = "SPREAD"
        self.current_qty = qty
        self.entry_price = price
        self.entry_time = now

        row = TradeRow(
            timestamp=now,
            symbol=SYMBOL,
            action=action,
            price=price,
            quantity=qty,
            pnl=0.0,  # No PnL at entry
            duration=0.0,
            position=self.current_position,
            status=status,
            ib_order_id=oid,
            extra={
                "avgFillPrice": avg_price,
                "filled": filled,
                "spread_type": "bear_call",
                "strategy": "0DTE",
            },
        )

        with self.lock:
            self.trade_log_buffer.append(row)
        self._flush_trade_log_buffer()

        self._logged_order_ids[oid] = True
        log_status(f"Logged spread fill: {action} x{qty} @ {price:.4f}, status={status}")

    # ─────────────────────────────────────────────────────
    # MARKET DATA HANDLER
    # ─────────────────────────────────────────────────────
    def _on_tick(self, _=None) -> None:
        """Handle live price updates"""
        if self._stop_requested or self._ticker is None:
            return

        price = (
            self._ticker.last
            or self._ticker.marketPrice()
            or self._ticker.close
            or 0.0
        )
        
        if price <= 0:
            return

        price = float(price)
        self.prices.append(price)
        if len(self.prices) > 10_000:
            self.prices = self.prices[-5_000:]

        # Check for signal
        signal = self.compute_signal(price)
        
        if signal == "BUY_SPREAD":
            # Store price for async execution in main loop (thread-safe)
            self.pending_spread_price = price

        self._write_heartbeat(status="running", last_price=price)

    async def _execute_spread(self, underlying_price: float):
        """Execute bear call spread placement"""
        try:
            short_leg, long_leg = await self.get_filtered_call_chain(underlying_price)
            if short_leg and long_leg:
                await self.place_spread_order(short_leg, long_leg, underlying_price)
        except Exception as e:
            log_status(f"Error executing spread: {e}")
        finally:
            # Clear pending flag after execution attempt
            self.pending_spread_price = None

    # ─────────────────────────────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────────────────────────────
    async def load_historical_prices(self):
        """Load previous N-1 closes for SMA calculation"""
        try:
            bars = await self.ib.reqHistoricalDataAsync(
                self.contract,
                endDateTime='',
                durationStr='3 M',
                barSizeSetting='1 day',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1
            )
            df = util.df(bars)
            df['date'] = pd.to_datetime(df['date'])
            
            # Filter to only past dates
            eastern = pytz.timezone("US/Eastern")
            today = datetime.now(eastern).date()
            df = df[df['date'].dt.date < today]
            
            self.price_history = df['close'].tail(SMA_PERIOD - 1).tolist()
            log_status(f"Loaded {len(self.price_history)} historical closes for SMA")
            
        except Exception as e:
            log_status(f"Error loading historical data: {e}")
            self.price_history = []

    def run(self) -> None:
        """Main synchronous entry point"""
        asyncio.run(self.async_run())

    async def async_run(self) -> None:
        """Main async loop"""
        global CLIENT_ID
        
        # Connect to IBKR
        while True:
            try:
                log_status(f"Connecting to IBKR at {HOST}:{PORT} (clientId={CLIENT_ID})")
                await self.ib.connectAsync(HOST, PORT, clientId=CLIENT_ID)
                await self.ib.qualifyContractsAsync(self.contract)
                log_status(f"Connected. Qualified contract: {self.contract}")
                break
            except Exception as e:
                msg = str(e).lower()
                if "client id already in use" in msg:
                    new_id = bump_client_id(name=APP_NAME, role="strategy")
                    CLIENT_ID = new_id
                    log_status(f"Client ID in use. Bumped to {new_id}, retrying...")
                    await asyncio.sleep(2)
                    continue
                else:
                    log_status(f"Fatal connection error: {e}")
                    self._write_heartbeat(status="error_connect")
                    return

        # Load historical data
        await self.load_historical_prices()

        # Subscribe to live data
        self._ticker = self.ib.reqMktData(self.contract, '', False, False)
        self._ticker.updateEvent += self._on_tick
        log_status("Subscribed to SPX market data")

        self._write_heartbeat(status="running")

        try:
            while not self._stop_requested and self.ib.isConnected():
                await asyncio.sleep(1)
                
                # Handle pending spread orders (async-safe execution)
                if self.pending_spread_price is not None:
                    price = self.pending_spread_price
                    self.pending_spread_price = None
                    try:
                        short_leg, long_leg = await self.get_filtered_call_chain(price)
                        if short_leg and long_leg:
                            await self.place_spread_order(short_leg, long_leg, price)
                    except Exception as e:
                        log_status(f"Error executing spread: {e}")
                
                # Reset daily flag if trading window ended
                if is_trade_window_ended() and self.has_traded_today:
                    log_status("Trading window ended. Resetting for next day.")
                    # Wait until next trading day
                    eastern = pytz.timezone("US/Eastern")
                    tomorrow = datetime.now(eastern) + timedelta(days=1)
                    while not is_trading_day(tomorrow):
                        tomorrow += timedelta(days=1)
                    
                    next_start = eastern.localize(
                        datetime.combine(tomorrow.date(), TRADE_WINDOW_START)
                    )
                    wait_seconds = (next_start - datetime.now(eastern)).total_seconds()
                    
                    if wait_seconds > 0:
                        log_status(f"Sleeping until next trading day: {next_start}")
                        await asyncio.sleep(wait_seconds)
                    
                    self.has_traded_today = False
                    await self.load_historical_prices()

        except KeyboardInterrupt:
            log_status("KeyboardInterrupt received")
        except Exception as e:
            log_status(f"Exception in main loop: {e}")
        finally:
            try:
                self._write_heartbeat(status="stopped")
            except Exception:
                pass
            if self.ib.isConnected():
                log_status("Disconnecting from IBKR")
                self.ib.disconnect()
            log_status("Runner stopped")

    def stop(self) -> None:
        """Request shutdown"""
        self._stop_requested = True


# ─────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────
def main() -> None:
    runner = StrategyRunner()
    runner.run()


if __name__ == "__main__":
    main()