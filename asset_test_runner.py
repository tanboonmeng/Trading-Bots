"""
Basic Test Runner (no IBKR)
- Generates a fake trade_log.csv compatible with the read-only dashboard.
- Use this just to verify that the dashboard can:
    • discover the strategy
    • load trade_log.csv
    • compute metrics
    • render tables & charts
"""

import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd

# ==============================
# CONFIG — EDIT IF YOU WISH
# ==============================
# This is the "Strategy Name" that will appear on the dashboard
APP_NAME = "ASSET"  # or "ASSET_TEST", "DEMO_STRATEGY", etc.

SYMBOL = "ASSET"    # just a label; dashboard treats it as a ticker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # strategies_runner/
LOG_ROOT = os.path.join(BASE_DIR, "logs", APP_NAME)
os.makedirs(LOG_ROOT, exist_ok=True)

TRADE_LOG_PATH = os.path.join(LOG_ROOT, "trade_log.csv")


def generate_dummy_trades(n: int = 10) -> pd.DataFrame:
    """
    Create a small set of fake trades:
    BUY → SELL pairs with simple PnL and durations.
    Columns match the dashboard EXPECTED_COLUMNS.
    """
    rows: List[Dict[str, Any]] = []

    base_time = datetime.utcnow().replace(microsecond=0)

    # simple pattern: BUY, SELL, BUY, SELL, ...
    buy_price = None
    buy_time = None
    position = "NONE"
    ib_order_id = 1000

    for i in range(n):
        ts = base_time + timedelta(minutes=5 * i)
        action = "BUY" if i % 2 == 0 else "SELL"
        price = 100 + i  # just a rising price
        quantity = 1     # min qty

        if action == "BUY":
            # entering long
            buy_price = price
            buy_time = ts
            pnl = 0.0
            duration = 0.0
            position = "LONG"
        else:
            # exiting long
            if buy_price is not None:
                pnl = (price - buy_price) * quantity
            else:
                pnl = 0.0

            if buy_time is not None:
                duration = (ts - buy_time).total_seconds()
            else:
                duration = 0.0

            position = "NONE"

        row = {
            "timestamp": ts.isoformat(),  # dashboard will parse & localize
            "symbol": SYMBOL,
            "action": action,
            "price": float(price),
            "quantity": int(quantity),
            "pnl": float(pnl),
            "duration": float(duration),
            "position": position,
            "status": "Filled",        # pretend IBKR filled it
            "ib_order_id": ib_order_id,
        }
        rows.append(row)
        ib_order_id += 1

    df = pd.DataFrame(rows)
    return df


def write_trade_log():
    new_df = generate_dummy_trades(n=10)

    if os.path.exists(TRADE_LOG_PATH):
        try:
            old_df = pd.read_csv(TRADE_LOG_PATH)
        except Exception:
            old_df = pd.DataFrame()
        df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        df = new_df

    # Deduplicate by ib_order_id, sort by timestamp
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    df["dedup_key"] = df["ib_order_id"].astype(str)
    df = df.drop_duplicates(subset=["dedup_key"]).drop(columns=["dedup_key"])

    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")

    df.to_csv(TRADE_LOG_PATH, index=False)
    print(f"Wrote {len(new_df)} dummy trades to {TRADE_LOG_PATH}")


if __name__ == "__main__":
    write_trade_log()
