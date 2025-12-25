# ============================================================
# NVRP ANALYSIS WITH IBKR DATA
# Historical Implied Volatility for ANY Stock
# Requires: IB Gateway or TWS running with API enabled
# ============================================================
# 
# FIRST TIME SETUP - Run this in your terminal/command prompt:
#   pip install ib_insync pandas numpy matplotlib
#
# ============================================================

from ib_insync import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ----------------------
# CONFIGURATION
# ----------------------
# Connection settings
IB_HOST = '127.0.0.1'     # localhost
IB_PORT = 4002            # 4001=Gateway Live, 4002=Gateway Paper, 7496=TWS Live, 7497=TWS Paper
CLIENT_ID = 88             # Unique client ID

# Analysis settings
TICKER = "nvda"           # Change to any optionable stock: AAPL, NVDA, TSLA, GOOGL, etc.
EXCHANGE = "SMART"        # SMART routing
CURRENCY = "USD"

# Historical data settings
DURATION = "2 Y"          # How much history: "1 Y", "2 Y", "5 Y", etc.
BAR_SIZE = "1 day"        # Bar size for historical data

print("="*60)
print("NVRP ANALYSIS WITH IBKR DATA")
print("="*60)
print(f"\nConfiguration:")
print(f"  Ticker:     {TICKER}")
print(f"  IB Port:    {IB_PORT}")
print(f"  Duration:   {DURATION}")
print(f"\n⚠️  Make sure IB Gateway/TWS is running with API enabled!")


# ============================================================
# CONNECT TO IBKR
# ============================================================

print("\n" + "="*60)
print("CONNECTING TO IBKR...")
print("="*60)

# Create IB instance
ib = IB()

try:
    # Connect to IB Gateway/TWS
    ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID)
    print(f"\n✓ Connected to IBKR")
    print(f"  Server Version: {ib.client.serverVersion()}")
    
    # Check if connected
    if ib.isConnected():
        print(f"  Status: CONNECTED ✓")
    else:
        raise ConnectionError("Failed to establish connection")
        
except Exception as e:
    print(f"\n❌ CONNECTION FAILED: {e}")
    print(f"\nTroubleshooting:")
    print(f"  1. Is IB Gateway/TWS running?")
    print(f"  2. Is API enabled? (Configure > API > Settings)")
    print(f"  3. Is the port correct? (Check API settings)")
    print(f"  4. Try different CLIENT_ID if another app is connected")
    raise


# ============================================================
# DOWNLOAD HISTORICAL IV AND HV DATA
# ============================================================

print("\n" + "="*60)
print(f"DOWNLOADING HISTORICAL DATA FOR {TICKER}")
print("="*60)

# Create contract
contract = Stock(TICKER, EXCHANGE, CURRENCY)

# Qualify the contract (get full contract details)
ib.qualifyContracts(contract)
print(f"\n✓ Contract qualified: {contract}")

# --- Download Historical Implied Volatility ---
print(f"\nDownloading Implied Volatility ({DURATION})...")

iv_bars = ib.reqHistoricalData(
    contract,
    endDateTime='',           # Empty = now
    durationStr=DURATION,
    barSizeSetting=BAR_SIZE,
    whatToShow='OPTION_IMPLIED_VOLATILITY',
    useRTH=True,              # Regular trading hours only
    formatDate=1
)

if not iv_bars:
    print("❌ No IV data returned. Stock may not have options or insufficient history.")
    ib.disconnect()
    raise ValueError(f"No IV data available for {TICKER}")

iv_df = util.df(iv_bars)
iv_df.set_index('date', inplace=True)
iv_df.index = pd.to_datetime(iv_df.index)
print(f"  ✓ IV data: {len(iv_df)} bars")

# --- Download Historical Volatility (Realized) ---
print(f"\nDownloading Historical Volatility ({DURATION})...")

hv_bars = ib.reqHistoricalData(
    contract,
    endDateTime='',
    durationStr=DURATION,
    barSizeSetting=BAR_SIZE,
    whatToShow='HISTORICAL_VOLATILITY',
    useRTH=True,
    formatDate=1
)

if not hv_bars:
    print("❌ No HV data returned.")
    ib.disconnect()
    raise ValueError(f"No HV data available for {TICKER}")

hv_df = util.df(hv_bars)
hv_df.set_index('date', inplace=True)
hv_df.index = pd.to_datetime(hv_df.index)
print(f"  ✓ HV data: {len(hv_df)} bars")

# --- Download Price Data ---
print(f"\nDownloading Price Data ({DURATION})...")

price_bars = ib.reqHistoricalData(
    contract,
    endDateTime='',
    durationStr=DURATION,
    barSizeSetting=BAR_SIZE,
    whatToShow='TRADES',
    useRTH=True,
    formatDate=1
)

price_df = util.df(price_bars)
price_df.set_index('date', inplace=True)
price_df.index = pd.to_datetime(price_df.index)
print(f"  ✓ Price data: {len(price_df)} bars")

# --- Combine into single DataFrame ---
df = pd.DataFrame(index=price_df.index)
df['close'] = price_df['close']
df['IV'] = iv_df['close'].reindex(df.index)    # IV is in 'close' column
df['HV'] = hv_df['close'].reindex(df.index)    # HV is in 'close' column

# Drop NaN rows
df = df.dropna()

print(f"\n✓ Combined dataset: {len(df)} rows")
print(f"\nSample data (last 5 rows):")
print(df.tail().to_string())


# ============================================================
# CALCULATE VRP AND NVRP
# ============================================================

print("\n" + "="*60)
print("CALCULATING VRP AND NVRP")
print("="*60)

# VRP = Implied Vol - Historical Vol
df['VRP'] = df['IV'] - df['HV']

# NVRP = VRP / HV (normalized VRP)
df['NVRP'] = df['VRP'] / df['HV']

# Current values
current_price = df['close'].iloc[-1]
current_iv = df['IV'].iloc[-1]
current_hv = df['HV'].iloc[-1]
current_vrp = df['VRP'].iloc[-1]
current_nvrp = df['NVRP'].iloc[-1]

# VRP statistics
pct_positive = (df['VRP'] > 0).mean() * 100
mean_vrp = df['VRP'].mean()
mean_nvrp = df['NVRP'].mean()

# Dashboard
print(f"\n" + "="*55)
print(f"     {TICKER} VOLATILITY RISK PREMIUM DASHBOARD")
print("="*55)
print(f"""
  Last Price:          ${current_price:.2f}
  Date:                {df.index[-1].strftime('%Y-%m-%d')}
  
  ─────────────────────────────────────────────────────
  30-Day Implied Vol:  {current_iv*100:>6.2f}%
  30-Day Realized Vol: {current_hv*100:>6.2f}%
  ─────────────────────────────────────────────────────
  VRP (IV - HV):       {current_vrp*100:>+6.2f}%
  NVRP (VRP/HV):       {current_nvrp:>+6.2f}x
  ─────────────────────────────────────────────────────
  
  HISTORICAL STATS ({len(df)} trading days):
  VRP positive:        {pct_positive:.1f}% of time
  Mean VRP:            {mean_vrp*100:+.2f}%
  Mean NVRP:           {mean_nvrp:+.2f}x
""")

# Interpretation
if current_vrp > 0.05:
    signal = "📈 POSITIVE VRP"
    interpretation = "Options are OVERPRICED → Favorable to SELL premium"
elif current_vrp > 0:
    signal = "🟡 SLIGHT POSITIVE VRP"
    interpretation = "Options fairly priced → Neutral environment"
else:
    signal = "📉 NEGATIVE VRP"
    interpretation = "Options are UNDERPRICED → Be CAUTIOUS selling premium"

print(f"  SIGNAL: {signal}")
print(f"  {interpretation}")
print("="*55)


# ============================================================
# VISUALIZATION - IV vs HV
# ============================================================

print("\n" + "="*60)
print("GENERATING CHARTS")
print("="*60)

fig, axes = plt.subplots(3, 1, figsize=(14, 12))

# --- Chart 1: IV vs HV ---
ax1 = axes[0]
ax1.plot(df.index, df['IV'] * 100, label='30-Day Implied Volatility (IV)', 
         color='blue', linewidth=1.2)
ax1.plot(df.index, df['HV'] * 100, label='30-Day Realized Volatility (HV)', 
         color='orange', linewidth=1.2)
ax1.set_title(f"{TICKER}: Implied Volatility vs Realized Volatility", 
              fontsize=14, fontweight='bold')
ax1.set_ylabel("Volatility (%)")
ax1.legend(loc='upper right')
ax1.grid(True, alpha=0.3)
ax1.set_xlim(df.index[0], df.index[-1])

# Shade between IV and HV
ax1.fill_between(df.index, df['IV']*100, df['HV']*100, 
                  where=(df['IV'] > df['HV']), color='green', alpha=0.2, label='_')
ax1.fill_between(df.index, df['IV']*100, df['HV']*100, 
                  where=(df['IV'] <= df['HV']), color='red', alpha=0.2, label='_')

# --- Chart 2: VRP ---
ax2 = axes[1]
ax2.plot(df.index, df['VRP'] * 100, label='VRP (IV - HV)', 
         color='steelblue', linewidth=1)
ax2.axhline(0, linestyle='--', color='black', linewidth=1)
ax2.fill_between(df.index, 0, df['VRP']*100, 
                  where=(df['VRP'] > 0), color='green', alpha=0.3, label='Positive VRP')
ax2.fill_between(df.index, 0, df['VRP']*100, 
                  where=(df['VRP'] <= 0), color='red', alpha=0.3, label='Negative VRP')
ax2.set_title(f"{TICKER}: Volatility Risk Premium (IV - HV)", 
              fontsize=14, fontweight='bold')
ax2.set_ylabel("VRP (%)")
ax2.legend(loc='upper right')
ax2.grid(True, alpha=0.3)
ax2.set_xlim(df.index[0], df.index[-1])

# Current VRP annotation
vrp_color = 'green' if current_vrp > 0 else 'red'
ax2.annotate(f"Current VRP: {current_vrp*100:+.1f}%", 
             xy=(0.02, 0.95), xycoords='axes fraction',
             fontsize=11, fontweight='bold', color=vrp_color,
             verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='white', edgecolor=vrp_color, alpha=0.8))

# --- Chart 3: NVRP Distribution ---
ax3 = axes[2]
nvrp_values = df['NVRP'].dropna()
ax3.hist(nvrp_values, bins=80, color='steelblue', edgecolor='black', alpha=0.7)
ax3.axvline(0, linestyle='--', color='red', linewidth=2, label='Zero NVRP')
ax3.axvline(nvrp_values.mean(), linestyle='-', color='green', linewidth=2,
            label=f'Mean: {nvrp_values.mean():.2f}x')
ax3.axvline(current_nvrp, linestyle='-', color='purple', linewidth=2,
            label=f'Current: {current_nvrp:.2f}x')
ax3.set_title(f"{TICKER}: NVRP Distribution (Normalized VRP = VRP/HV)", 
              fontsize=14, fontweight='bold')
ax3.set_xlabel("NVRP (x)")
ax3.set_ylabel("Frequency")
ax3.legend(loc='upper right')
ax3.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(f"{TICKER}_nvrp_charts.png", dpi=150)
print(f"✓ Charts saved to {TICKER}_nvrp_charts.png")
plt.show()


# ============================================================
# NVRP PERCENTILE & REGIME ANALYSIS
# ============================================================

print("\n" + "="*60)
print("NVRP PERCENTILE & REGIME ANALYSIS")
print("="*60)

# Calculate percentiles
current_nvrp_percentile = (df['NVRP'] < current_nvrp).mean() * 100
current_iv_percentile = (df['IV'] < current_iv).mean() * 100
current_hv_percentile = (df['HV'] < current_hv).mean() * 100

print(f"\nCURRENT PERCENTILE RANKS ({len(df)} day history):")
print(f"  IV Percentile:   {current_iv_percentile:>5.1f}%")
print(f"  HV Percentile:   {current_hv_percentile:>5.1f}%")
print(f"  NVRP Percentile: {current_nvrp_percentile:>5.1f}%")

# Regime classification
def get_nvrp_regime(nvrp_pct):
    if nvrp_pct >= 80:
        return "VERY HIGH VRP (Top 20%)", "🔥", "Extremely rich premium"
    elif nvrp_pct >= 60:
        return "HIGH VRP (Top 40%)", "✅", "Rich premium, good for selling"
    elif nvrp_pct >= 40:
        return "NORMAL VRP", "🟡", "Typical environment"
    elif nvrp_pct >= 20:
        return "LOW VRP (Bottom 40%)", "⚠️", "Below average premium"
    else:
        return "VERY LOW VRP (Bottom 20%)", "❌", "Poor premium, be cautious"

regime, emoji, desc = get_nvrp_regime(current_nvrp_percentile)

print(f"\n" + "="*50)
print(f"CURRENT REGIME: {emoji} {regime}")
print(f"  {desc}")
print(f"  NVRP = {current_nvrp:.2f}x ({current_nvrp_percentile:.0f}th percentile)")
print("="*50)

# Regime breakdown
print(f"\nHISTORICAL REGIME BREAKDOWN:")
print(f"  NVRP > 0.5x  (Very High): {(df['NVRP'] > 0.5).mean()*100:>5.1f}% of time")
print(f"  NVRP 0.2-0.5x (High):     {((df['NVRP'] > 0.2) & (df['NVRP'] <= 0.5)).mean()*100:>5.1f}% of time")
print(f"  NVRP 0-0.2x (Normal):     {((df['NVRP'] > 0) & (df['NVRP'] <= 0.2)).mean()*100:>5.1f}% of time")
print(f"  NVRP < 0 (Negative):      {(df['NVRP'] <= 0).mean()*100:>5.1f}% of time")


# ============================================================
# "HARVEST VRP" BACKTEST
# ============================================================

print("\n" + "="*60)
print("'HARVEST VRP' BACKTEST (Illustrative)")
print("="*60)

# Strategy: Capture positive VRP only
SCALE_FACTOR = 0.01  # Conservative: capture 1% of VRP per day

df['daily_pnl'] = df['VRP'].clip(lower=0) * SCALE_FACTOR
df['equity'] = (1 + df['daily_pnl']).cumprod()

# Metrics
total_return = df['equity'].iloc[-1] - 1
years = len(df) / 252
cagr = (df['equity'].iloc[-1] ** (1/years)) - 1 if years > 0 else 0
daily_returns = df['daily_pnl']
sharpe = daily_returns.mean() / daily_returns.std() * np.sqrt(252) if daily_returns.std() > 0 else 0
max_dd = (df['equity'] / df['equity'].cummax() - 1).min()

# Plot
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(df.index, df['equity'], color='steelblue', linewidth=1.5)
ax.set_title(f"{TICKER}: Toy 'Harvest VRP' Equity Curve (Illustrative Only)", 
             fontsize=14, fontweight='bold')
ax.set_ylabel("Equity (Starting at 1.0)")
ax.set_xlabel("Date")
ax.grid(True, alpha=0.3)
ax.set_xlim(df.index[0], df.index[-1])

# Metrics annotation
metrics_text = f"Total Return: {total_return*100:.1f}%\nCAGR: {cagr*100:.2f}%\nSharpe: {sharpe:.2f}\nMax DD: {max_dd*100:.1f}%"
ax.annotate(metrics_text, xy=(0.02, 0.95), xycoords='axes fraction',
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

plt.tight_layout()
plt.savefig(f"{TICKER}_harvest_vrp.png", dpi=150)
print(f"✓ Equity curve saved to {TICKER}_harvest_vrp.png")
plt.show()

print(f"\n📈 BACKTEST RESULTS (Toy Model)")
print(f"   Period: {df.index[0].strftime('%Y-%m-%d')} to {df.index[-1].strftime('%Y-%m-%d')}")
print(f"   Total Return:  {total_return*100:.1f}%")
print(f"   CAGR:          {cagr*100:.2f}%")
print(f"   Sharpe Ratio:  {sharpe:.2f}")
print(f"   Max Drawdown:  {max_dd*100:.1f}%")


# ============================================================
# EXPORT DATA TO CSV
# ============================================================

print("\n" + "="*60)
print("EXPORTING DATA")
print("="*60)

csv_filename = f"{TICKER}_nvrp_data.csv"
df.to_csv(csv_filename)
print(f"✓ Data exported to {csv_filename}")


# ============================================================
# DISCONNECT
# ============================================================

print("\n" + "="*60)
print("CLEANUP")
print("="*60)

ib.disconnect()
print("✓ Disconnected from IBKR")

print(f"\n" + "="*60)
print("ANALYSIS COMPLETE")
print("="*60)
print(f"\nFiles created:")
print(f"  • {TICKER}_nvrp_data.csv (data)")
print(f"  • {TICKER}_nvrp_charts.png (IV vs HV charts)")
print(f"  • {TICKER}_harvest_vrp.png (equity curve)")
