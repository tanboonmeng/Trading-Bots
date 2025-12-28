import yfinance as yf
import pandas as pd
import numpy as np
import json
import datetime
import time
import schedule
from hmmlearn.hmm import GaussianHMM
from sklearn.ensemble import RandomForestClassifier

# ==========================================
# 1. CONFIGURATION
# ==========================================
TICKERS = ["SPY", "HYG", "LQD", "IEF", "SHY"]
START_DATE = "2005-01-01"
OUTPUT_FILE = "market_weather.json"
RUN_TIME = "08:00"

# --- EMOJI MAP ---
REGIME_EMOJIS = {
    0: "☁️  Transition",
    1: "☀️  Calm",
    2: "🌬️  Trend",
    3: "⛈️  Crisis"
}

# --- STRATEGY MATRIX (The "Brain") ---
# 0=Transition, 1=Calm, 2=Trend, 3=Crisis
STRATEGY_PERMISSIONS = {
    0: { # Transition
        "mean_reversion": "WATCH", "trend": "WATCH", "breakout": "STOP",
        "option_selling": "STOP", "option_buying": "STOP",
        "pairs_trading": "STOP", "event_driven": "STOP"
    },
    1: { # Calm
        "mean_reversion": "GO", "trend": "STOP", "breakout": "WATCH",
        "option_selling": "GO", "option_buying": "STOP",
        "pairs_trading": "GO", "event_driven": "GO"
    },
    2: { # Trend
        "mean_reversion": "STOP", "trend": "GO", "breakout": "GO",
        "option_selling": "GO", "option_buying": "GO",
        "pairs_trading": "GO", "event_driven": "GO"
    },
    3: { # Crisis
        "mean_reversion": "STOP", "trend": "STOP", "breakout": "GO",
        "option_selling": "STOP", "option_buying": "GO",
        "pairs_trading": "STOP", "event_driven": "STOP"
    }
}

# ==========================================
# 2. CORE ANALYSIS LOGIC
# ==========================================
def fetch_data():
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 📥 Downloading market data...")
    data = yf.download(TICKERS, start=START_DATE, progress=False)
    df = data["Close"].copy()
    df.dropna(inplace=True)
    return df

def calculate_arm_regime(df):
    work_df = df.copy()
    
    # Returns & Spreads
    work_df["SPY_RET"] = work_df["SPY"].pct_change()
    work_df["HYG_RET"] = work_df["HYG"].pct_change()
    work_df["LQD_RET"] = work_df["LQD"].pct_change()
    work_df["YC_SPREAD"] = work_df["IEF"] - work_df["SHY"]
    work_df["CREDIT_SPREAD_RET"] = work_df["HYG_RET"] - work_df["LQD_RET"]
    work_df["RV20"] = work_df["SPY_RET"].rolling(20).std() * np.sqrt(252)
    work_df.dropna(inplace=True)

    # Volatility State
    low_thr = work_df["RV20"].quantile(0.33)
    high_thr = work_df["RV20"].quantile(0.66)
    
    def get_vol_state(rv):
        if rv < low_thr: return "LOW"
        elif rv > high_thr: return "HIGH"
        else: return "MEDIUM"
    work_df["vol_state"] = work_df["RV20"].apply(get_vol_state)

    # HMM State
    X = work_df["SPY_RET"].values.reshape(-1, 1)
    hmm_model = GaussianHMM(n_components=3, covariance_type="full", n_iter=100, random_state=42)
    hmm_model.fit(X)
    hidden_states = hmm_model.predict(X)
    
    # Identify Crisis (Highest Volatility State)
    state_vars = [hmm_model.covars_[i][0][0] for i in range(3)]
    crisis_state = np.argsort(state_vars)[2]
    work_df["hmm_state"] = ["CRISIS" if s == crisis_state else "NORMAL" for s in hidden_states]

    # Macro Stress
    rolling_mean = work_df["CREDIT_SPREAD_RET"].rolling(60).mean()
    rolling_std = work_df["CREDIT_SPREAD_RET"].rolling(60).std()
    work_df["macro_z"] = (work_df["CREDIT_SPREAD_RET"] - rolling_mean) / rolling_std
    work_df["macro_state"] = work_df["macro_z"].apply(lambda x: "STRESS" if x < -1.5 else "NORMAL")

    # ARM Score Calculation
    def get_arm_score(row):
        if row["hmm_state"] == "CRISIS": return 3
        if row["vol_state"] == "HIGH" and row["macro_state"] == "STRESS": return 3
        if row["vol_state"] == "LOW" and row["macro_state"] == "NORMAL": return 1
        if row["vol_state"] == "MEDIUM": return 2
        return 0

    work_df["ARM_regime"] = work_df.apply(get_arm_score, axis=1)
    return work_df

def predict_future_regime(df):
    data = df.copy()
    data["RET_5"] = data["SPY"].pct_change(5)
    data["RET_20"] = data["SPY"].pct_change(20)
    data["RV_5"] = data["SPY_RET"].rolling(5).std() * np.sqrt(252)
    data["YC_change"] = data["YC_SPREAD"].diff(5)
    data["Target_5d"] = data["ARM_regime"].shift(-5)
    data.dropna(inplace=True)
    
    features = ["RV20", "RV_5", "RET_5", "RET_20", "YC_SPREAD", "YC_change", "ARM_regime"]
    X = data[features]
    y = data["Target_5d"].astype(int)
    
    clf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    clf.fit(X, y)
    
    latest_row = df.iloc[[-1]].copy()
    latest_row["RET_5"] = latest_row["SPY"].pct_change(5)
    latest_row["RET_20"] = latest_row["SPY"].pct_change(20)
    latest_row["RV_5"] = latest_row["SPY_RET"].rolling(5).std() * np.sqrt(252)
    latest_row["YC_change"] = latest_row["YC_SPREAD"].diff(5)
    
    latest_features = latest_row[features].fillna(0)
    return int(clf.predict(latest_features)[0])

# ==========================================
# 3. THE JOB
# ==========================================
def update_weather_report():
    print(f"\n⏰ Waking up: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        raw_df = fetch_data()
        regime_df = calculate_arm_regime(raw_df)
        forecast_5d = predict_future_regime(regime_df)
        
        latest_data = regime_df.iloc[-1]
        current_regime = int(latest_data["ARM_regime"])
        
        # Look up permissions based on today's regime
        strategy_permissions = STRATEGY_PERMISSIONS.get(current_regime, STRATEGY_PERMISSIONS[0])

        weather_report = {
            "last_updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_date": latest_data.name.strftime('%Y-%m-%d'),
            "market_condition": {
                "regime_code": current_regime,
                "regime_desc": REGIME_EMOJIS[current_regime],
                "forecast_5d_code": forecast_5d,
                "forecast_5d_desc": REGIME_EMOJIS[forecast_5d]
            },
            "strategy_commands": strategy_permissions
        }
        
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(weather_report, f, indent=4)
            
        print("-" * 40)
        print(f"🌡️  WEATHER: {REGIME_EMOJIS[current_regime]}")
        print(f"🔮  FORECAST: {REGIME_EMOJIS[forecast_5d]}")
        print(f"🤖  COMMANDS: {json.dumps(strategy_permissions)}")
        print("-" * 40)
        print(f"✅ Saved to {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"❌ ERROR: Failed to update weather report. {e}")
    
    print("💤 Job Done. Sleeping.\n")

# ==========================================
# 4. THE SCHEDULE LOOP
# ==========================================
if __name__ == "__main__":
    print(f"📡 Weather Station Active. Checking every 60 minutes.")
    
    # 1. Run immediately on startup so we don't wait an hour for the first data
    update_weather_report() 
    
    # 2. Schedule it to run every 60 minutes thereafter
    schedule.every(60).minutes.do(update_weather_report) 
    
    while True:
        schedule.run_pending()
        time.sleep(60)