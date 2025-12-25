from ib_insync import *
import datetime
import time 

# --- CONFIGURATION ---
PORT = 4002 
SYMBOL = 'NVDA'
RECONNECT_DELAY = 60 # Seconds to wait before reconnecting after a crash

# We use a global variable here so the inner function knows the user's choice
INTERVAL_SECONDS = 60  # Default to 60s

def run_bot_cycle():
    """The main logic for connecting and fetching data."""
    ib = IB()
    try:
        print(f"   ⏳ Attempting to connect to Port {PORT}...")
        ib.connect('127.0.0.1', PORT, clientId=1)
        
        stock = Stock(SYMBOL, 'SMART', 'USD')
        
        while True:
            current_time = datetime.datetime.now().strftime("%H:%M:%S")
            
            # 1. Fetch Data
            print(f"[{current_time}] 🟢 Connected. Fetching data...")
            bars = ib.reqHistoricalData(
                stock, endDateTime='', durationStr='1 D', 
                barSizeSetting='1 hour', whatToShow='TRADES', useRTH=True
            )
            
            if bars:
                print(f"   ✅ Data Received: Close Price ${bars[-1].close}")
            else:
                print("   ⚠️ No data received.")

            # 2. User-Defined Sleep
            print(f"   ...Sleeping for {INTERVAL_SECONDS/60} minutes...")
            ib.sleep(INTERVAL_SECONDS)

    except Exception as e:
        print(f"   ❌ Connection Lost/Error: {e}")
        
    finally:
        try:
            ib.disconnect()
        except:
            pass

# --- OUTER "IMMORTAL" LOOP ---
if __name__ == "__main__":
    
    # 1. Ask User for Interval (Restored Feature)
    try:
        user_input = input("Enter interval in minutes (e.g., 1): ")
        INTERVAL_SECONDS = float(user_input) * 60
    except ValueError:
        print("Invalid input. Defaulting to 1 minute.")
        INTERVAL_SECONDS = 60

    print("--- STARTING IMMORTAL BOT ---")
    
    while True:
        try:
            run_bot_cycle()
        except KeyboardInterrupt:
            print("\n🛑 Stopped by user.")
            break
        except Exception as e:
            print(f"❌ Critical Script Crash: {e}")
        
        print(f"⚠️ Restarting bot in {RECONNECT_DELAY} seconds...")
        time.sleep(RECONNECT_DELAY)