import time
from ib_insync import IB

# CONFIGURATION
HOST = "127.0.0.1"
PORT = 4002          # Matches your Gateway port
CLIENT_ID = 9999     # Temporary ID for testing

def test_cash_balance():
    ib = IB()
    print(f"Connecting to {HOST}:{PORT}...")
    
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID)
        print("✅ Connected!")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # Store cash here once found
    found_cash = 0.0
    
    # Define handler
    def on_summary(val):
        nonlocal found_cash
        # print(f"DEBUG: Received {val.tag} ({val.currency}): {val.value}") # Uncomment to see all tags
        if val.tag == "TotalCashBalance" and val.currency == "BASE":
            try:
                found_cash = float(val.value)
            except: pass

    # Hook up the event
    ib.accountSummaryEvent += on_summary
    
    print("Requesting Account Summary...")
    ib.reqAccountSummary()
    
    # Wait Loop (Simulating the fix)
    print("Waiting for 'TotalCashBalance' (BASE)...")
    start_time = time.time()
    
    while found_cash <= 0:
        ib.sleep(1) # Keeps connection alive while waiting
        if time.time() - start_time > 20:
            print("❌ Timeout: Did not receive cash balance within 20 seconds.")
            break
            
    if found_cash > 0:
        print(f"✅ SUCCESS: Retrieved Cash Balance: ${found_cash:,.2f} (BASE)")
    else:
        print("⚠️ Failed to retrieve positive cash balance.")

    ib.disconnect()
    print("Disconnected.")

if __name__ == "__main__":
    test_cash_balance()