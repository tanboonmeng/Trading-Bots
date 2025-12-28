import requests
import sys
import os

# ────────────────────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────────────────────
# You only need to set these ONCE here.
ENABLE_TELEGRAM = True
TELEGRAM_BOT_TOKEN = "8555045217:AAFViBEXHbnrNEBvXnGeiGu4v4KsSze32DQ"  # <--- Paste Token
TELEGRAM_CHAT_ID = "472283039"              # <--- Your ID

def send_alert(message: str, app_name: str = "BOT"):
    """
    Sends a message to Telegram.
    Args:
        message (str): The text to send.
        app_name (str): Optional name of the bot sending the alert.
    """
    if not ENABLE_TELEGRAM:
        return

    # Add a bold header with the App Name for clarity
    formatted_msg = f"🔔 <b>[{app_name}]</b>\n{message}"

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": formatted_msg,
            "parse_mode": "HTML"
        }
        # Short timeout so it doesn't block trading if internet is slow
        requests.post(url, json=payload, timeout=3)
    except Exception as e:
        print(f"⚠️ Telegram Alert Failed: {e}")