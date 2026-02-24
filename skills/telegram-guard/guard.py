import sys
import os
import requests
import json
from datetime import datetime

from dotenv import load_dotenv

# Load security environment
BASE_DIR = r"c:\Gerald-superBrain"
load_dotenv(os.path.join(BASE_DIR, ".env"))

def load_config():
    return {
        "bot_token": os.getenv("TELEGRAM_BOT_TOKEN"),
        "default_chat_id": int(os.getenv("TELEGRAM_CHAT_ID", 0)),
        "alert_prefix": "🚨 GERALD: "
    }

def send_alert(message):
    config = load_config()
    token = config["bot_token"]
    chat_id = config.get("default_chat_id")
    
    if not chat_id:
        # Fallback: try to find chat_id from recent updates
        try:
            url = f"https://api.telegram.org/bot{token}/getUpdates"
            r = requests.get(url, timeout=10).json()
            if r.get("ok") and r.get("result"):
                # Get last chat_id that messaged the bot
                chat_id = r["result"][-1]["message"]["chat"]["id"]
                # Save it
                config["default_chat_id"] = chat_id
                with open(CONFIG_PATH, 'w') as f:
                    json.dump(config, f, indent=2)
            else:
                return "ERROR: No chat_id found. Please message the bot first."
        except Exception as e:
            return f"ERROR: Could not fetch updates: {e}"

    prefix = config.get("alert_prefix", "🚨 GERALD: ")
    full_message = f"{prefix}{message}"
    
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": full_message, "parse_mode": "HTML"}
        r = requests.post(url, json=payload, timeout=10).json()
        if r.get("ok"):
            return "SUCCESS"
        else:
            return f"ERROR: {r.get('description')}"
    except Exception as e:
        return f"ERROR: {e}"

if __name__ == "__main__":
    if len(sys.argv) > 1:
        msg = " ".join(sys.argv[1:])
        result = send_alert(msg)
        print(result)
    else:
        print("Usage: python guard.py <message>")
