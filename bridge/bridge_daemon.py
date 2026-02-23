import requests
import os
import sys
import time
import json
import subprocess
from datetime import datetime

# Configuration
BASE_DIR = r"c:\Gerald-superBrain"
BRIDGE_DIR = os.path.join(BASE_DIR, "bridge")
INBOX = os.path.join(BRIDGE_DIR, "inbox")
OUTBOX = os.path.join(BRIDGE_DIR, "outbox")
STATUS_FILE = os.path.join(BRIDGE_DIR, "status.json")
TG_CONFIG_PATH = os.path.join(BASE_DIR, "skills", "telegram-guard", "config.json")

def get_tg_config():
    try:
        with open(TG_CONFIG_PATH, 'r') as f:
            return json.load(f)
    except:
        return None

def update_status(inbox_count, outbox_count, status="running", error=None):
    try:
        with open(STATUS_FILE, 'r') as f:
            data = json.load(f)
    except:
        data = {"bridge_version": "1.1.0", "errors": [], "tg_offset": 0}
    
    data["status"] = status
    data["last_sync"] = datetime.now().isoformat()
    data["inbox_pending"] = inbox_count
    data["outbox_pending"] = outbox_count
    data["antigravity_available"] = True
    
    if error:
        data["errors"].append(f"[{datetime.now().isoformat()}] {error}")
        if len(data["errors"]) > 10:
            data["errors"] = data["errors"][-10:]
            
    with open(STATUS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def check_telegram():
    config = get_tg_config()
    if not config or not config.get("bot_token"):
        return

    try:
        with open(STATUS_FILE, 'r') as f:
            data = json.load(f)
            offset = data.get("tg_offset", 0)
    except:
        offset = 0

    token = config["bot_token"]
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    
    try:
        resp = requests.get(url, params={"offset": offset, "timeout": 5}, timeout=10)
        updates = resp.json().get("result", [])
        
        for update in updates:
            new_offset = update["update_id"] + 1
            # Update status with new offset
            with open(STATUS_FILE, 'r') as f:
                data = json.load(f)
            data["tg_offset"] = new_offset
            with open(STATUS_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            
            if "message" in update and "text" in update["message"]:
                msg = update["message"]
                chat_id = msg["chat"]["id"]
                text = msg["text"]
                
                # Check if it's Slava
                if chat_id == config.get("default_chat_id"):
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] 💬 Telegram Message from Slava: {text[:30]}...")
                    # Create inbox message specifically for TG
                    msg_id = f"tg_{update['update_id']}"
                    file_path = os.path.join(INBOX, f"{msg_id}.msg")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(text)
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚫 Unauthorized TG access from {chat_id}")
    except Exception as e:
        print(f"⚠️ Telegram poll error: {e}")

def deliver_telegram_responses():
    config = get_tg_config()
    if not config or not config.get("bot_token") or not config.get("default_chat_id"):
        return

    files = [f for f in os.listdir(OUTBOX) if f.startswith("tg_") and f.endswith(".resp")]
    token = config["bot_token"]
    chat_id = config["default_chat_id"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    for file in files:
        file_path = os.path.join(OUTBOX, file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                response = f.read()
            
            # Send to Telegram
            requests.post(url, json={"chat_id": chat_id, "text": f"🧠 GERALD:\n{response}"}, timeout=10)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 📤 Delivered to Telegram: {file}")
            os.remove(file_path)
        except Exception as e:
            print(f"❌ Failed to deliver {file}: {e}")

def process_request(filename):
    file_path = os.path.join(INBOX, filename)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚙️ Processing: {filename}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            prompt = f.read().strip()
            
        if not prompt:
            if os.path.exists(file_path): os.remove(file_path)
            return

        env = os.environ.copy()
        env["OLLAMA_API_KEY"] = "ollama-local"
        
        result = subprocess.run(
            ["npx", "openclaw", "agent", "--agent", "main", "--message", prompt],
            capture_output=True,
            text=True,
            encoding='utf-8',
            cwd=BASE_DIR,
            env=env,
            shell=True,
            timeout=300
        )
        
        response_file = os.path.join(OUTBOX, filename.replace(".msg", ".resp"))
        with open(response_file, 'w', encoding='utf-8') as f:
            if result.returncode == 0:
                f.write(result.stdout)
            else:
                f.write(f"⚠️ Error: {result.stderr}")
                
        if os.path.exists(file_path): os.remove(file_path)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Completed: {filename}")
        
    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")
        update_status(0, 0, status="error", error=str(e))

def gpu_is_safe():
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=temperature.gpu", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5
        )
        temp = int(result.stdout.strip())
        return temp < 80, temp
    except:
        return True, 0

def main():
    print("🚀 Gerald Bridge Daemon 1.1 (Telegram Integrated)")
    print(f"Monitoring: {INBOX} + Telegram Polling")
    
    last_heartbeat_time = 0
    last_maintenance_time = 0
    
    while True:
        try:
            now_ts = time.time()
            safe, current_temp = gpu_is_safe()
            
            # 1. Maintenance
            if now_ts - last_maintenance_time > 3600:
                m_path = os.path.join(BASE_DIR, "scripts", "maintenance.py")
                if os.path.exists(m_path):
                    subprocess.run([sys.executable, m_path], capture_output=True, timeout=300)
                last_maintenance_time = now_ts

            if not safe:
                time.sleep(30)
                continue
            
            # 2. Check Telegram
            check_telegram()
            
            # 3. Process Local Inbox
            files = [f for f in os.listdir(INBOX) if f.endswith(".msg")]
            for file in files:
                process_request(file)
            
            # 4. Deliver Responses back to Telegram
            deliver_telegram_responses()
            
            inbox_count = len([f for f in os.listdir(INBOX) if f.endswith(".msg")])
            outbox_count = len([f for f in os.listdir(OUTBOX) if f.endswith(".resp")])
            update_status(inbox_count, outbox_count)
            
            if now_ts - last_heartbeat_time > 30:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 💓 Online | CPU/GPU OK | TB status: connected")
                last_heartbeat_time = now_ts
                
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Fatal error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()

