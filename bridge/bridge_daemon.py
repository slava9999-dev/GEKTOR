"""
Gerald-SuperBrain: Antigravity Bridge Daemon
Actively monitors the bridge directory for incoming requests and process them.
"""
import os
import time
import json
import subprocess
import shutil
from datetime import datetime

# Configuration
BASE_DIR = r"c:\Gerald-superBrain"
BRIDGE_DIR = os.path.join(BASE_DIR, "bridge")
INBOX = os.path.join(BRIDGE_DIR, "inbox")
OUTBOX = os.path.join(BRIDGE_DIR, "outbox")
STATUS_FILE = os.path.join(BRIDGE_DIR, "status.json")

def update_status(inbox_count, outbox_count, status="running", error=None):
    try:
        with open(STATUS_FILE, 'r') as f:
            data = json.load(f)
    except:
        data = {"bridge_version": "1.0.0", "errors": []}
    
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

def process_request(filename):
    file_path = os.path.join(INBOX, filename)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing: {filename}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            prompt = f.read().strip()
            
        if not prompt:
            os.remove(file_path)
            return

        # Call OpenClaw Agent
        # Note: We use global openclaw install since it's an npm package
        result = subprocess.run(
            ["openclaw", "agent", "--message", prompt],
            capture_output=True,
            text=True,
            encoding='utf-8',
            cwd=BASE_DIR,
            timeout=60
        )
        
        response_file = os.path.join(OUTBOX, filename.replace(".msg", ".resp"))
        with open(response_file, 'w', encoding='utf-8') as f:
            if result.returncode == 0:
                f.write(result.stdout)
            else:
                f.write(f"ERROR: {result.stderr}")
                
        # Move processed file to a 'processed' folder (optional, here we just delete)
        os.remove(file_path)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Completed: {filename}")
        
    except Exception as e:
        print(f"❌ Error processing {filename}: {e}")
        update_status(0, 0, status="error", error=str(e))

def gpu_is_safe():
    """Check if GPU is within safe thermal limits (< 80°C)."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=temperature.gpu", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5
        )
        temp = int(result.stdout.strip())
        if temp >= 80:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔥 THERMAL ALERT: GPU at {temp}°C! Pausing processing...")
            return False, temp
        return True, temp
    except:
        return True, 0 # Assume safe if nvidia-smi missing

def main():
    print("🚀 Gerald Bridge Daemon Started")
    print(f"Monitoring: {INBOX}")
    
    last_heartbeat_time = 0
    last_maintenance_time = 0
    paused_by_thermal = False
    
    while True:
        try:
            now_ts = time.time()
            safe, current_temp = gpu_is_safe()
            
            # Maintenance every hour
            if now_ts - last_maintenance_time > 3600:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛠 Running scheduled maintenance...")
                subprocess.run(["python", os.path.join(BASE_DIR, "scripts", "maintenance.py")], 
                               capture_output=True)
                last_maintenance_time = now_ts

            if not safe:
                paused_by_thermal = True
                update_status(0, 0, status="thermal_pause", error=f"GPU Overheat: {current_temp}C")
                time.sleep(30) # Cool down period
                continue
            
            if paused_by_thermal:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🟢 GPU cooled down ({current_temp}°C). Resuming...")
                paused_by_thermal = False

            files = [f for f in os.listdir(INBOX) if f.endswith(".msg")]
            inbox_count = len(files)
            outbox_count = len([f for f in os.listdir(OUTBOX) if f.endswith(".resp")])
            
            update_status(inbox_count, outbox_count)
            
            # Heartbeat log every 30 seconds
            if now_ts - last_heartbeat_time > 30:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 💓 Heartbeat: Bridge active | GPU: {current_temp}°C | Inbox: {inbox_count}")
                last_heartbeat_time = now_ts
            
            for file in files:
                process_request(file)
                
            time.sleep(2)  # Poll every 2 seconds
        except KeyboardInterrupt:
            print("\nShutting down bridge...")
            break
        except Exception as e:
            print(f"Fatal error in daemon loop: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
