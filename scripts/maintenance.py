"""
Gerald-SuperBrain: Auto-Maintenance & Self-Healing
Checks health, prunes logs, and ensures all services are optimal.
"""
import os
import sys
import subprocess
import json
import time
from datetime import datetime

BASE_DIR = r"c:\Gerald-superBrain"
LOG_FILES = [
    os.path.join(BASE_DIR, "gerald-setup.log"),
]

def check_docker_chroma():
    """Ensure ChromaDB container is running and healthy."""
    try:
        result = subprocess.run(["docker", "ps", "--filter", "name=chroma", "--format", "{{.Status}}"], 
                              capture_output=True, text=True)
        if "Up" not in result.stdout:
            print("⚠️  ChromaDB container down. Attempting restart...")
            subprocess.run(["docker", "start", "chroma"])
            return False
        return True
    except:
        return False

def prune_logs(max_lines=1000):
    """Keep log files at a manageable size."""
    for log_path in LOG_FILES:
        if os.path.exists(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                if len(lines) > max_lines:
                    print(f"🧹 Pruning log: {os.path.basename(log_path)}")
                    with open(log_path, 'w', encoding='utf-8') as f:
                        f.writelines(lines[-max_lines:])
            except Exception as e:
                print(f"❌ Error pruning {log_path}: {e}")

def gpu_safety_check():
    """Verify GPU isn't thermal throttling."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=temperature.gpu", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5
        )
        temp = int(result.stdout.strip())
        if temp > 80:
            print(f"🔥 CRITICAL: GPU temperature at {temp}°C!")
            # Logic to notify model manager or unload models could go here
            return False
        return True
    except:
        return True

def main():
    print(f"[{datetime.now().isoformat()}] 🛠 Running Gerald Maintenance...")
    
    # 1. Self-Healing & Reflection
    check_docker_chroma()
    try:
        reflector_path = os.path.join(BASE_DIR, "scripts", "reflector.py")
        subprocess.run([sys.executable, reflector_path], capture_output=True)
    except:
        pass
    
    # 2. Housekeeping
    prune_logs()
    
    # 3. Safety
    gpu_safety_check()
    
    print("✅ Maintenance complete.")

if __name__ == "__main__":
    main()
