"""
Gerald-SuperBrain: Startup Script
Ensures all components are running before starting Gerald.
"""
import subprocess
import sys
import time
import urllib.request
import json
import os
from datetime import datetime


def check_ollama():
    """Check if Ollama is running and models are available."""
    try:
        result = subprocess.run(
            ["ollama", "list"], capture_output=True, text=True, timeout=10
        )
        models = result.stdout
        has_qwen = "qwen2.5:7b" in models or "qwen2.5-coder:14b" in models
        has_mistral = "mistral:latest" in models
        
        if has_qwen and has_mistral:
            print("✅ Ollama: Primary (Qwen) + Fallback (Mistral) models available")
            return True
        else:
            missing = []
            if not has_qwen:
                missing.append("qwen2.5:7b")
            if not has_mistral:
                missing.append("mistral:latest")
            print(f"❌ Ollama: missing models: {', '.join(missing)}")
            return False
    except Exception as e:
        print(f"❌ Ollama not running: {e}")
        return False


def check_chroma():
    """Check if ChromaDB is running."""
    try:
        req = urllib.request.Request("http://localhost:8000/api/v2/heartbeat")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            print(f"✅ ChromaDB: heartbeat OK ({data.get('nanosecond_heartbeat', 'alive')})")
            return True
    except Exception:
        # Try starting Docker container
        print("⚠️  ChromaDB not responding, attempting to start Docker container...")
        try:
            subprocess.run(["docker", "start", "chroma"], capture_output=True, timeout=15)
            time.sleep(3)
            req = urllib.request.Request("http://localhost:8000/api/v2/heartbeat")
            with urllib.request.urlopen(req, timeout=5) as resp:
                print("✅ ChromaDB: started successfully")
                return True
        except Exception as e2:
            print(f"❌ ChromaDB failed to start: {e2}")
            return False


def check_gpu():
    """Check GPU temperature."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=temperature.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10
        )
        parts = result.stdout.strip().split(", ")
        temp = int(parts[0])
        mem_used = int(parts[1])
        mem_total = int(parts[2])
        
        status = "🟢" if temp < 70 else "🟡" if temp < 80 else "🔴"
        print(f"{status} GPU: {temp}°C, VRAM: {mem_used}/{mem_total} MiB")
        
        if temp >= 80:
            print("⚠️  GPU is HOT! Consider reducing num_gpu layers.")
            return False
        return True
    except Exception:
        print("⚠️  GPU check unavailable (no nvidia-smi)")
        return True  # Non-fatal


def check_bridge():
    """Check if Bridge Daemon is active."""
    status_file = r"c:\Gerald-superBrain\bridge\status.json"
    try:
        if not os.path.exists(status_file):
            print("❌ Bridge: status.json missing (Daemon never started?)")
            return False
            
        with open(status_file, 'r') as f:
            data = json.load(f)
            
        last_sync_str = data.get("last_sync")
        if not last_sync_str:
            print("❌ Bridge: status.json has no last_sync timestamp")
            return False
            
        # Handle potential timezone issues by making everything naive for the comparison
        try:
            last_sync = datetime.fromisoformat(last_sync_str.replace('Z', '+00:00'))
            if last_sync.tzinfo:
                last_sync = last_sync.replace(tzinfo=None)
            
            now = datetime.now()
            diff = (now - last_sync).total_seconds()
        except ValueError:
            print(f"❌ Bridge: Invalid timestamp format in status.json: {last_sync_str}")
            return False
        
        if data.get("status") == "running" and diff < 60:
            print(f"✅ Bridge: Active (Last sync: {int(diff)}s ago)")
            return True
        else:
            print(f"❌ Bridge: Inactive or Stale (Status: {data.get('status')}, Last sync: {int(diff)}s ago)")
            return False
    except Exception as e:
        print(f"❌ Bridge check failed: {e}")
        return False


def main():
    print("=" * 50)
    print("🧠 Gerald-SuperBrain — Startup Check")
    print("=" * 50)
    print()
    
    checks = {
        "Ollama": check_ollama(),
        "ChromaDB": check_chroma(),
        "GPU": check_gpu(),
        "Bridge": check_bridge(),
    }
    
    print()
    all_ok = all(checks.values())
    critical_ok = checks["Ollama"] and checks["ChromaDB"]
    
    if all_ok:
        print("✅ All systems GO! Gerald is ready.")
        print()
        print("Start Gerald with:")
        print("  openclaw gateway run")
        print()
        print('Talk to Gerald:')
        print('  openclaw agent --message "Gerald, расскажи о себе"')
    elif critical_ok:
        reasons = []
        if not checks["GPU"]: reasons.append("GPU temp high")
        if not checks["Bridge"]: reasons.append("Bridge daemon inactive")
        print(f"⚠️  Gerald is operational but with warnings ({', '.join(reasons)})")
    else:
        print("❌ Critical components missing. Fix issues above before starting.")
    
    return 0 if critical_ok else 1


if __name__ == "__main__":
    sys.exit(main())
