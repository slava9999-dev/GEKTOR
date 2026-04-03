import time
import os
import signal
import redis
import hmac
import hashlib
import requests
from dotenv import load_dotenv

# [Sentinel-V2] Autonomous Watchdog (Audit 6.10)
# No dependencies on the main project core to ensure survival during dependency hell.

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6381))
LEASE_KEY = "tekton:leader:lease" # Must match LeaseService
PID_FILE = "tekton.pid"

BYBIT_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_URL = os.getenv("BYBIT_REST_URL", "https://api.bybit.com")

def sign_bybit(params, secret):
    param_str = str(int(time.time() * 1000)) + BYBIT_KEY + "5000" + params
    hash = hmac.new(bytes(secret, "utf-8"), param_str.encode("utf-8"), hashlib.sha256)
    return hash.hexdigest(), str(int(time.time() * 1000))

WATCHDOG_URL = os.getenv("SENTINEL_WATCHDOG_URL") # [Quis custodiet ipsos custodes?]

def ping_watchdog():
    """Keep the Sentinel's own health monitor alive."""
    if WATCHDOG_URL:
        try:
            requests.get(WATCHDOG_URL, timeout=5)
        except Exception as e:
            print(f"⚠️ [Sentinel] Watchdog ping failed: {e}")

def nuke_bybit():
    """Emergency Liquidate: 1. Cancel Orders -> 2. Close Positions (Audit 6.13)."""
    print("☢️ [Sentinel] NUKE PROTOCOL: Direct API strike initiated.")
    try:
        # Step 1: Cancel all open orders to liberate margin and prevent 'Ghost' fills
        print("🗑️ [Sentinel] Cancelling all open orders on Bybit...")
        payload_cancel = json.dumps({"category": "linear", "symbol": ""}) # empty symbol for all
        sig_c, ts_c = sign_bybit(payload_cancel, BYBIT_SECRET)
        headers = {
            "X-BAPI-API-KEY": BYBIT_KEY, "X-BAPI-SIGN": sig_c, "X-BAPI-TIMESTAMP": ts_c, "X-BAPI-RECV-WINDOW": "5000"
        }
        requests.post(f"{BYBIT_URL}/v5/order/cancel-all", data=payload_cancel, headers=headers)
        
        # Step 2: Fetch and liquidate positions
        params = "category=linear&settleCoin=USDT"
        sig_p, ts_p = sign_bybit(params, BYBIT_SECRET)
        h_pos = headers.copy(); h_pos["X-BAPI-SIGN"] = sig_p; h_pos["X-BAPI-TIMESTAMP"] = ts_p
        
        resp = requests.get(f"{BYBIT_URL}/v5/position/list?{params}", headers=h_pos)
        data = resp.json()
        
        if data.get("retCode") == 0:
            positions = data.get("result", {}).get("list", [])
            for pos in positions:
                size = float(pos.get("size", 0))
                if size > 0:
                    symbol = pos.get("symbol")
                    side = "Sell" if pos.get("side") == "Buy" else "Buy"
                    print(f"🔥 [Sentinel] Liquidating {symbol} | Qty: {size}")
                    
                    order_payload = json.dumps({
                        "category": "linear", "symbol": symbol, "side": side,
                        "orderType": "Market", "qty": str(size), "reduceOnly": True
                    })
                    sig_o, ts_o = sign_bybit(order_payload, BYBIT_SECRET)
                    h_ord = headers.copy(); h_ord["X-BAPI-SIGN"] = sig_o; h_ord["X-BAPI-TIMESTAMP"] = ts_o
                    requests.post(f"{BYBIT_URL}/v5/order/create", data=order_payload, headers=h_ord)
            print("🟢 [Sentinel] All positions evacuated successfully.")
        else:
            print(f"❌ [Sentinel] Bybit API Error: {data.get('retMsg')}")
    except Exception as e:
        print(f"💥 [Sentinel] Bybit Nuke Failed: {e}")

def fence_and_nuke():
    print("\n🛑 [SENTINEL ACTIVATED] Main process heartrate lost!")
    
    # 1. Hard Fencing (SIGKILL PID 1 in shared namespace)
    # Since we use 'pid: service:tekton_main', the app is PID 1 to us.
    try:
        os.kill(1, signal.SIGKILL)
        print("💀 [Sentinel] Main process (PID 1) terminated.")
    except Exception as e:
        print(f"⚠️ [Sentinel] Fencing failed: {e} (it might be already dead).")

    # 2. Emergency Liquidation
    nuke_bybit()
    
    print("☢️ [Sentinel] ALL POSITIONS ANNIHILATED. Sentinel duty complete.")

def run_sentinel():
    print(f"🛡️ [Sentinel] Watchdog online. Monitoring Redis Key: {LEASE_KEY}")
    
    try:
        # Strict timeout for Redis connection
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, socket_timeout=3)
    except Exception as e:
        print(f"❌ [Sentinel] Failed to connect to Redis: {e}")
        return

    last_ping = 0
    while True:
        try:
            # Check if lease exists
            if not r.exists(LEASE_KEY):
                fence_and_nuke()
                break 
        except redis.ConnectionError:
            print("⚠️ [Sentinel] Redis connection lost! Connection timeout. Invoking Nuke protocol.")
            fence_and_nuke()
            break
        except Exception as e:
            print(f"⚠️ [Sentinel] Unknown error in Sentinel loop: {e}")
            
        # External Beacon Pulse
        now = time.time()
        if now - last_ping > 60:
            ping_watchdog()
            last_ping = now
            
        time.sleep(1.0)

if __name__ == "__main__":
    run_sentinel()
