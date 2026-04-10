import os
import sys
import time
import hmac
import hashlib
import httpx
from dotenv import load_dotenv

# Absolute Kill Switch (Audit 15.10)
# This script is standalone and bypasses the entire bot architecture.
# Use it ONLY if the main process is unresponsive (Zombified).

def generate_signature(api_secret: str, param_str: str) -> str:
    return hmac.new(api_secret.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()

def execute_panic_flatten():
    # Load credentials directly from .env (no DI, no config objects)
    load_dotenv()
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    base_url = os.getenv("BYBIT_REST_URL", "https://api.bybit.com")

    if not api_key or not api_secret:
        print("❌ [PANIC] Missing API_KEY or API_SECRET in .env!")
        return

    print(f"🚨 [PANIC BUTTON] INITIATING EMERGENCY FLATTEN ON {base_url}...")
    
    with httpx.Client(base_url=base_url, timeout=10.0) as client:
        # 1. Sync Time
        try:
            timestamp = str(int(time.time() * 1000))
            recv_window = "10000"

            # 2. Fetch all open positions (Linear/USDT)
            query = "category=linear&settleCoin=USDT"
            # In Bybit V5 GET requests, signature is timestamp + api_key + recv_window + query
            sign_str = timestamp + api_key + recv_window + query
            headers = {
                "X-BAPI-API-KEY": api_key,
                "X-BAPI-SIGN": generate_signature(api_secret, sign_str),
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": recv_window
            }
            
            resp = client.get(f"/v5/position/list?{query}", headers=headers).json()
            if resp.get('retCode') != 0:
                print(f"❌ [PANIC] Failed to fetch positions: {resp.get('retMsg')} (Code {resp.get('retCode')})")
                return

            positions = [p for p in resp.get('result', {}).get('list', []) if float(p.get('size', 0)) > 0]
            
            if not positions:
                print("✅ [PANIC] No open positions found. Exchange is clear.")
                return

            print(f"🔥 Found {len(positions)} active positions. Killing them now...")

            # 3. Flatten each position with Market ReduceOnly orders
            for pos in positions:
                symbol = pos['symbol']
                size = pos['size']
                side = pos['side'] # 'Buy' or 'Sell'
                exit_side = "Sell" if side == "Buy" else "Buy"
                
                print(f"🧨 Killing {symbol} | {side} | {size}...")

                # Construct POST payload
                payload_dict = {
                    "category": "linear",
                    "symbol": symbol,
                    "side": exit_side,
                    "orderType": "Market",
                    "qty": size,
                    "reduceOnly": True,
                    "timeInForce": "IOC",
                    "positionIdx": 0
                }
                
                # Bybit POST sign: timestamp + api_key + recv_window + payload_json
                import json
                payload_json = json.dumps(payload_dict)
                sign_str_post = timestamp + api_key + recv_window + payload_json
                
                post_headers = headers.copy()
                post_headers.update({
                    "X-BAPI-SIGN": generate_signature(api_secret, sign_str_post),
                    "Content-Type": "application/json"
                })
                
                res = client.post("/v5/order/create", headers=post_headers, content=payload_json).json()
                print(f"   ∟ Status for {symbol}: {res.get('retMsg')} (Code {res.get('retCode')})")

        except Exception as e:
            print(f"❌ [PANIC] Fatal crash in panic logic: {e}")

    print("🛑 [PANIC BUTTON] Operation completed. KILL THE MAIN PROCESS (Ctrl+C).")

if __name__ == "__main__":
    print("-" * 50)
    print("WARNING: THIS SCRIPT WILL MARKET-CLOSE ALL LIVE POSITIONS ON BYBIT.")
    print("-" * 50)
    confirm = input("Confirm emergency flatten? (Y/N): ")
    if confirm.lower() == 'y':
        execute_panic_flatten()
    else:
        print("Cancelled.")
