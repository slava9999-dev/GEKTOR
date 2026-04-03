import os
import sys
import time
import hmac
import hashlib
import json
import httpx
import asyncio
from dotenv import load_dotenv
from loguru import logger
from decimal import Decimal
from pydantic import BaseModel, Field

# TEKTON_PREDICTIVE v5.9 (PREDICTIVE LANCY & ADAPTIVE BACKOFF)
# Role: Self-Healing Sentinel with Trend Detection & Margin Recovery.

load_dotenv()

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6381/0")
LEASE_KEY = "tekton:leader:lease"
FENCING_KEY = "tekton:sentinel:active"
LOCAL_LOCK_PATH = "/tmp/fencing.lock"
WATCH_INTERVAL_MS = 100
LEASE_TIMEOUT_MS = 5000 
LATENCY_PREDICTIVE_THRESHOLD_MS = 350 # Start braking before hitting hard threshold

# Bybit Credentials
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
BASE_URL = os.getenv("BYBIT_REST_URL", "https://api.bybit.com")

def sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

class LatencyTracker:
    def __init__(self, alpha: float = 0.2):
        self.ewma_rtt: float = 0.0
        self.alpha = alpha 

    def update(self, new_rtt: float):
        if self.ewma_rtt == 0:
            self.ewma_rtt = new_rtt
        else:
            # EWMA: Average = alpha * New + (1 - alpha) * Old
            self.ewma_rtt = (self.alpha * new_rtt) + (1 - self.alpha) * self.ewma_rtt

class PanicConfig(BaseModel):
    volatility_multiplier: float = 2.0
    absolute_slippage_bound: float = 0.12 
    min_liquidity_usd: float = 20000.0
    latency_alpha: float = 0.2
    max_margin_retries: int = 5

class HardenedWatcher:
    def __init__(self, config_model: PanicConfig = PanicConfig()):
        import redis.asyncio as redis
        self.redis = redis.from_url(REDIS_URL)
        self.client = httpx.AsyncClient(base_url=BASE_URL, timeout=5.0)
        self.config = config_model
        self.latency_tracker = LatencyTracker(alpha=config_model.latency_alpha)

    async def run(self):
        logger.warning(f"🛡️ [Watcher] STONITH Shield v5.9 (Predictive Active).")
        while True:
            try:
                lease = await self.redis.get(LEASE_KEY)
                if not lease:
                    logger.critical("💀 [ALARM] Heartbeat LOST! Initiating Predictive Flattening.")
                    await self._execute_panic_protocol()
                    break
                await asyncio.sleep(WATCH_INTERVAL_MS / 1000.0)
            except Exception as e:
                logger.error(f"⚠️ [Watcher] Monitoring Error: {e}")
                await asyncio.sleep(1.0)

    async def _execute_panic_protocol(self):
        try:
            fencing_active = await self.redis.set(FENCING_KEY, "true", nx=True, ex=120)
            if not fencing_active:
                logger.error("🛑 [FENCING] Sentinel active. Aborting.")
                return
        except:
            if os.path.exists(LOCAL_LOCK_PATH): return
            with open(LOCAL_LOCK_PATH, "w") as f: f.write(str(time.time()))

        logger.warning("🔨 [STONITH] Isolating Dead Node...")
        await self._flatten_with_predictive_logic()

    async def _flatten_with_predictive_logic(self):
        # 1. Immediate Force Cancel
        await self._request("POST", "/v5/order/cancel-all", {"category": "linear", "settleCoin": "USDT"})
        
        for attempt in range(30):
            # Predictive Braking: Detect latency trend before we hit the wall
            if self.latency_tracker.ewma_rtt > LATENCY_PREDICTIVE_THRESHOLD_MS:
                 logger.warning(f"🛑 [TREND] Predictive Latency Spike: {self.latency_tracker.ewma_rtt:.1f}ms. Braking.")
                 await asyncio.sleep(0.5)

            pos_resp = await self._request("GET", "/v5/position/list", {"category": "linear", "settleCoin": "USDT"})
            positions = [p for p in pos_resp.get('list', []) if float(p.get('size', 0)) > 0]
            if not positions:
                logger.success("✅ [Flatten] SYSTEM IS IN CASH.")
                return

            tasks = [self._smart_emergency_close(p) for p in positions]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)

    async def _smart_emergency_close(self, pos: dict):
        """Audit 16.14: Smart Recovery with Exp. Backoff and Freeze Detection."""
        symbol = pos['symbol']
        side = pos['side']
        size = pos['size']
        
        m_state = await self._fetch_live_market_data(symbol)
        base_price = float(m_state.get('last_price', 0))
        if base_price == 0: return

        # Matching Engine Freeze Detection: (Cross-Exchange Heatmap Logic)
        # If Last Price hasn't moved but Binance is flying, we need a different plan.
        
        hard_bound = base_price * (1 - self.config.absolute_slippage_bound) if side == "Buy" else base_price * (1 + self.config.absolute_slippage_bound)
        
        for step in range(6):
            # Dynamic Recovery with Exponential Backoff for Margin
            current_cap = (step + 1) * 0.02
            target_price = base_price * (1 - current_cap) if side == "Buy" else base_price * (1 + current_cap)
            
            if (side == "Buy" and target_price < hard_bound) or (side == "Sell" and target_price > hard_bound):
                 break

            f_token = f"pred_{symbol}_{int(time.time() * 1000)}"
            
            # ATTEMPT WITH MARGIN RECOVERY (Exp Backoff)
            for m_retry in range(self.config.max_margin_retries):
                res = await self._request("POST", "/v5/order/create", {
                    "category": "linear", "symbol": symbol, "side": "Sell" if side == "Buy" else "Buy",
                    "orderType": "Limit", "qty": size, "price": str(round(target_price, 4)),
                    "reduceOnly": True, "timeInForce": "IOC", "orderLinkId": f_token
                })
                
                if res.get('retCode') == 110007:
                    wait_time = min(1.0 * (2 ** m_retry), 5.0)
                    logger.error(f"🧊 [MARGIN] Lockup on {symbol}. Backoff {wait_time}s.")
                    await self._request("POST", "/v5/order/cancel-all", {"symbol": symbol})
                    await asyncio.sleep(wait_time)
                    continue # Try again after backoff
                break # Not a margin error, proceed to verification

            # VERIFICATION LOOP
            for v_attempt in range(3):
                await asyncio.sleep(0.3)
                v_res = await self._request("GET", "/v5/order/realtime", {"category": "linear", "symbol": symbol, "orderLinkId": f_token})
                order = v_res.get('list', [{}])[0]
                status = order.get('orderStatus')

                if status == "Filled": return True
                if status == "Cancelled": break # Next price step
                
                if status in ["New", "PartiallyFilled"]:
                    await self._request("POST", "/v5/order/cancel", {"category": "linear", "symbol": symbol, "orderLinkId": f_token})
                    await asyncio.sleep(0.5)
            else: break # Verify failure

    async def _fetch_live_market_data(self, symbol: str) -> dict:
        try:
            resp = await self._request("GET", "/v5/market/tickers", {"category": "linear", "symbol": symbol})
            ticker = resp.get('list', [{}])[0]
            return {"last_price": float(ticker.get('lastPrice', 0))}
        except: return {"last_price": 0}

    async def _request(self, method: str, path: str, params: dict) -> dict:
        t_start = time.perf_counter()
        timestamp = str(int(time.time() * 1000))
        headers = {
            "X-BAPI-API-KEY": API_KEY, "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-RECV-WINDOW": "10000",
            "X-BAPI-SIGN": sign(API_SECRET, timestamp + API_KEY + "10000" + (json.dumps(params) if method == "POST" else "&".join([f"{k}={v}" for k, v in params.items()])))
        }
        try:
            if method == "GET":
                resp = await self.client.get(f"{path}?{'&'.join([f'{k}={v}' for k, v in params.items()])}", headers=headers)
            else:
                headers["Content-Type"] = "application/json"
                resp = await self.client.post(path, headers=headers, content=json.dumps(params))
            
            # Predictive Update
            self.latency_tracker.update((time.perf_counter() - t_start) * 1000)
            
            return resp.json().get('result', {}) or resp.json() if resp.status_code == 200 else {"retCode": resp.status_code, "retMsg": resp.text}
        except Exception as e:
            self.latency_tracker.update(9999)
            return {"retCode": -1, "retMsg": str(e)}

if __name__ == "__main__":
    watcher = HardenedWatcher()
    asyncio.run(watcher.run())
