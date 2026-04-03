import os
import json
import asyncio
import hmac
import hashlib
import time
import httpx
from redis.asyncio import Redis
from loguru import logger
from dotenv import load_dotenv

# THE OMEGA BRIDGE v5.22 (KILL SWITCH)
# Role: Cross-Exchange Emergency Shutdown on API Lockout.

load_dotenv()

# Configuration
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_URL = "https://fapi.binance.com"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6381/0")

STREAM_NAME = "tekton:hedge:orders"
DLO_STREAM = "tekton:hedge:dead_letters"
KILL_SWITCH_CHANNEL = "tekton_emergency_kill"
GROUP_NAME = "hedge_group"
CONSUMER_NAME = "exec_node"
MAX_RETRIES = 3

def sign(secret: str, query: str) -> str:
    return hmac.new(secret.encode('utf-8'), query.encode('utf-8'), hashlib.sha256).hexdigest()

class BinanceExecutor:
    def __init__(self):
        self.redis = Redis.from_url(REDIS_URL)
        self.client = httpx.AsyncClient(base_url=BINANCE_URL, timeout=5.0)
        self.current_epoch: int = 0 

    async def _setup_streams(self):
        try:
            await self.redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        except: pass
        
        stored_epoch = await self.redis.get("tekton:epoch:global_counter")
        self.current_epoch = int(stored_epoch or 0)
        logger.info(f"🧬 [PRODUCTION_START] Global Epoch: {self.current_epoch}")

    async def run(self):
        await self._setup_streams()
        logger.warning(f"🌉 [BinanceBridge] Cross-Exchange KillSwitch Active.")
        
        while True:
            try:
                global_curr_epoch_raw = await self.redis.get("tekton:epoch:global_counter")
                self.current_epoch = int(global_curr_epoch_raw or 0)
                
                response = await self.redis.xreadgroup(
                    GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=1, block=200
                )
                if not response: continue
                
                for stream, messages in response:
                    for msg_id, data in messages:
                        # Poison Pill Check
                        pending_info = await self.redis.xpending_range(
                            STREAM_NAME, GROUP_NAME, min=msg_id, max=msg_id, count=1
                        )
                        if pending_info and pending_info[0].get('times_delivered', 0) > MAX_RETRIES:
                             await self.redis.xadd(DLO_STREAM, {"id": msg_id, "data": str(data)})
                             await self.redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                             continue

                        payload = {k.decode(): v.decode() for k, v in data.items()}
                        
                        # 1. Check for Emergency Flatten signals from Main
                        if payload.get("action") == "CLOSE_ALL":
                             logger.critical("🔨 [FLATTEN] Urgent Close-All request received! Execution Start.")
                             await self._process_order({"side": "CLOSE_ALL_POSITION"})
                             await self.redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                             continue

                        msg_epoch = int(payload.get('epoch', 0))
                        if msg_epoch != self.current_epoch:
                            await self.redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue
                        
                        # 2. Process with Fatal Error Detection (Kill Switch)
                        try:
                            result = await self._process_order(payload)
                            if result == "FATAL":
                                 await self._trigger_global_kill_switch("API_RESTRICTED")
                                 return # Shutdown executor
                            elif result == "SUCCESS":
                                 await self.redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                        except Exception as e:
                            logger.error(f"⚠️ [EXEC_ERROR] {e}")
                        
            except Exception as e:
                logger.error(f"❌ [Loop] {e}")
                await asyncio.sleep(1.0)

    async def _trigger_global_kill_switch(self, reason: str):
        """Audit 16.30: Publish to emergency channel to halt Bybit Main node."""
        logger.critical(f"🛑 [KILL_SWITCH] Triggering Global Shutdown! Reason: {reason}")
        await self.redis.publish(KILL_SWITCH_CHANNEL, json.dumps({"action": "STOP_IMMEDIATELY", "reason": reason}))

    async def _process_order(self, order: dict) -> str:
        # Mock logic for API Restriction detection
        # In reality, check resp.json() for specific error codes like -2015 (Invalid API-key)
        # or -1002 (Account Restricted)
        symbol = order.get("symbol", "BTCUSDT")
        side = order.get("side", "SELL")
        qty = order.get("qty")
        
        timestamp = int(time.time() * 1000)
        query = f"symbol={symbol}&side={side}&type=MARKET&quantity={qty}&timestamp={timestamp}"
        signature = sign(BINANCE_API_SECRET, query)
        
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        url = f"/fapi/v1/order?{query}&signature={signature}"
        
        try:
            resp = await self.client.post(url, headers=headers)
            data = resp.json()
            
            # Audit 16.30: Detect Account Restriction
            if resp.status_code == 403 or data.get("code") in [-1002, -2015]:
                 logger.critical(f"⛔ [BINANCE_BANNED] Account {reason}: {data.get('msg')}")
                 return "FATAL"
            
            if resp.status_code == 200:
                logger.success(f"✅ [EPOCH_{self.current_epoch}] Hedge Filled.")
                return "SUCCESS"
            return "ERROR"
        except: return "ERROR"

if __name__ == "__main__":
    executor = BinanceExecutor()
    asyncio.run(executor.run())
