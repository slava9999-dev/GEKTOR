import asyncio
import time
from loguru import logger
from typing import Optional
from redis.asyncio import Redis

# THE OMEGA BRIDGE: Ultra-Fast Predictive Sentinel v5.11
# Role: Detection of Bybit Matching Engine Freeze (300ms trigger) via Redis Streams.

class CrossExchangeSentinel:
    def __init__(self, redis_url: str, threshold_ms: int = 300, price_diff_pct: float = 0.001):
        self.redis = Redis.from_url(redis_url)
        self.threshold_ms = threshold_ms
        self.price_diff_pct = price_diff_pct
        self.is_frozen = False
        
        # Micro-Structure State
        self.last_bybit_pulse: float = time.perf_counter()
        self.last_bybit_price: float = 0.0
        self.last_binance_price: float = 0.0

    async def run(self, symbol: str = "BTCUSDT"):
        logger.info(f"🧐 [Sentinel] Active. Correlation Threshold: {self.threshold_ms}ms (v5.11).")
        while True:
            try:
                # 1. Fetch prices and heartbeat from EventBus (Redis Keys)
                bybit_str = await self.redis.get(f"tekton:price:bybit:{symbol}")
                binan_str = await self.redis.get(f"tekton:price:binance:{symbol}")
                
                # Update pulse timestamp on Bybit price change or tick
                if bybit_str and float(bybit_str) != self.last_bybit_price:
                     self.last_bybit_pulse = time.perf_counter()
                     self.last_bybit_price = float(bybit_str)
                
                self.last_binance_price = float(binan_str) if binan_str else 0.0
                
                # 2. MATCHING ENGINE FREEZE DETECTION (v5.11 Ultra-Fast)
                if self.last_binance_price > 0 and self.last_bybit_price > 0:
                    delta_pct = abs(self.last_binance_price - self.last_bybit_price) / self.last_bybit_price
                    
                    # TRIGGER: If Binance moved more than 0.1% while Bybit is 'Stale' for 300ms
                    if delta_pct > self.price_diff_pct and (time.perf_counter() - self.last_bybit_pulse) > (self.threshold_ms / 1000.0):
                        await self._execute_stream_hedge(symbol)
                
                await asyncio.sleep(0.05) # 50ms check cycle for HFT
            except Exception as e:
                logger.error(f"⚠️ [Sentinel] Correlation Loop Error: {e}")
                await asyncio.sleep(1.0)

    async def _execute_stream_hedge(self, symbol: str):
        if self.is_frozen: return
        
        binance_rtt = float(await self.redis.get("tekton:metrics:binance:rtt") or 0)
        if binance_rtt > 1000:
             logger.critical("🚨 [GLOBAL BLACKOUT] Binance also failing. Aborting hedge.")
             return

        logger.critical(f"🧊 [FREEZE] Bybit stalled for {self.threshold_ms}ms. Adding to Redis Stream OMEGA.")
        self.is_frozen = True
        
        # [Audit 25.6] Fetch current SL from Distributed Bus to ensure symmetry
        sl_val = await self.redis.get(f"tekton:sl:{symbol}")
        
        # Transactional Publish via Redis Streams (Audit 16.16)
        payload = {
            "symbol": symbol,
            "side": "SELL", # Standard inverse for Long. In real: fetch from position-snapshot.
            "qty": "0.1",
            "stop_loss": sl_val.decode() if sl_val else "0",
            "ts": str(int(time.time() * 1000))
        }
        
        # Audit 16.16: XADD ensures order is persistent and can be ACK'ed
        await self.redis.xadd("tekton:hedge:orders", payload, maxlen=100)
        
        # Record event for StateReconciler (Audit 16.17)
        await self.redis.set(f"tekton:hedge:active:{symbol}", "true", ex=300)
