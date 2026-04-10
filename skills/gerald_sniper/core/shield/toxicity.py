# core/shield/toxicity.py

import asyncio
import logging
import time
from typing import Protocol, Dict, List
from loguru import logger

# [ARJANCODES] Strict Interface for Dependency Injection
class MarketStateInterface(Protocol):
    def get_mid_price(self, symbol: str) -> float: ...

class ToxicityMonitor:
    """
    [GEKTOR v8.4] Adverse Selection Guard (Post-Trade Markout).
    Measures if execution is being "milked" by toxic HFT flow.
    """
    def __init__(self, task_registry, redis_client, market_state: MarketStateInterface):
        self.registry = task_registry
        self.redis = redis_client
        self.market_state = market_state
        
        self.QUARANTINE_TTL_SEC = 14400  # 4 Hours
        self.TOXIC_THRESHOLD_PCT = -0.30 
        self.HISTORY_DEPTH = 3

    def track_execution(self, symbol: str, side: str, exec_price: float):
        """Called by OrderManager right after FILLED status confirmed."""
        self.registry.fire_and_forget(
            self._measure_markout_vector(symbol, side, exec_price),
            name=f"markout_{symbol}_{int(time.time())}"
        )

    async def _measure_markout_vector(self, symbol: str, side: str, exec_price: float):
        """
        [QUANT] Multi-Interval Markout (1s, 3s, 5s).
        Uses TWAP-like average to filter microsecond noise/spikes.
        """
        prices = []
        # Sample points: T+1, T+3, T+5
        for delay in [1.0, 2.0, 2.0]:
            await asyncio.sleep(delay)
            price = self.market_state.get_mid_price(symbol)
            if price > 0:
                prices.append(price)

        if not prices:
            return

        avg_mid_price = sum(prices) / len(prices)

        if side.upper() == "BUY":
            markout_pct = ((avg_mid_price - exec_price) / exec_price) * 100
        else:
            markout_pct = ((exec_price - avg_mid_price) / exec_price) * 100

        logger.debug(f"🔍 [MARKOUT] {symbol} {side} T+5s (TWAP): {markout_pct:.3f}%")
        await self._register_markout(symbol, markout_pct)

    async def _register_markout(self, symbol: str, markout_pct: float):
        """[KLEPPMANN] Idempotent history in Redis (survives crashes)."""
        hist_key = f"MARKOUT_HIST:{symbol}"
        
        try:
            # Atomic push + trim
            pipeline = self.redis.pipeline()
            pipeline.lpush(hist_key, str(markout_pct))
            pipeline.ltrim(hist_key, 0, self.HISTORY_DEPTH - 1)
            pipeline.expire(hist_key, 86400)
            await pipeline.execute()
            
            # Fetch history to check for toxicity
            raw_history = await self.redis.lrange(hist_key, 0, -1)
            if len(raw_history) >= self.HISTORY_DEPTH:
                avg_markout = sum(float(x) for x in raw_history) / len(raw_history)
                
                if avg_markout < self.TOXIC_THRESHOLD_PCT:
                    await self._trigger_quarantine(symbol, avg_markout)
                    await self.redis.delete(hist_key) # clear history after ban
        except Exception as e:
            logger.error(f"❌ [TOXICITY] Redis history update failed: {e}")

    async def _trigger_quarantine(self, symbol: str, avg_markout: float):
        """Hardware quarantine lock for toxic symbols."""
        lock_key = f"QUARANTINE_LOCK:{symbol}"
        try:
            await self.redis.set(lock_key, "TOXIC_FLOW", ex=self.QUARANTINE_TTL_SEC)
            
            logger.critical(
                f"☣️ [TOXICITY ALERT] MARKET MAKER ON {symbol} "
                f"IS FISHING THE RADAR (Avg Markout: {avg_markout:.2f}%). "
                f"TICKER QUARANTINED FOR 4 HOURS."
            )
        except Exception as e:
            logger.error(f"❌ [TOXICITY] Failed to set quarantine lock: {e}")
