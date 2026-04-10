# core/radar/beta_sentry.py
import asyncio
import time
import collections
import numpy as np
from loguru import logger

class GlobalBetaSentry:
    """
    [GEKTOR v21.9] Market-Wide Shock Detector.
    High-frequency monitor for the "Guide" (BTC-USDT).
    Detects liquidity waterfalls and systemic beta risk.
    """
    def __init__(self, shock_z: float = 3.5, window_ticks: int = 50):
        self.shock_z = shock_z
        self.window_ticks = window_ticks
        self.prices = collections.deque(maxlen=window_ticks)
        self.returns = collections.deque(maxlen=window_ticks)
        self.is_alarm_active: bool = False
        self._lock = asyncio.Lock()

    async def update(self, btc_price: float):
        """High-frequency tick update for the Guard asset (BTC)."""
        async with self._lock:
            if not self.prices:
                 self.prices.append(btc_price)
                 return

            last_price = self.prices[-1]
            ret = (btc_price - last_price) / last_price
            self.returns.append(ret)
            self.prices.append(btc_price)
            
            # Not enough data for Z-score
            if len(self.returns) < 10: return

            # Calculate Z-score of returns
            mean_ret = np.mean(self.returns)
            std_ret = np.std(self.returns) + 1e-9
            current_z = (ret - mean_ret) / std_ret
            
            # TRIGGER: Systemic Shock Detection
            if current_z < -self.shock_z:
                logger.critical(f"🌊 [BetaSentry] SYSTEMIC_SHOCK (BTC_CRASH). Z-score: {current_z:.2f}. Raising ALARM!")
                self.is_alarm_active = True
                await self._broadcast_shock("BTC_LIQUIDITY_WATERFALL", current_z)
            
            # Reset alarm after stabilization or a cooling period (simplified here)
            if self.is_alarm_active and abs(current_z) < 1.0:
                logger.warning(f"🩹 [BetaSentry] Alarm Reset. Market stabilization detected.")
                self.is_alarm_active = False

    async def _broadcast_shock(self, reason: str, intensity: float):
        """Triggers local mass-abort for all monitoring contexts."""
        # Broadcast internally via signal or bus
        pass

# Global Singleton
beta_sentry = GlobalBetaSentry()
