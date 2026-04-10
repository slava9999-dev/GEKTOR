# core/radar/integrity.py
import logging
from loguru import logger

class PriceIntegrityGuard:
    """
    [GEKTOR v21.8] Slip-Protection Guard.
    Ensures that the current market price hasn't drifted too far from 
    the bar close during high-latency events.
    """
    def __init__(self, max_allowed_deviation_pct: float = 0.002): # 0.2% default
        self.max_deviation = max_allowed_deviation_pct

    def is_price_still_valid(self, bar_price: float, current_market_price: float) -> bool:
        """
        Validates if the signal is entry-feasible despite network lag.
        """
        if not current_market_price or not bar_price:
            return False
            
        deviation = abs(current_market_price - bar_price) / bar_price
        if deviation > self.max_deviation:
            logger.warning(f"📉 [Integrity] PRICE_DEVIATION too high: {deviation:.3%} (Max: {self.max_deviation:.1%}). Signal invalidated.")
            return False
        return True

class PerformanceCircuitBreaker:
    """
    [GEKTOR v21.8] Self-Preservation Switch.
    Detects Event Loop blocking or CPU starvation by measuring internal processing lag.
    """
    def __init__(self, critical_lag_ms: float = 200.0):
        self.critical_lag = critical_lag_ms
        self.is_broken = False

    def check_internal_lag(self, processing_lag_ms: float) -> bool:
        """
        Monitors health. If processing exceeds threshold, system halts to prevent toxic execution.
        """
        if processing_lag_ms > self.critical_lag:
            logger.critical(f"🚨 [Integrity] INTERNAL_SYSTEM_LAG: {processing_lag_ms:.2f}ms. CIRCUIT_BREAKER TRIPPED!")
            self.is_broken = True
            # In a real environment, this would trigger a cleanup or safe shutdown
            return False
        return not self.is_broken
