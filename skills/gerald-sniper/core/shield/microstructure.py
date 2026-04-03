from loguru import logger
import time

class MicrostructureWatchdog:
    """
    Spoofing Protection for Bailout Logic (Audit 15.7).
    Uses EWMA (Exponential Weighted Moving Average) to filter out flashing limit orders.
    """
    def __init__(self, decay_factor: float = 0.2):
        self.decay_factor = decay_factor # Sensitivity: 0.2 = ~5 tick smoothing
        self.ewma_imbalance: float = 0.5 # Neutral state
        self.last_update_ts: float = 0.0

    def update_orderbook(self, best_bid_vol: float, best_ask_vol: float):
        """
        Fast $O(1)$ float calculation for real-time imbalance.
        Called on orderbook update (level 1).
        """
        total = best_bid_vol + best_ask_vol
        if total <= 0: return

        # Raw Imbalance (0.0=Ask only, 1.0=Bid only)
        raw = best_bid_vol / total

        # EWMA Filter
        self.ewma_imbalance = (raw * self.decay_factor) + (self.ewma_imbalance * (1.0 - self.decay_factor))
        self.last_update_ts = time.perf_counter()

    def is_bailout_required(self, is_long: bool) -> bool:
        """
        Signals an emergency exit if persistent pressure is detected.
        """
        # Thresholds (Audit 15.7): 0.15 = 85% Sell pressure, 0.85 = 85% Buy pressure
        if is_long and self.ewma_imbalance < 0.15:
             logger.warning(f"🚨 [SPOOF_GUARD] Persistent SELL pressure (EWMA: {self.ewma_imbalance:.2f}). Triggering Bailout.")
             return True
        
        if not is_long and self.ewma_imbalance > 0.85:
             logger.warning(f"🚨 [SPOOF_GUARD] Persistent BUY pressure (EWMA: {self.ewma_imbalance:.2f}). Triggering Bailout.")
             return True

        return False
