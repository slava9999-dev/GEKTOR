import numpy as np
from loguru import logger
import time
import statistics
from collections import deque

class OutlierFilter:
    """
    [Gektor v2.11.1] Adaptive Outlier Filter.
    Categories assets into MAJOR, ALT, and MEME to avoid blocking real market moves.
    Includes Auto-Bypass logic for SIREN-level volatility.
    """
    def __init__(self, max_deviation_pct: float = 5.0):
        # Пороги отклонения для разных классов активов
        self.thresholds = {
            "MAJOR": 0.05,    # BTC, ETH (строго 5%)
            "ALT": 0.15,      # SOL, ADA, и т.д. (15%)
            "MEME": 0.60,     # SIREN, MAGMA (даем дышать до 60%)
            "DEFAULT": 0.20   # Все остальное (20%)
        }
        self.price_window = deque(maxlen=20)
        self.consecutive_triggers = 0
        self.bypass_mode = False
        self.bypass_until = 0.0

    def _classify_asset(self, symbol: str) -> str:
        s = symbol.upper().replace("USDT", "")
        if s in ["BTC", "ETH"]: return "MAJOR"
        if s in ["SOL", "XRP", "LTC", "LINK"]: return "ALT"
        return "MEME"

    def is_price_sane(self, symbol: str, price: float, volatility: float = 0.0) -> bool:
        """Sanity check with Adaptive Thresholds and Failsafe Bypass."""
        now = time.time()
        
        if self.bypass_mode:
            if now < self.bypass_until:
                self.price_window.append(price)
                return True
            else:
                self.bypass_mode = False
                self.consecutive_triggers = 0
                logger.info(f"🔄 [OUTLIER] Failsafe expired for {symbol}. Returning to filtered mode.")

        if not self.price_window:
            self.price_window.append(price)
            return True

        # Use median for stability
        ref_price = statistics.median(self.price_window) if len(self.price_window) >= 5 else self.price_window[-1]
        
        category = self._classify_asset(symbol)
        limit = self.thresholds.get(category, self.thresholds["DEFAULT"])
        
        # Expand limit if high volatility or high-vol asset
        if volatility > 0.5:
            limit = max(limit, self.thresholds["MEME"])
            
        deviation = abs(price - ref_price) / ref_price if ref_price > 0 else 0
        
        if deviation > limit:
            self.consecutive_triggers += 1
            logger.critical(f"🚨 [Outlier] {symbol} deviation {deviation:.2%} > limit {limit:.2%}. Count: {self.consecutive_triggers}")
            
            if self.consecutive_triggers >= 50:
                self.bypass_mode = True
                self.bypass_until = now + 300
                logger.warning(f"🛡️ [Outlier] SIREN MODE ACTIVE for {symbol} (5 mins).")
            return False

        self.consecutive_triggers = 0
        self.price_window.append(price)
        return True
