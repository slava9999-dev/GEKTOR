import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Optional
from decimal import Decimal
from loguru import logger

@dataclass
class CompressionResult:
    detected: bool
    range_shrink_pct: float
    avg_range_early: float
    avg_range_late: float
    direction: str # 'toward_resistance' | 'toward_support' | 'away'

@dataclass
class StructureEvent:
    type: str # compression|break|retest|sweep
    level_price: float
    timestamp: Optional[float] = None

@dataclass
class TrendlineResult:
    detected: bool
    side: str # 'UP' | 'DOWN'
    slope: float
    touches: int
    r_squared: float

class StructureDetector:
    """
    Detects price action patterns like compression, sweep, and retest.
    """
    
    @staticmethod
    def detect_compression(candles: List[dict], level_price: float, side: str) -> CompressionResult:
        """
        Detects range contraction near a level.
        """
        if len(candles) < 10:
            return CompressionResult(False, 0, 0, 0, 'unknown')
            
        n = len(candles)
        half = n // 2
        
        ranges = [float(c['high']) - float(c['low']) for c in candles]
        early_ranges = ranges[:half]
        late_ranges = ranges[half:]
        
        avg_early = np.mean(early_ranges) if early_ranges else 0
        avg_late = np.mean(late_ranges) if late_ranges else 0
        
        shrink = (avg_early - avg_late) / avg_early if avg_early > 0 else 0
        
        # Direction check
        closes = [float(c['close']) for c in candles]
        if side == 'RESISTANCE':
            # Upward drift toward level
            is_toward = closes[-1] > closes[0]
            direction = 'toward_resistance' if is_toward else 'away'
        else:
            # Downward drift toward level
            is_toward = closes[-1] < closes[0]
            direction = 'toward_support' if is_toward else 'away'
            
        # Digash Scenario 4 logic: Range shrinking + price staying near level
        detected = (shrink > 0.30) and is_toward
        
        return CompressionResult(
            detected=detected,
            range_shrink_pct=float(shrink * 100),
            avg_range_early=float(avg_early),
            avg_range_late=float(avg_late),
            direction=direction
        )

    @staticmethod
    def detect_trendline(candles: List[dict], side: str = "SUPPORT") -> TrendlineResult:
        """
        Simple linear regression on local lows (SUPPORT) or highs (RESISTANCE).
        """
        if len(candles) < 10:
            return TrendlineResult(False, 'none', 0, 0, 0)
            
        prices = [float(c['low'] if side == "SUPPORT" else c['high']) for c in candles]
        x = np.arange(len(prices))
        
        # We only take significant points or just use all for simple line
        z = np.polyfit(x, prices, 1)
        p = np.poly1d(z)
        
        # Calculate R-squared for "straightness"
        y_hat = p(x)
        y_bar = np.mean(prices)
        ss_res = np.sum((prices - y_hat)**2)
        ss_tot = np.sum((prices - y_bar)**2)
        r_sq = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        slope = z[0]
        detected = (r_sq > 0.6) and (slope > 0 if side == "SUPPORT" else slope < 0)
        
        return TrendlineResult(
            detected=detected,
            side='UP' if slope > 0 else 'DOWN',
            slope=float(slope),
            touches=3, # Mock for now
            r_squared=float(r_sq)
        )

    @staticmethod
    def detect_break(candles: List[dict], level: float, side: str, tolerance_pct: float = 0.1) -> bool:
        """
        Digash Scenario 2: Момент слома (breakout).
        """
        if len(candles) < 2: return False
        prev_close = float(candles[-2]['close'])
        curr_close = float(candles[-1]['close'])
        
        if side == 'RESISTANCE':
            # Was below, now above
            return prev_close < level and curr_close > level * (1 + tolerance_pct/100)
        else:
            # Was above, now below
            return prev_close > level and curr_close < level * (1 - tolerance_pct/100)

    @staticmethod
    def detect_retest(candles: List[dict], level: float, side: str, tolerance_pct: float = 0.3) -> bool:
        """
        Digash Scenario 3: Ретест слома.
        """
        if len(candles) < 3: return False
        
        # Simple check: last low/high touched level after being on the other side
        last_low = float(candles[-1]['low'])
        last_high = float(candles[-1]['high'])
        last_close = float(candles[-1]['close'])
        
        dist_pct = abs(last_close - level) / level * 100
        
        if side == 'RESISTANCE':
            # Long retest: broke up, now testing as support
            # We want last candle to stay ABOVE level but touch/near it
            return last_low <= level * (1 + tolerance_pct/100) and last_close > level and dist_pct < 0.5
        else:
            # Short retest: broke down, now testing as resistance
            return last_high >= level * (1 - tolerance_pct/100) and last_close < level and dist_pct < 0.5

    def determine_scenario(self, candles: List[dict], level_price: float, side: str) -> int:
        """
        Maps current price action to Digash Scenario (1-4).
        """
        if not candles: return 1 # Default approach
        
        if self.detect_break(candles, level_price, side):
            return 2
        if self.detect_retest(candles, level_price, side):
            return 3
        if self.detect_compression(candles, level_price, side).detected:
            return 4
            
        return 1 # Approach / Trendline

    @staticmethod
    def detect_sweep(candles: List[dict], level_price: float, side: str, tolerance_pct: float = 0.2) -> bool:
        """
        Detects a liquidity sweep (wick beyond level, close inside).
        """
        if not candles: return False
        last = candles[-1]
        tol = level_price * (tolerance_pct / 100.0)
        
        high = float(last['high'])
        low = float(last['low'])
        close = float(last['close'])
        
        if side == 'RESISTANCE':
            return high > (level_price + tol) and close < level_price
        else:
            return low < (level_price - tol) and close > level_price

# Global Instance
structure_detector = StructureDetector()
