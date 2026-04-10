# core/radar/liquidity.py
import time
from typing import List, Tuple, Dict
from loguru import logger
from dataclasses import dataclass

@dataclass(slots=True)
class L2Snapshot:
    symbol: str
    bids: List[Tuple[float, float]]  # [(price, volume), ...]
    asks: List[Tuple[float, float]]
    last_update_ts: float

class LiquidityEngine:
    """
    [GEKTOR v21.14] Microstructural Liquidity Guard.
    Calculates institutional-grade exit prices using VWAP 'Walk the Book' depth analysis.
    Implements L2FreshnessGuard to detect stale state during API throttling.
    """
    def __init__(self, stale_threshold_ms: int = 500):
        self.stale_threshold = stale_threshold_ms / 1000.0

    def calculate_capitulation_floor(self, snapshot: L2Snapshot, position_size: float, is_long: bool = True) -> Dict:
        """
        Walks the book to find the VWAP exit price for the entire position.
        """
        # 1. Freshness Check
        latency = time.time() - snapshot.last_update_ts
        is_stale = latency > self.stale_threshold
        
        if is_stale:
            logger.warning(f"🍱 [Liquidity] STALE DATA DETECTED ({latency*1000:.0f}ms). Execution confidence: LOW.")

        # 2. Walk the Book (L2 depth analysis)
        book = snapshot.bids if is_long else snapshot.asks
        accumulated_vol = 0.0
        weighted_price_sum = 0.0
        target_vol = position_size * 1.5 # 150% depth coverage for safety
        
        floor_price = 0.0
        for price, vol in book:
            needed = target_vol - accumulated_vol
            take = min(vol, needed)
            
            weighted_price_sum += (take * price)
            accumulated_vol += take
            floor_price = price
            
            if accumulated_vol >= target_vol:
                break

        # 3. Aggregate results
        vwap_exit = weighted_price_sum / accumulated_vol if accumulated_vol > 0 else 0.0
        
        # [GEKTOR v21.14] Confidence Scoring
        confidence = "HIGH" if not is_stale and accumulated_vol >= target_vol else "LOW"

        return {
            "exit_price": round(floor_price, 6),
            "vwap_exit": round(vwap_exit, 6),
            "is_stale": is_stale,
            "data_age_ms": int(latency * 1000),
            "confidence": confidence
        }
