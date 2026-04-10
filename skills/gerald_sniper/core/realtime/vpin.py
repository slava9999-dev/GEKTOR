# core/realtime/vpin.py
import asyncio
import time
from dataclasses import dataclass, field
from typing import List, Optional
from loguru import logger

@dataclass(slots=True)
class RawTick:
    timestamp: float
    price: float
    volume: float

@dataclass(slots=True)
class VolumeBucket:
    target_volume: float
    current_volume: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    
    @property
    def is_full(self) -> bool:
        return self.current_volume >= self.target_volume
    
    @property
    def imbalance(self) -> float:
        return abs(self.buy_volume - self.sell_volume)

class MacroVPINTracker:
    """
    [GEKTOR v21.5] Institutional Accumulation Monitor (VPIN).
    Designed for medium-term anomaly detection by analyzing volume-synchronized toxicity.
    """
    def __init__(self, bucket_volume: float, window_size: int = 50):
        self.bucket_volume = bucket_volume
        self.window_size = window_size
        
        self.current_bucket = VolumeBucket(target_volume=bucket_volume)
        self.imbalance_history: List[float] = []
        
        self.last_price: float = 0.0
        self.last_tick_direction: int = 1 # 1 for Buy, -1 for Sell
        
        self.stats = {
            "buckets_processed": 0,
            "last_vpin": 0.0,
            "max_vpin": 0.0
        }

    def ingest_tick(self, tick: RawTick) -> Optional[float]:
        """
        Absorbs ticks without blocking the Event Loop.
        Classifies volume by Tick Rule for VPIN calculation.
        """
        if self.last_price > 0:
            if tick.price > self.last_price:
                self.last_tick_direction = 1
            elif tick.price < self.last_price:
                self.last_tick_direction = -1
            # If tick.price == last_price, we use the previous direction (Standard Tick Rule)

        # Distribute volume
        if self.last_tick_direction == 1:
            self.current_bucket.buy_volume += tick.volume
        else:
            self.current_bucket.sell_volume += tick.volume
            
        self.current_bucket.current_volume += tick.volume
        self.last_price = tick.price

        if self.current_bucket.is_full:
            return self._process_full_bucket()
            
        return None

    def _process_full_bucket(self) -> Optional[float]:
        """Calculates VPIN upon bucket completion and performs atomic state hand-off."""
        self.imbalance_history.append(self.current_bucket.imbalance)
        self.stats["buckets_processed"] += 1
        
        # Memory Window Shift (Klepman Rule: Limit state size)
        if len(self.imbalance_history) > self.window_size:
            self.imbalance_history.pop(0)
            
        # Conflation: Carry over volume overflow for numerical stability
        overflow_vol = self.current_bucket.current_volume - self.bucket_volume
        self.current_bucket = VolumeBucket(target_volume=self.bucket_volume)
        
        # Assign overflow based on the last tick direction (to prevent leakage)
        if self.last_tick_direction == 1:
            self.current_bucket.buy_volume = overflow_vol
        else:
            self.current_bucket.sell_volume = overflow_vol
        self.current_bucket.current_volume = overflow_vol
        
        # Wait for burn-in before emitting signal
        if len(self.imbalance_history) < self.window_size:
            return None
            
        total_imbalance = sum(self.imbalance_history)
        vpin = total_imbalance / (self.window_size * self.bucket_volume)
        
        # [GEKTOR v21.5] Update internal stats for monitoring
        self.stats["last_vpin"] = vpin
        self.stats["max_vpin"] = max(self.stats["max_vpin"], vpin)
        
        logger.debug(f"[VPIN] Anomaly Scanned: {vpin:.4f} (Buckets: {self.stats['buckets_processed']})")
        return vpin

    def get_state(self) -> dict:
        """Klepman-Ready State Persistence."""
        return {
            "imbalance_history": self.imbalance_history,
            "last_price": self.last_price,
            "last_direction": self.last_tick_direction,
            "stats": self.stats,
            "current_bucket": {
                "buy": self.current_bucket.buy_volume,
                "sell": self.current_bucket.sell_volume,
                "curr": self.current_bucket.current_volume
            }
        }

    def set_state(self, state: dict):
        """Atomic State Restoration."""
        self.imbalance_history = state.get("imbalance_history", [])
        self.last_price = state.get("last_price", 0.0)
        self.last_tick_direction = state.get("last_direction", 1)
        self.stats = state.get("stats", self.stats)
        
        cb = state.get("current_bucket", {})
        self.current_bucket.buy_volume = cb.get("buy", 0.0)
        self.current_bucket.sell_volume = cb.get("sell", 0.0)
        self.current_bucket.current_volume = cb.get("curr", 0.0)
