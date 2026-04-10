# skills/gerald-sniper/core/realtime/zero_alloc.py
import gc
import time
import numpy as np
from typing import List, Tuple, Optional
from loguru import logger

class ZeroAllocationTickBuffer:
    """
    [GEKTOR v21.19] Zero-Allocation Microstructure Ring Buffer.
    - Eliminates Python object overhead in the Hot Path.
    - Prevents GC (Generation 2) pauses by using contiguous C-arrays.
    - Structured for O(1) ingestion of L2 Tick Flow.
    """
    
    def __init__(self, capacity: int = 1_000_000):
        self.capacity = capacity
        self.cursor = 0
        self.is_wrapped = False
        
        # Pre-allocate contiguous memory (C-style structure)
        # 1M ticks * (8+4+4+1) bytes = ~17 MB of static RAM
        self._buffer = np.zeros(self.capacity, dtype=[
            ('ts', np.float64),
            ('price', np.float32),
            ('vol', np.float32),
            ('side', np.int8) # 1=Buy/Maker, 0=Sell/Taker
        ])
        
        logger.info(f"💾 [ZeroAlloc] Buffer initialized: {self._buffer.nbytes / (1024**2):.2f} MB static overhead.")

    def push(self, ts: float, price: float, vol: float, side: int):
        """Zero-allocation O(1) push. Directly writes to C-memory offset."""
        self._buffer[self.cursor] = (ts, price, vol, side)
        
        self.cursor += 1
        if self.cursor >= self.capacity:
            self.cursor = 0
            self.is_wrapped = True

    def get_view(self, lookback: int) -> np.ndarray:
        """
        Returns a zero-copy memory view of the N most recent ticks.
        Safe for vector math (VPIN / OrderFlow Imbalance).
        """
        if not self.is_wrapped and self.cursor < lookback:
            return self._buffer[:self.cursor]
            
        if self.cursor >= lookback:
            return self._buffer[self.cursor - lookback : self.cursor]
            
        # Concatenate view on wrap-around (minimal allocation)
        tail_len = lookback - self.cursor
        return np.concatenate((
            self._buffer[-tail_len:], 
            self._buffer[:self.cursor]
        ))

class GCSentry:
    """
    Manages Zero-Allocation sessions by controlling the Python Garbage Collector.
    - Disables cyclic GC during Hot sessions to prevent STW (Stop-The-World) pauses.
    - Triggers manual deep cleanup during market rollover/low-volatility periods.
    """
    
    @staticmethod
    def session_start():
        logger.warning("⛓️ [GC] Disabling automatic GC for HFT Session. Zero-Allocation active.")
        gc.disable()

    @staticmethod
    def manual_sweep():
        """Manual Clean-up (Phase 6 Low Volatility Window)."""
        logger.info("🧹 [GC] Triggering manual state sweep...")
        before = gc.get_count()
        collected = gc.collect()
        logger.info(f"🧹 [GC] Collected {collected} objects. Previous generation counts: {before}")

    @staticmethod
    def session_stop():
        gc.enable()
        logger.info("⛓️ [GC] Restoration complete. Cyclic collector back online.")

# Global Tick Storage for Ingestion Engine
tick_store = ZeroAllocationTickBuffer(capacity=2_000_000)
