# core/realtime/temporal_integrity.py
import time
import logging
from loguru import logger

class ClockSyncManager:
    """
    [GEKTOR v21.7] High-Fidelity Clock Alignment.
    Estimates and smooths the offset between Local Clock and Exchange Clock
    to ensure VWAS integrity despite NTP drift or Jitter.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ClockSyncManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, alpha: float = 0.05):
        if hasattr(self, '_initialized'): return
        self.alpha = alpha
        self.offset_ema_ms: float = 0.0
        self._initialized = True

    def update(self, exchange_ts_ms: int, local_receive_ts_ms: int):
        """
        Adapts to clock drift. 
        Note: We assume a baseline latency (e.g. 50ms) is part of offset 
        to keep staleness calculation conservative (non-negative).
        """
        raw_offset = local_receive_ts_ms - exchange_ts_ms
        
        if self.offset_ema_ms == 0.0:
            self.offset_ema_ms = raw_offset
        else:
            self.offset_ema_ms = (self.alpha * raw_offset) + (1 - self.alpha) * self.offset_ema_ms

    def get_corrected_staleness(self, exchange_ts_ms: int) -> float:
        """Staleness = (Now_local_ms - Exchange_ms) - Corrected_Drift"""
        current_ms = time.time() * 1000
        # Corrected staleness filters out the constant clock drift component
        staleness = current_ms - exchange_ts_ms - self.offset_ema_ms
        return max(0.0, staleness)

# Singleton
clock_sync = ClockSyncManager()
