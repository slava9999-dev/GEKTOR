import time
import numpy as np
from loguru import logger

class ChronosEngine:
    """
    [GEKTOR v14.8.1] Monotonic Time Integrity Provider.
    Zero-Jitter & Bidirectional Temporal Protection.
    """
    def __init__(self, window_size: int = 10, amnesia_threshold_ms: float = 500, forward_threshold_ms: float = 5000):
        self._rtt_history = np.empty(window_size)
        self._rtt_history.fill(np.nan)
        self._rtt_idx = 0
        
        self._offset_ns = 0
        self._last_exchange_ts_ns = 0
        self._amnesia_threshold_ns = amnesia_threshold_ms * 1_000_000
        self._forward_threshold_ns = forward_threshold_ms * 1_000_000
        self.is_synchronized = False

    def sync(self, exchange_ts_ms: int, rtt_ms: float):
        """
        [HFT Quant] Offset synchronization with Bidirectional Temporal Protection.
        """
        current_mono_ns = time.monotonic_ns()
        exchange_ts_ns = exchange_ts_ms * 1_000_000
        
        if self._last_exchange_ts_ns > 0:
            # 1. Backward Jump Detection (Temporal Collapse)
            jump_back = self._last_exchange_ts_ns - exchange_ts_ns
            if jump_back > self._amnesia_threshold_ns:
                logger.critical(f"☢️ [TEMPORAL_COLLAPSE] Exchange clock jumped BACKWARD by {jump_back/1e6:.2f}ms!")
                return "TRIGGER_AMNESIA"

            # 2. Forward Jump Detection (Future Ghost)
            # If the exchange jumps forward > 5s, we must AMNESIA to avoid blocking 
            # execution reports still in flight with 'old' timestamps.
            jump_forward = exchange_ts_ns - self._last_exchange_ts_ns
            if jump_forward > self._forward_threshold_ns:
                logger.critical(f"👻 [FUTURE_GHOST] Exchange clock jumped FORWARD by {jump_forward/1e6:.2f}ms!")
                return "TRIGGER_AMNESIA"

        # 3. Jitter Filtering (Min-RTT)
        self._rtt_history[self._rtt_idx % len(self._rtt_history)] = rtt_ms
        self._rtt_idx += 1
        min_rtt_ns = int(np.nanmin(self._rtt_history) * 1_000_000)
        
        # 4. Offset Calculation (Smooth Reality)
        exchange_now_ns = exchange_ts_ns + (min_rtt_ns // 2)
        self._offset_ns = exchange_now_ns - current_mono_ns
        self._last_exchange_ts_ns = exchange_ts_ns
        self.is_synchronized = True
        
        return "OK"

    def effective_now_ms(self) -> int:
        """[David Beazley] Monotonic True Time in Exchange Logical Scale."""
        return (time.monotonic_ns() + self._offset_ns) // 1_000_000

# Global Instance
chronos = ChronosEngine()
