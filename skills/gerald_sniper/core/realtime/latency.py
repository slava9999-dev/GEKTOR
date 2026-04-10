import time
import logging
from typing import Optional
from abc import ABC, abstractmethod
from loguru import logger

class ILatencyMonitor(ABC):
    @abstractmethod
    def is_stale(self, exchange_ts_ms: int, local_ts_ms: float) -> bool:
        pass

class DriftAwareLatencyMonitor(ILatencyMonitor):
    """
    [GEKTOR v10.1] Advanced Latency Tracking.
    Resilient to NTP Clock Drift and BGP Path Baseline changes.
    Tracks 'Baseline Delta' using EMA to detect sudden spikes (Network Lag).
    """
    def __init__(self, spike_threshold_ms: int = 500, alpha: float = 0.05):
        self._threshold = spike_threshold_ms
        self._alpha = alpha  # Faster adaptation to absorb drift without re-anchoring
        self._baseline_delta: Optional[float] = None
        
        # Diagnostics
        self.total_packets = 0
        self.stale_drops = 0
        from utils.math_utils import log_throttler
        self._throttler = log_throttler

    def is_stale(self, exchange_ts_ms: int, local_ts_ms: float) -> bool:
        self.total_packets += 1
        current_delta = local_ts_ms - exchange_ts_ms

        if self._baseline_delta is None:
            self._baseline_delta = current_delta
            logger.info(f"🛰️ [LatencyGuard] Initialized baseline delta: {self._baseline_delta:.2f}ms")
            return False

        latency_spike = current_delta - self._baseline_delta

        # 1. Massive Drift Re-anchoring (e.g. NTP Jump or OS freeze)
        if abs(latency_spike) > 1000:
            if self._throttler.should_log("latency_reanchor", 60):
                logger.warning(f"🕒 [LatencyGuard] Significant drift ({latency_spike:.2f}ms). Re-anchoring baseline.")
            self._baseline_delta = current_delta
            return False

        # 2. Smooth Adaptation (EMA)
        self._baseline_delta = (self._alpha * current_delta) + ((1 - self._alpha) * self._baseline_delta)

        # 3. Filtering
        if latency_spike > self._threshold:
            self.stale_drops += 1
            if self._throttler.should_log("stale_drop", 15):
                logger.warning(
                    f"🗑️ [StaleDrop] Packet delayed by {latency_spike:.2f}ms "
                    f"(Threshold: {self._threshold}ms). Baseline: {self._baseline_delta:.2f}ms"
                )
            return True

        return False

# Factory for monitoring
def get_latency_monitor(threshold_ms: int = 500) -> ILatencyMonitor:
    return DriftAwareLatencyMonitor(spike_threshold_ms=threshold_ms)
