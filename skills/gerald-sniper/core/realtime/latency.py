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
    def __init__(self, spike_threshold_ms: int = 150, alpha: float = 0.02):
        self._threshold = spike_threshold_ms
        self._alpha = alpha  # Slow adaptation to avoid 'chasing' jitter but tracking drift
        self._baseline_delta: Optional[float] = None
        
        # Diagnostics
        self.total_packets = 0
        self.stale_drops = 0

    def is_stale(self, exchange_ts_ms: int, local_ts_ms: float) -> bool:
        """
        Calculates if the packet is a 'spike' relative to the moving baseline.
        exchange_ts_ms: From exchange payload
        local_ts_ms: local time.time() * 1000.0
        """
        self.total_packets += 1
        
        # 1. Raw Delta (Local_Time - Exchange_Time)
        # May be negative if Local clock is behind Exchange
        current_delta = local_ts_ms - exchange_ts_ms

        # 2. Initialization: First packet sets the baseline
        if self._baseline_delta is None:
            self._baseline_delta = current_delta
            logger.info(f"🛰️ [LatencyGuard] Initialized baseline delta: {self._baseline_delta:.2f}ms")
            return False

        # 3. Spike Detection
        # Difference between current delay and our running average delay
        latency_spike = current_delta - self._baseline_delta

        # 4. Baseline Adaptation (EMA)
        # We only update the baseline if it's NOT a massive lag spike.
        # This prevents the baseline from 'adapting' to a stalled network.
        if abs(latency_spike) < self._threshold:
            # Regular drift (NTP/Clock-Speed/Route) adaptation
            self._baseline_delta = (self._alpha * current_delta) + ((1 - self._alpha) * self._baseline_delta)
        else:
            # If spike is negative (packet arrived "too fast"), it means local clock was shifted BACK (NTP)
            # We must force-resync the baseline immediately to prevent blocking all future packets.
            if latency_spike < -self._threshold:
                logger.warning(f"🕒 [LatencyGuard] Negative spike detected ({latency_spike:.2f}ms). Local clock shift? Resetting baseline.")
                self._baseline_delta = current_delta
                return False

        # 5. Filtering
        if latency_spike > self._threshold:
            self.stale_drops += 1
            if self.stale_drops % 100 == 1: # Squelched logging
                logger.warning(
                    f"🗑️ [StaleDrop] Packet delayed by {latency_spike:.2f}ms "
                    f"(Threshold: {self._threshold}ms). Baseline: {self._baseline_delta:.2f}ms"
                )
            return True

        return False

# Factory for monitoring
def get_latency_monitor(threshold_ms: int = 150) -> ILatencyMonitor:
    return DriftAwareLatencyMonitor(spike_threshold_ms=threshold_ms)
