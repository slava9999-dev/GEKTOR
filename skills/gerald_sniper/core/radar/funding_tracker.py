# core/radar/funding_tracker.py
"""
[GEKTOR v21.15] Funding Rate Velocity Tracker.

Tracks historical Funding Rate snapshots per symbol in a ring buffer,
computes ΔFR/Δt (funding velocity), and exposes the "prior regime" state
for Short Squeeze discrimination.

Data Source: Bybit REST /v5/market/tickers (fundingRate field).
Already ingested in RadarV2Scanner → scanner.py:L70.
This module captures the TEMPORAL DERIVATIVE of that value.
"""
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from loguru import logger


@dataclass(slots=True)
class FundingSnapshot:
    """Single funding rate observation."""
    rate: float          # Raw funding rate (e.g., -0.0005 = -0.05%)
    timestamp: float     # Unix timestamp of observation


class FundingRateTracker:
    """
    [GEKTOR v21.15] Funding Rate Velocity Engine.
    
    Maintains a ring buffer of funding rate snapshots per symbol.
    Computes:
    1. Current funding rate
    2. ΔFR/Δt (velocity: how fast funding is changing)
    3. Prior regime (was funding deeply negative before the spike?)
    
    Update Cadence: Called by RadarV2Scanner on each scan cycle (~60s).
    """
    __slots__ = ('_history', '_max_snapshots', '_squeeze_fr_threshold')

    def __init__(self, max_snapshots: int = 60, squeeze_fr_threshold: float = -0.0003):
        """
        Args:
            max_snapshots: Number of historical FR observations to keep (default ~1h at 1/min).
            squeeze_fr_threshold: FR below this signals "short-heavy regime" (default -0.03%).
        """
        self._history: Dict[str, deque] = {}
        self._max_snapshots = max_snapshots
        self._squeeze_fr_threshold = squeeze_fr_threshold

    def record(self, symbol: str, funding_rate: float) -> None:
        """
        Records a funding rate snapshot from ticker data.
        Should be called once per scan cycle.
        """
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._max_snapshots)
        
        self._history[symbol].append(FundingSnapshot(
            rate=funding_rate,
            timestamp=time.time()
        ))

    def get_velocity(self, symbol: str, lookback_samples: int = 10) -> float:
        """
        Computes ΔFR/Δt: rate of change of funding rate over recent history.
        
        Positive velocity = FR rising (shorts closing / longs entering).
        Negative velocity = FR falling (longs closing / shorts entering).
        
        Returns:
            ΔFR per hour. E.g., 0.0003 means FR rose by 0.03% in the last hour.
        """
        buf = self._history.get(symbol)
        if not buf or len(buf) < 2:
            return 0.0

        # Use min(lookback, available) samples
        n = min(lookback_samples, len(buf))
        recent = list(buf)[-n:]
        
        # Simple linear regression slope: ΔFR / Δt (normalized to per-hour)
        if len(recent) < 2:
            return 0.0

        first = recent[0]
        last = recent[-1]
        dt_hours = (last.timestamp - first.timestamp) / 3600.0

        if dt_hours <= 0:
            return 0.0

        return (last.rate - first.rate) / dt_hours

    def get_prior_regime(self, symbol: str, lookback_samples: int = 30) -> Tuple[float, bool]:
        """
        Determines the "regime" before the current VPIN spike.
        
        Returns:
            - avg_prior_fr: Average FR over the lookback window
            - was_short_heavy: True if the market was deeply net-short (FR < squeeze_threshold)
        """
        buf = self._history.get(symbol)
        if not buf or len(buf) < 3:
            return (0.0, False)

        # Exclude the most recent 3 observations (the "spike period")
        # and average the prior ones
        n = min(lookback_samples, len(buf))
        historical = list(buf)[-n:-3] if n > 3 else list(buf)[:-1]
        
        if not historical:
            return (0.0, False)

        avg_fr = sum(s.rate for s in historical) / len(historical)
        was_short_heavy = avg_fr < self._squeeze_fr_threshold

        return (avg_fr, was_short_heavy)

    def get_current_rate(self, symbol: str) -> float:
        """Returns the most recent funding rate observation."""
        buf = self._history.get(symbol)
        if not buf:
            return 0.0
        return buf[-1].rate

    def get_diagnostics(self, symbol: str) -> Dict:
        """Full diagnostic dump for Telegram alert enrichment."""
        buf = self._history.get(symbol)
        if not buf:
            return {"error": "no_data"}
        
        velocity = self.get_velocity(symbol)
        avg_prior, was_short = self.get_prior_regime(symbol)
        
        return {
            "current_fr": buf[-1].rate,
            "velocity_per_hour": velocity,
            "avg_prior_fr": avg_prior,
            "was_short_heavy": was_short,
            "samples": len(buf)
        }


# Global singleton — initialized once in Composition Root
funding_tracker = FundingRateTracker()
