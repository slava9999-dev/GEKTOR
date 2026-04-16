import math
from loguru import logger
from dataclasses import dataclass
from typing import Optional, Dict
from src.infrastructure.config import settings

@dataclass(slots=True, frozen=True)
class L2Level:
    price: float
    volume: float

@dataclass(slots=True, frozen=True)
class L2Snapshot:
    symbol: str
    best_bid: L2Level
    best_ask: L2Level
    exchange_ts: int


class AdaptiveOFIFilter:
    """
    [GEKTOR v4.3] Welford Online Z-Score Filter for OFI.
    
    Replaces static thresholds with a rolling statistical baseline.
    Only fires when |OFI Z-Score| > z_threshold, meaning the current OFI
    is a true statistical outlier relative to recent market noise.
    
    Calibration: First `min_samples` snapshots are used to build the baseline.
    During calibration, ALL impulses are suppressed (no false positives).
    """
    __slots__ = ['_k', '_m', '_s', '_min_samples', '_z_threshold']

    def __init__(self, min_samples: int = 100, z_threshold: float = 2.5):
        self._k: int = 0
        self._m: float = 0.0
        self._s: float = 0.0
        self._min_samples = min_samples
        self._z_threshold = z_threshold

    @property
    def is_calibrated(self) -> bool:
        return self._k >= self._min_samples

    def ingest(self, ofi: float) -> float:
        """
        Updates running stats and returns Z-Score.
        Returns 0.0 during calibration phase.
        """
        self._k += 1
        old_m = self._m
        self._m += (ofi - self._m) / self._k
        self._s += (ofi - old_m) * (ofi - self._m)

        if not self.is_calibrated:
            return 0.0

        variance = self._s / (self._k - 1) if self._k > 1 else 0.0
        std = math.sqrt(max(0.0, variance))

        if std < 1e-9:
            return 0.0

        return (ofi - self._m) / std

    def is_true_anomaly(self, ofi: float) -> bool:
        """Returns True only if OFI is a statistically significant outlier."""
        z = self.ingest(ofi)
        return abs(z) >= self._z_threshold


class MicrostructureDefender:
    """
    [GEKTOR v4.3] Integrity Verification Module (Phantom Liquidity Filter).
    
    Uses Adaptive Z-Score OFI filtering to eliminate HFT spoofing noise.
    Static thresholds are retained ONLY as hysteresis release gates.
    Signal generation requires BOTH:
      1. Z-Score anomaly (statistical significance)
      2. Hysteresis state transition from NEUTRAL (structural confirmation)
    """
    def __init__(self, symbol: str, confirmed_threshold: Optional[float] = None, release_factor: Optional[float] = None):
        self.symbol = symbol
        self._prev_snapshot: Optional[L2Snapshot] = None
        self._accumulated_ofi: float = 0.0
        
        # [INTRADAY v4.1] Адаптивные пороги в зависимости от ликвидности актива
        is_major = symbol in ["BTCUSDT", "ETHUSDT"]
        config = settings.MICRO_OFI_CONFIG["MAJORS"] if is_major else settings.MICRO_OFI_CONFIG["ALTS"]
        
        self._confirmed_threshold = confirmed_threshold or config["threshold"]
        factor = release_factor or config["factor"]
        self._release_threshold = self._confirmed_threshold * factor
        
        # Tracks realized trade volume between snapshots
        self._execution_buffer: float = 0.0 
        
        # [HYSTERESIS STATE MACHINE]
        self._current_state: str = "NEUTRAL" # NEUTRAL, ACCUMULATION, DISTRIBUTION

        # [ADAPTIVE Z-SCORE FILTER] Welford-based noise suppressor
        self._z_filter = AdaptiveOFIFilter(min_samples=100, z_threshold=2.5)

    def update_execution(self, volume: float, side: str):
        """Updates the trade volume delta since the last L2 snapshot."""
        # Simple net volume: Buy (+) / Sell (-)
        if side.upper() == 'BUY':
            self._execution_buffer += volume
        else:
            self._execution_buffer -= volume

    def calculate_informed_ofi(self, current: L2Snapshot) -> float:
        """
        Calculates OFI with Trade-Delta Convergence.
        Prevents being trapped by HFT spoofers (Phantom Liquidity).
        """
        if not self._prev_snapshot:
            self._prev_snapshot = current
            return 0.0

        p_bid, p_ask = self._prev_snapshot.best_bid, self._prev_snapshot.best_ask
        c_bid, c_ask = current.best_bid, current.best_ask

        # 1. Raw OFI Calculation (Cont et al, 2014)
        ofi_bid = 0.0
        if c_bid.price >= p_bid.price: ofi_bid += c_bid.volume
        if c_bid.price <= p_bid.price: ofi_bid -= p_bid.volume

        ofi_ask = 0.0
        if c_ask.price <= p_ask.price: ofi_ask += c_ask.volume
        if c_ask.price >= p_ask.price: ofi_ask -= p_ask.volume

        raw_ofi = ofi_bid - ofi_ask
        
        # 2. [PHANTOM FILTER] Check convergence with realized Trade Delta
        confirmed_ofi = raw_ofi
        if raw_ofi > 0: # Bullish Imbalance
            confirmed_ofi = min(raw_ofi, max(self._execution_buffer, 0) * 1.5)
        elif raw_ofi < 0: # Bearish Imbalance
            confirmed_ofi = max(raw_ofi, min(self._execution_buffer, 0) * 1.5)

        # 3. Update state
        self._prev_snapshot = current
        self._execution_buffer = 0.0 # Reset for next window
        
        # EWMA for noise reduction
        self._accumulated_ofi = (confirmed_ofi * 0.2) + (self._accumulated_ofi * 0.8)
        return self._accumulated_ofi

    async def ingest_snapshot(self, snapshot: L2Snapshot) -> dict:
        """Entry point for the Event Bus with Adaptive Z-Score + Hysteresis Shield."""
        ofi = self.calculate_informed_ofi(snapshot)
        is_new_impulse = False
        prev_state = self._current_state

        # [GATE 0: ADAPTIVE Z-SCORE] Feed OFI into Welford filter.
        # During calibration (first 100 snapshots), z_is_anomaly is always False.
        # After calibration, only statistically significant OFI deviations pass.
        z_is_anomaly = self._z_filter.is_true_anomaly(ofi)

        # [HYSTERESIS STATE MACHINE] — transitions require Z-Score confirmation
        if self._current_state == "NEUTRAL":
            # Entry requires BOTH: Z-Score anomaly AND directional threshold breach
            if z_is_anomaly and ofi > self._confirmed_threshold:
                self._current_state = "ACCUMULATION"
                is_new_impulse = True
                logger.warning(f"🚨 [MICROSTRUCTURE] ACCUMULATION IMPULSE STARTED | {snapshot.symbol} | OFI: {ofi:.2f}")
            elif z_is_anomaly and ofi < -self._confirmed_threshold:
                self._current_state = "DISTRIBUTION"
                is_new_impulse = True
                logger.warning(f"🚨 [MICROSTRUCTURE] DISTRIBUTION IMPULSE STARTED | {snapshot.symbol} | OFI: {ofi:.2f}")

        elif self._current_state == "ACCUMULATION":
            # Release does NOT require Z-Score — just mean-reversion
            if ofi < self._release_threshold:
                self._current_state = "NEUTRAL"
        
        elif self._current_state == "DISTRIBUTION":
            if ofi > -self._release_threshold:
                self._current_state = "NEUTRAL"

        # Safety latch: impulse only valid from NEUTRAL transition
        if is_new_impulse and prev_state != "NEUTRAL":
            is_new_impulse = False

        return {
            "symbol": snapshot.symbol,
            "ofi": ofi,
            "state": self._current_state,
            "is_new_impulse": is_new_impulse
        }
