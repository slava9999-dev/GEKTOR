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


class VolatilityAdjustedOFI:
    """
    [GEKTOR v4.4] EMA Z-Score Filter with Micro-Noise Guard.

    Replaces cumulative Welford (which turns to concrete after 12h)
    with Exponential Moving Average that forgets old regime data.
    alpha=0.05 ≈ effective window of ~40 snapshots (~20 seconds at 2/sec).

    Dual Lock:
      1. Z-Score > threshold (statistical significance vs recent baseline)
      2. |OFI| > min_absolute_ofi (physical mass — rejects dust trades)
    
    min_absolute_ofi also acts as a variance floor:
      effective_std = max(ema_std, min_absolute_ofi / z_threshold)
    This prevents Z-Score explosions when liquidity dries up.
    """
    __slots__ = ['alpha', 'ema_mean', 'ema_var', 'min_absolute_ofi',
                 'count', '_warmup_required', '_z_threshold']

    def __init__(self, alpha: float = 0.05, min_absolute_ofi: float = 500.0, z_threshold: float = 2.5):
        self.alpha = alpha
        self.ema_mean: float = 0.0
        self.ema_var: float = 0.0
        self.min_absolute_ofi = min_absolute_ofi
        self._z_threshold = z_threshold
        self.count: int = 0
        # Warmup: need ~2x the effective window before trusting the EMA
        self._warmup_required = int(1 / alpha) * 2  # ~40 snapshots

    @property
    def is_calibrated(self) -> bool:
        return self.count >= self._warmup_required

    def reset_state(self) -> None:
        """Immediate state annihilation. Called on GAP DETECTED."""
        self.ema_mean = 0.0
        self.ema_var = 0.0
        self.count = 0

    def is_true_anomaly(self, current_ofi: float) -> bool:
        """
        Updates EMA stats and returns True only if the OFI is BOTH:
        - A statistical outlier (Z-Score > threshold)
        - Physically significant (|OFI| > min_absolute_ofi)
        """
        if self.count == 0:
            self.ema_mean = current_ofi
            self.ema_var = 0.0
            self.count += 1
            return False

        # EMA update (exponential decay — forgets old regimes)
        delta = current_ofi - self.ema_mean
        self.ema_mean += self.alpha * delta
        self.ema_var = (1 - self.alpha) * (self.ema_var + self.alpha * delta ** 2)
        self.count += 1

        # Block during warmup UNLESS it's an undeniable, massive institutional strike (> 5x absolute noise floor)
        # This answers the GAP paradox: we capture Godzilla-tier Alpha even while mathematically blind.
        if not self.is_calibrated:
            if abs(current_ofi) > self.min_absolute_ofi * 5.0:
                logger.warning(f"🦍 [GODZILLA BYPASS] Massive OFI ({current_ofi:.2f}) pierced the calibration matrix!")
                return True
            return False

        # Variance floor: prevents Z-Score explosion on dry liquidity
        std = math.sqrt(max(0.0, self.ema_var))
        effective_std = max(std, self.min_absolute_ofi / self._z_threshold)

        z_score = abs(current_ofi - self.ema_mean) / effective_std

        # DUAL LOCK: statistical outlier AND physical mass
        return (z_score > self._z_threshold) and (abs(current_ofi) > self.min_absolute_ofi)


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

        # [ADAPTIVE Z-SCORE FILTER] EMA-based noise suppressor
        self._z_filter = VolatilityAdjustedOFI(
            alpha=0.05,
            min_absolute_ofi=self._confirmed_threshold,
            z_threshold=2.5
        )

    def reset_state(self) -> None:
        """[GAP DETECTED] Immediate state annihilation. Forgets all previous microstructural context."""
        logger.warning(f"🧹 [MICROSTRUCTURE] Gap detected. Erasing memory for {self.symbol}.")
        self._prev_snapshot = None
        self._accumulated_ofi = 0.0
        self._current_state = "NEUTRAL"
        self._execution_buffer = 0.0
        self._z_filter.reset_state()

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
