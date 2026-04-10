# skills/gerald-sniper/core/realtime/regime.py
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from loguru import logger
import time

class MarketRegime(Enum):
    RISK_ON = 1       # Macro Accumulation (Bullish Shift)
    NEUTRAL = 0       # Noise / Mean-Reversion
    RISK_OFF = -1     # Macro Distribution (Bearish Shift/Flash Crash)

@dataclass
class GlobalRegimeState:
    """
    [GEKTOR v21.21] O(1) Shared Memory State.
    Thread-safe readout via GIL (Atomic pointer updates).
    Ensures all Altcoin workers act in consensus with the 'Market Leader' (BTC).
    """
    current_regime: MarketRegime = MarketRegime.NEUTRAL
    baseline_volatility: float = 0.001
    last_shift_ts: float = 0.0
    btc_vpin: float = 0.0

class CUSUMBaselineMonitor:
    """
    [GEKTOR v21.21] Macro Structural Shift Detector.
    Implements Symmetric CUSUM (Cumulative Sum) Filter on BTC/USDT.
    - Lopez de Prado: Captures permanent structural change vs temporary volatility.
    """
    def __init__(self, shared_state: GlobalRegimeState, threshold_multiplier: float = 3.0):
        self.state = shared_state
        self.threshold_mult = threshold_multiplier
        self.s_pos = 0.0
        self.s_neg = 0.0
        self.last_price: Optional[float] = None
        self._last_vpin: float = 0.0

    def process_baseline_tick(self, timestamp: float, price: float, current_volatility: float, btc_vpin: float = 0.0):
        """Hot Path: Updates the Global Machine State. Only for Leader Asset (BTC)."""
        if self.last_price is None:
            self.last_price = price
            self.state.baseline_volatility = current_volatility
            return

        # 1. Logarithmic Return calculation
        ret = (price - self.last_price) / self.last_price
        self.last_price = price
        self.state.btc_vpin = btc_vpin
        
        # 2. Dynamic Update of Baseline Volatility (EMA-100)
        self.state.baseline_volatility = 0.99 * self.state.baseline_volatility + 0.01 * current_volatility
        threshold = self.state.baseline_volatility * self.threshold_mult

        # 3. Recursive CUSUM Calculation (Symmetric T-Filter)
        # Filters out noise below the current volatility floor
        self.s_pos = max(0.0, self.s_pos + ret - self.state.baseline_volatility)
        self.s_neg = min(0.0, self.s_neg + ret + self.state.baseline_volatility)

        # 4. Regime Transition Detection
        new_regime = self.state.current_regime
        if self.s_pos > threshold:
            new_regime = MarketRegime.RISK_ON
            self.s_pos, self.s_neg = 0.0, 0.0
        elif self.s_neg < -threshold:
            new_regime = MarketRegime.RISK_OFF
            self.s_pos, self.s_neg = 0.0, 0.0

        if new_regime != self.state.current_regime:
            logger.warning(f"🚨 [MACRO SHIFT] BTC Baseline: {self.state.current_regime.name} -> {new_regime.name}")
            self.state.current_regime = new_regime
            self.state.last_shift_ts = timestamp

# Global Singleton for Dependency Injection
global_regime = GlobalRegimeState()

class AltcoinConsensusEngine:
    """
    [GEKTOR v21.21] Idiosyncratic Alpha Resolver.
    Distinguishes between 'Market Beta' (correlated crash) and 'Asset Alpha' (decoupling).
    """
    def __init__(self, regime: GlobalRegimeState):
        self.regime = regime
        # Threshold for Idiosyncratic Piercing (Rule 2.2)
        # Conviction must be N times stronger than market noise
        self.idiosyncratic_vpin_threshold = 6.0 

    def validate_signal(self, asset: str, signal_type: str, alt_vpin: float) -> bool:
        """
        Validates signal against Macro Regime with Idiosyncratic Bypass.
        - Logic: ALLOW if (Regime OK) OR (Extreme Local Conviction AND Negative Correlation).
        """
        regime = self.regime.current_regime
        
        # 1. Standard Macro-Block (Filter Beta)
        is_blocked = False
        if signal_type == "LONG" and regime == MarketRegime.RISK_OFF:
            is_blocked = True
        elif signal_type == "SHORT" and regime == MarketRegime.RISK_ON:
            is_blocked = True

        if not is_blocked:
            return True

        # 2. Idiosyncratic Alpha Bypass (The 'Black Swan' Exception)
        # We allow a LONG during RISK_OFF only if conviction is dominant (VPIN decoupling)
        vpin_ratio = alt_vpin / (self.regime.btc_vpin + 1e-9)
        
        if vpin_ratio > self.idiosyncratic_vpin_threshold:
            logger.success(
                f"🛡️ [BYPASS] {asset} {signal_type} Approved via IDIOSYNCRATIC ALPHA. "
                f"Conviction Ratio: {vpin_ratio:.2f}x (PIERCING MACRO BLOCK)."
            )
            return True

        logger.debug(f"🛑 [SUPPRESSED] {asset} {signal_type} killed by {regime.name}. Conviction ratio: {vpin_ratio:.2f}x")
        return False
