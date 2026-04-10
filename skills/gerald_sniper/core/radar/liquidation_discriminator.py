# core/radar/liquidation_discriminator.py
"""
[GEKTOR v21.15] Liquidation Discriminator — Phase 4 False Positive Filter.

Core Purpose:
    Distinguish between ORGANIC institutional buying (whale accumulation)
    and SHORT SQUEEZE (forced liquidation cascades) that produce identical
    VPIN signatures but have zero follow-through.

Architecture:
    Three-factor weighted discriminant function:
    1. ΔOI (Open Interest Delta)      — Weight: 0.45
    2. ΔFR/Δt (Funding Rate Velocity) — Weight: 0.30
    3. LiqVol Ratio                    — Weight: 0.25

    P(Squeeze) > 0.55 → Signal BLOCKED with LIQUIDATION_SQUEEZE_DETECTED.

Integration Point:
    ScoringEngine.process_macro_anomaly() — between Consensus Check and Core Scoring.

References:
    López de Prado, "Advances in Financial Machine Learning", Ch. 18 (VPIN limitations).
    Cont & Kukanov, "Optimal order placement in limit order markets" (Adverse Selection).
"""
import math
from dataclasses import dataclass
from typing import Optional, Tuple
from loguru import logger

from core.radar.liquidation_feed import liquidation_accumulator, LiquidationAccumulator
from core.radar.funding_tracker import funding_tracker, FundingRateTracker


@dataclass(slots=True, frozen=True)
class SqueezeVerdict:
    """Immutable result of squeeze discrimination analysis."""
    squeeze_probability: float
    is_squeeze: bool
    
    # Diagnostic decomposition (for Telegram alert enrichment)
    oi_score: float           # Contribution from OI Delta
    funding_score: float      # Contribution from Funding Velocity
    liq_ratio_score: float    # Contribution from Liquidation Volume
    
    # Raw inputs (for audit trail)
    delta_oi_pct: float
    funding_velocity: float
    liq_vol_ratio: float
    prior_funding_rate: float
    
    @property
    def reason(self) -> str:
        if not self.is_squeeze:
            return "ORGANIC_ACCUMULATION"
        
        factors = []
        if self.oi_score > 0.15:
            factors.append(f"OI_DECAY({self.delta_oi_pct:+.1f}%)")
        if self.funding_score > 0.10:
            factors.append(f"FR_WHIPLASH(v={self.funding_velocity:+.6f}/h)")
        if self.liq_ratio_score > 0.10:
            factors.append(f"LIQ_CASCADE({self.liq_vol_ratio:.0%})")
        
        return "LIQUIDATION_SQUEEZE: " + " + ".join(factors) if factors else "LIQUIDATION_SQUEEZE"


class _NullOIStore:
    """Null Object Pattern: Safe fallback when OI data is unavailable.
    Returns 0.0 for all queries instead of crashing with AttributeError."""
    def get_oi_delta_pct(self, symbol: str):
        return 0.0

_NULL_OI = _NullOIStore()


class LiquidationDiscriminator:
    """
    [GEKTOR v21.15] Three-Factor Squeeze Discriminant.
    
    Stateless computation engine. All state lives in:
    - OISnapshotStore (macro_radar.py) — injected via constructor
    - FundingRateTracker (funding_tracker.py) 
    - LiquidationAccumulator (liquidation_feed.py)
    
    This class is a pure function wrapper — safe for concurrent use.
    
    ArjanCodes: ALL dependencies are Constructor Injected.
    No set_something() methods. Immutable graph from boot.
    """
    # Discriminant weights (must sum to 1.0)
    W_OI = 0.45
    W_FUNDING = 0.30
    W_LIQ_VOL = 0.25
    
    # Thresholds
    SQUEEZE_THRESHOLD = 0.55       # P(Squeeze) above this = REJECT
    OI_DECAY_CRITICAL_PCT = -2.0   # OI fell >2% = strong squeeze signal
    OI_GROWTH_ORGANIC_PCT = 3.0    # OI grew >3% = strong organic signal
    FR_VELOCITY_CRITICAL = 0.0002  # FR jumped >0.02%/hour from negative territory
    LIQ_RATIO_CRITICAL = 0.40     # >40% of VPIN vol is liquidation-driven

    def __init__(
        self,
        oi_store=None,  # OISnapshotStore — Constructor Injected, NullObject fallback
        liq_accumulator: Optional[LiquidationAccumulator] = None,
        fr_tracker: Optional[FundingRateTracker] = None,
        squeeze_threshold: float = 0.55
    ):
        # ArjanCodes: Immutable after construction. No set_*() methods.
        self._oi_store = oi_store if oi_store is not None else _NULL_OI
        self._liq = liq_accumulator or liquidation_accumulator
        self._fr = fr_tracker or funding_tracker
        self.SQUEEZE_THRESHOLD = squeeze_threshold

    def evaluate(
        self,
        symbol: str,
        vpin_volume_usd: float,
        price_direction: int = 1  # 1 = price rising, -1 = falling
    ) -> SqueezeVerdict:
        """
        Main entry point. Evaluates whether the current VPIN anomaly
        is organic accumulation or a liquidation squeeze.
        
        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            vpin_volume_usd: Total USD volume in the VPIN anomaly window
            price_direction: 1 if price is rising, -1 if falling
            
        Returns:
            SqueezeVerdict with probability and diagnostic decomposition.
        """
        # === Factor 1: OI Delta ===
        delta_oi_pct = self._get_oi_delta(symbol)
        oi_score = self._score_oi_divergence(delta_oi_pct, price_direction)
        
        # === Factor 2: Funding Rate Velocity ===
        fr_velocity = self._fr.get_velocity(symbol)
        prior_fr, was_short_heavy = self._fr.get_prior_regime(symbol)
        funding_score = self._score_funding_velocity(fr_velocity, prior_fr, was_short_heavy)
        
        # === Factor 3: Liquidation Volume Ratio ===
        liq_ratio = self._liq.get_short_liq_ratio(symbol, vpin_volume_usd)
        liq_score = self._score_liq_ratio(liq_ratio)
        
        # === Composite Score ===
        squeeze_prob = max(0.0, min(1.0, oi_score + funding_score + liq_score))
        is_squeeze = squeeze_prob > self.SQUEEZE_THRESHOLD
        
        verdict = SqueezeVerdict(
            squeeze_probability=round(squeeze_prob, 4),
            is_squeeze=is_squeeze,
            oi_score=round(oi_score, 4),
            funding_score=round(funding_score, 4),
            liq_ratio_score=round(liq_score, 4),
            delta_oi_pct=round(delta_oi_pct, 2),
            funding_velocity=round(fr_velocity, 6),
            liq_vol_ratio=round(liq_ratio, 4),
            prior_funding_rate=round(prior_fr, 6)
        )
        
        if is_squeeze:
            logger.critical(
                f"🦈 [DISCRIMINATOR] [{symbol}] SHORT SQUEEZE DETECTED! "
                f"P(Sq)={squeeze_prob:.2%} | "
                f"OI={delta_oi_pct:+.1f}% | FR_v={fr_velocity:+.6f}/h | "
                f"LiqRatio={liq_ratio:.1%}"
            )
        else:
            logger.info(
                f"✅ [DISCRIMINATOR] [{symbol}] ORGANIC_ACCUMULATION confirmed. "
                f"P(Sq)={squeeze_prob:.2%}"
            )
        
        return verdict

    def _get_oi_delta(self, symbol: str) -> float:
        """Fetches OI Delta % from the OISnapshotStore (NullObject-safe)."""
        delta = self._oi_store.get_oi_delta_pct(symbol)
        return delta if delta is not None else 0.0

    def _score_oi_divergence(self, delta_oi_pct: float, price_direction: int) -> float:
        """
        OI Factor scoring (Weight: 0.45).
        
        Key insight: Price UP + OI DOWN = Positions closing (Squeeze).
                     Price UP + OI UP = New positions opening (Organic).
        """
        score = 0.0
        
        # Only applicable when price is RISING (potential long squeeze-in)
        if price_direction > 0:
            if delta_oi_pct < self.OI_DECAY_CRITICAL_PCT:
                # Strong OI decay during price rise = classic squeeze
                score = self.W_OI
            elif delta_oi_pct < 0:
                # Partial OI decay = partial squeeze signal
                score = self.W_OI * (abs(delta_oi_pct) / abs(self.OI_DECAY_CRITICAL_PCT))
            elif delta_oi_pct > self.OI_GROWTH_ORGANIC_PCT:
                # Strong OI growth = counterargument (organic buying)
                score = -0.20  # Negative contribution reduces total P(Squeeze)
        
        # Price FALLING + OI DOWN = Long Squeeze (inverse case, less common for us)
        elif price_direction < 0:
            if delta_oi_pct < self.OI_DECAY_CRITICAL_PCT:
                score = self.W_OI * 0.8  # Slightly lower weight for inverse case
        
        return max(0.0, score)

    def _score_funding_velocity(self, velocity: float, prior_fr: float, was_short_heavy: bool) -> float:
        """
        Funding Rate Factor scoring (Weight: 0.30).
        
        Key insight: If FR was deeply negative (shorts paying) and suddenly jumps positive,
                     it means shorts are being forcibly closed (liquidated).
        """
        if not was_short_heavy:
            # If prior FR wasn't negative, shorts weren't crowded.
            # Velocity alone doesn't indicate squeeze.
            return 0.0
        
        # FR was negative → now check velocity
        if velocity > self.FR_VELOCITY_CRITICAL:
            return self.W_FUNDING
        elif velocity > self.FR_VELOCITY_CRITICAL / 2:
            return self.W_FUNDING * (velocity / self.FR_VELOCITY_CRITICAL)
        
        return 0.0

    def _score_liq_ratio(self, liq_ratio: float) -> float:
        """
        Liquidation Volume Factor scoring (Weight: 0.25).
        
        Direct evidence: what fraction of buy volume is forced liquidation buys?
        """
        if liq_ratio > self.LIQ_RATIO_CRITICAL:
            return self.W_LIQ_VOL
        elif liq_ratio > self.LIQ_RATIO_CRITICAL * 0.5:
            return self.W_LIQ_VOL * (liq_ratio / self.LIQ_RATIO_CRITICAL)
        
        return 0.0


# Global singleton — initialized in Composition Root
liquidation_discriminator = LiquidationDiscriminator()
