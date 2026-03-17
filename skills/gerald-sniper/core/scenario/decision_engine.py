# core/scenario/decision_engine.py

from loguru import logger
from typing import Tuple, Optional
from .signal_entity import TradingSignal, SignalState

class DecisionEngine:
    """
    FEAT-002: Decision Engine (Фильтры)
    Цепочка проверок для перевода EVALUATING -> APPROVED.
    """
    
    def __init__(self, config: dict):
        self.config = config.get("orchestrator", {})
        self.min_detectors = self.config.get("min_detectors", 1)
        self.max_dist = self.config.get("max_distance_percent", self.config.get("max_distance_to_level", 1.5))
        self.min_vol_ratio = self.config.get("min_volume_spike", 0.0)
        self.min_score = self.config.get("min_score", 60)

    def evaluate(self, signal: TradingSignal) -> Tuple[bool, Optional[str]]:
        """
        Проверка сигнала по цепочке фильтров.
        """
        # 1. Detector Confluence (Критично)
        if len(signal.detectors) < self.min_detectors:
            return False, f"Detector Confluence < {self.min_detectors} (Got {len(signal.detectors)})"

        # 2. Level Proximity (Critical + Stage 5 Tiering)
        if signal.level_distance_pct is None:
            return False, "No level found for confluence"
        
        # Roadmap 10/10 Tiered thresholds
        tier_thresholds = {
            "A": 0.5, # Tier 1
            "B": 1.0, # Tier 2
            "C": 2.0, # Tier 3
            "D": 3.0, # Tier 4
        }
        current_max_dist = tier_thresholds.get(signal.liquidity_tier, self.max_dist)

        if signal.level_distance_pct > current_max_dist:
            return False, f"Level Proximity failed for Tier {signal.liquidity_tier}: {signal.level_distance_pct:.2f}% > {current_max_dist}%"

        # 3. BTC Alignment — REMOVED (v5.0)
        # Now handled by graduated penalty in TacticalOrchestrator.process_signal
        # Keeping only as a safety net for STRONG mismatches
        if signal.direction == "SHORT" and "STRONG_UP" in str(signal.btc_trend_1h).upper():
            return False, f"BTC Strong Trend mismatch: {signal.btc_trend_1h} vs SHORT"
            
        if signal.direction == "LONG" and "STRONG_DOWN" in str(signal.btc_trend_1h).upper():
            return False, f"BTC Strong Trend mismatch: {signal.btc_trend_1h} vs LONG"

        # 4. Volume Confirmation (Важно)
        if signal.volume_spike_ratio < self.min_vol_ratio:
            return False, f"Volume Spike low: {signal.volume_spike_ratio:.1f}x < {self.min_vol_ratio}x"

        # 5. Spread Check (Опционально)
        if signal.spread_pct > 0.15:
            return False, f"Spread too wide: {signal.spread_pct:.2f}%"

        # 6. Priority Age (Опционально)
        if signal.priority_age_sec > 900: # 15 min
            return False, f"Signal stale: {signal.priority_age_sec:.0f}s"

        # 7. Radar Score (v5.0: Lowered from 60 to 40)
        adaptive_min_score = 40  # Let more signals through for signal-focused system
        if signal.radar_score > 0 and signal.radar_score < adaptive_min_score:
            return False, f"Low Radar Score: {signal.radar_score} < {adaptive_min_score}"
            
        # 8. Detectors per signal (Rule 13 compat)
        min_det_per_sig = self.config.get("min_detectors_per_signal", 1)
        if len(signal.detectors) < min_det_per_sig:
            return False, f"Not enough detectors: {len(signal.detectors)} < {min_det_per_sig}"

        return True, None

decision_engine = None # Will be initialized by Orchestrator
