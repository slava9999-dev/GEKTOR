# core/scenario/signal_accumulator.py

import time
from typing import Dict, List, Set, Tuple
from loguru import logger
from .signal_entity import TradingSignal, SignalState

CONFLUENCE_RULES = {
    "min_factors": 1,
    "factor_groups": {
        "momentum": ["VELOCITY_SHOCK", "ACCELERATION", "MICRO_MOMENTUM"],
        "structure": ["LEVEL_PROXIMITY", "ORDERBOOK_IMBALANCE"],
        "quality":   ["RADAR_SCORE", "BTC_TREND"],
    },
    "require_different_groups": False,
    "accumulation_window_sec": 180, # Optimization: 120 -> 180 to capture slower confirmations
    "min_confidence": 0.65, # Roadmap 10/10: Return to strict but weighted scoring
}

FACTOR_WEIGHTS = {
    "VELOCITY_SHOCK":       0.35,
    "ACCELERATION":         0.30,
    "MICRO_MOMENTUM":       0.25,
    "ORDERBOOK_IMBALANCE":  0.25,
    "LEVEL_PROXIMITY":      0.30,
    "RADAR_SCORE":          0.15,
}

class SignalAccumulator:
    """
    Handles confluence logic (Roadmap Step 2):
    - Weighted confidence model
    - Synergy bonuses
    - Multi-group validation
    """
    
    @staticmethod
    def get_group(detector: str) -> str:
        for group, members in CONFLUENCE_RULES["factor_groups"].items():
            if detector.upper() in [m.upper() for m in members]:
                return group
        return "other"

    @staticmethod
    def evaluate_confluence(signal: TradingSignal) -> Tuple[bool, float, str]:
        """
        Returns (passed, confidence, reason)
        """
        detectors = signal.detectors
        if not detectors:
            return False, 0.0, "No detectors"

        unique_groups: Set[str] = set()
        for d in detectors:
            unique_groups.add(SignalAccumulator.get_group(d))

        # 1. Base Confidence Calculation
        confidence = 0.0
        for d in detectors:
            weight = FACTOR_WEIGHTS.get(d.upper(), 0.15)
            confidence += weight

        # 1a. Level Proximity Bonus (v5.2: Gradient scaling)
        # Replaces binary cutoff at 1.0% with linear decay to 3.0%
        if signal.level_distance_pct is not None and signal.level_distance_pct < 3.0:
            max_bonus = FACTOR_WEIGHTS.get("LEVEL_PROXIMITY", 0.30)
            level_bonus = round(max(0, max_bonus * (1.0 - signal.level_distance_pct / 3.0)), 2)
            confidence += level_bonus
            if level_bonus > 0.05:  # Only count as structure if meaningful
                unique_groups.add("structure")

        # 2. Synergy Bonuses (Rule 2.3)
        # Momentum + Structure = +0.10
        if "momentum" in unique_groups and "structure" in unique_groups:
            confidence += 0.10
            
        # 3+ Factors = +0.05
        if len(detectors) >= 3:
            confidence += 0.05
            
        # Radar Score Boost (if signal has score metadata)
        if signal.radar_score > 150:
            confidence += 0.15

        # 3. Validation Logic
        if len(detectors) < CONFLUENCE_RULES["min_factors"]:
            return False, confidence, f"Need {CONFLUENCE_RULES['min_factors']} factors (Got {len(detectors)})"
            
        if CONFLUENCE_RULES["require_different_groups"] and len(unique_groups) < 2:
            return False, confidence, f"Need factors from different groups (Groups: {list(unique_groups)})"

        confidence = round(confidence, 2)  # FIX T-01/T-02: IEEE 754 float precision

        if confidence < CONFLUENCE_RULES["min_confidence"]:
            if confidence >= 0.3:
                logger.debug(f"⚠️ [Confluence] {signal.symbol} weak: {confidence:.2f} < {CONFLUENCE_RULES['min_confidence']} | Factors: {detectors}")
            return False, confidence, f"Confidence {confidence:.2f} < {CONFLUENCE_RULES['min_confidence']}"

        return True, confidence, "Confluence achieved"
