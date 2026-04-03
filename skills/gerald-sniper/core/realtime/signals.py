import time
from typing import List, Dict, Set
from dataclasses import dataclass, field
from loguru import logger

@dataclass
class SignalEvent:
    symbol: str
    signal_type: str # e.g. MOMENTUM_BREAKOUT, VOLATILE_REVERSAL
    confidence: int
    factors: Set[str]
    price: float
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

class SignalEngine:
    """
    Gerald v3.1 Signal Engine.
    Combines independent detector factors into high-confidence trading signals.
    Rule 13: min 2 independent factors. Confidence >= 70.
    """
    def __init__(self):
        self.active_signals: Dict[str, SignalEvent] = {}
        self.cooldowns: Dict[str, float] = {} # symbol -> last_signal_ts

    def process_hits(self, symbol: str, hits: List[dict], current_price: float) -> List[SignalEvent]:
        """
        Analyzes a batch of hits for a specific symbol.
        Returns newly generated signals.
        """
        new_signals = []
        now = time.time()
        
        if not hits or len(hits) < 2:
            return []
            
        # Cooldown check: Rule 10 - Signal cooldown (60s default for same symbol)
        if now - self.cooldowns.get(symbol, 0) < 60:
            return []

        factor_types = {h['type'] for h in hits}
        confidence = self._calculate_confidence(hits)
        
        # Rule 13: Final Signal Engine trigger logic
        signal_type = None
        
        # Scenario: MOMENTUM BREAKOUT
        # VELOCITY_SHOCK + VOLUME_EXPLOSION + MICRO_MOMENTUM
        if "VELOCITY_SHOCK" in factor_types and "VOLUME_EXPLOSION" in factor_types:
            signal_type = "MOMENTUM_BREAKOUT"
        
        # Scenario: ORDERFLOW SURGE
        # ORDERBOOK_IMBALANCE + VELOCITY_SHOCK
        elif "ORDERBOOK_IMBALANCE" in factor_types and ("VELOCITY_SHOCK" in factor_types or "MICRO_MOMENTUM" in factor_types):
            signal_type = "ORDERFLOW_PRESSURE"
            
        if signal_type and confidence >= 70:
            sig = SignalEvent(
                symbol=symbol,
                signal_type=signal_type,
                confidence=confidence,
                factors=factor_types,
                price=current_price,
                metadata={"details": hits}
            )
            self.active_signals[symbol] = sig
            self.cooldowns[symbol] = now
            new_signals.append(sig)
            logger.warning(f"🏆 [SignalEngine] {symbol} | {signal_type} | Conf: {confidence} | Factors: {len(factor_types)}")
            
        return new_signals

    def _calculate_confidence(self, hits: List[dict]) -> int:
        """
        Confidence formula Rule 13:
        0.35 momentum_strength + 0.25 volume_spike + 0.20 orderflow
        (Refined for current hits structure)
        """
        score = 0
        factor_types = {h['type'] for h in hits}
        
        # Base score for factor count
        score += min(30, len(factor_types) * 10)
        
        for h in hits:
            t = h['type']
            if t == "VELOCITY_SHOCK": 
                ratio = h.get('ratio', 1.0)
                score += min(25, int(ratio * 4)) # max 25
            if t == "VOLUME_EXPLOSION":
                ratio = h.get('ratio', 1.0)
                score += min(20, int(ratio * 3)) # max 20
            if t == "ORDERBOOK_IMBALANCE":
                score += 15
            if t == "MICRO_MOMENTUM":
                score += 10
            if t == "ACCELERATION":
                score += 10
                
        return min(100, score)
