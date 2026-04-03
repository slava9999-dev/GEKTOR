import numpy as np

class AdaptiveGates:
    def __init__(self):
        pass
        
    def should_execute_signal(self, signal_confidence, btc_trend, current_vol):
        # Dynamic adaptation to market regime
        regime = self._detect_regime(current_vol)
        
        if regime == "HIGH_VOL":
            confidence_threshold = 0.55  # More aggressive in volatility
        elif btc_trend in ('STRONG_UP', 'STRONG_DOWN'):
            confidence_threshold = 0.45  # VERY aggressive in trends
        else:
            confidence_threshold = 0.65  # Conservative in flat
            
        # Return boolean and the required threshold
        return signal_confidence >= confidence_threshold, confidence_threshold

    def _detect_regime(self, vol):
        if vol and vol > 4.0:
            return "HIGH_VOL"
        return "NORMAL"
        
    def get_dynamic_proximity_threshold(self, market_volatility, sentiment_score):
        """
        Dynamically adjusts the maximum allowed distance to level.
        Base is usually provided by config (e.g. 5.0%), here we multiply it.
        """
        base_multiplier = 1.0
        
        # In high volatility, we allow signals further away from the level
        if market_volatility and market_volatility > 4.0:
            base_multiplier *= 2.0
            
        # If sentiment is very strong, the momentum will carry it - allow further distance
        if abs(sentiment_score) > 30:
            base_multiplier *= 1.5
            
        return base_multiplier

adaptive_gates = AdaptiveGates()
