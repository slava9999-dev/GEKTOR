# core/realtime/cognitive_context.py

import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from loguru import logger

@dataclass
class CognitiveState:
    """
    [Audit 5.12] Market Intelligence Container.
    Stores the results of 'Slow Thread' LLM analysis to inject into 'Fast Thread' HFT execution.
    """
    sentiment_score: float = 0.0 # -1.0 to 1.0
    regime_label: str = "UNKNOWN" # TREND, RANGE, VOLATILE
    macro_bias: float = 1.0 # Multiplier for position sizing
    last_updated: float = 0.0
    source: str = "NONE"
    
    @property
    def is_fresh(self) -> bool:
        # LLM context is valid for 5 minutes in HFT context
        return (time.time() - self.last_updated) < 300

class CognitivePipeline:
    """
    Asymmetric Synchronization Bridge.
    Caches LLM insights to prevent I/O blocking in the 5ms Radar pipeline.
    """
    def __init__(self):
        self._state = CognitiveState()
        
    def get_current_bias(self, direction: str) -> float:
        """
        Instant access for ExecutionEngine. 
        Returns a confidence modifier based on cached LLM sentiment.
        """
        if not self._state.is_fresh:
            return 0.0
            
        # Logic: If direction matches sentiment, boost confidence.
        bias = 0.0
        if direction == "LONG" and self._state.sentiment_score > 0.3:
            bias = 0.1 * self._state.sentiment_score
        elif direction == "SHORT" and self._state.sentiment_score < -0.3:
            bias = 0.1 * abs(self._state.sentiment_score)
            
        return bias

    def update_state(self, sentiment: float, label: str, source: str):
        """Called by the background LLM worker."""
        self._state = CognitiveState(
            sentiment_score=sentiment,
            regime_label=label,
            last_updated=time.time(),
            source=source
        )
        logger.info(f"🧠 [Cognitive] State updated via {source} | Sentiment: {sentiment:.2f} | Regime: {label}")

cognitive_pipeline = CognitivePipeline()
