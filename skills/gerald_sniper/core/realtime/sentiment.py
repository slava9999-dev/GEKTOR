# core/realtime/sentiment.py
"""
MarketSentiment v1.0 — Единый источник правды о настроении рынка.

Score: -100 (крайне медвежий) ... 0 (нейтральный) ... +100 (крайне бычий)

Replaces fragmented BTC checks across:
- TacticalOrchestrator._btc_penalty
- scoring.py macro_adj
- RegimeClassifier btc_up/btc_down
- CandleCache.btc_ctx trend string

Usage:
    sentiment = market_sentiment.score  # int -100..+100
    sentiment = market_sentiment.label  # "STRONG_BULL" / "BULL" / "NEUTRAL" / "BEAR" / "STRONG_BEAR"
    modifier  = market_sentiment.confidence_modifier("LONG")  # float, e.g. +0.12
"""

import time
from collections import deque
from dataclasses import dataclass, field
from loguru import logger


def _clamp(value: float, min_v: float, max_v: float) -> float:
    return max(min_v, min(max_v, value))


@dataclass
class SentimentSnapshot:
    """Immutable snapshot of sentiment at a point in time."""
    score: int = 0              # -100..+100
    label: str = "NEUTRAL"      # Human-readable
    btc_change_1h: float = 0.0
    btc_change_4h: float = 0.0
    momentum_aligned: bool = False  # 1h and 4h same direction
    timestamp: float = field(default_factory=time.time)


class MarketSentiment:
    """
    Centralized Market Sentiment Engine.
    
    Calculates a continuous score from BTC price action.
    Designed to REPLACE all fragmented BTC trend checks.
    """
    
    def __init__(self):
        self._score: int = 0
        self._label: str = "NEUTRAL"
        self._last_update: float = 0.0
        self._history: deque = deque(maxlen=100)
        self._btc_change_1h: float = 0.0
        self._btc_change_4h: float = 0.0
        self._momentum_aligned: bool = False
    
    @property
    def score(self) -> int:
        return self._score
    
    @property
    def label(self) -> str:
        return self._label
    
    @property
    def is_stale(self) -> bool:
        """Returns True if sentiment data is older than 10 minutes."""
        return (time.time() - self._last_update) > 600
    
    def update(self, btc_ctx: dict) -> int:
        """
        Recalculate sentiment from BTC context.
        Called from main loop alongside btc_ctx updates.
        
        Args:
            btc_ctx: dict with keys 'change_1h', 'change_4h', 'trend'
            
        Returns:
            Current sentiment score (-100..+100)
        """
        change_1h = btc_ctx.get('change_1h', 0.0)
        change_4h = btc_ctx.get('change_4h', 0.0)
        
        self._btc_change_1h = change_1h
        self._btc_change_4h = change_4h
        
        raw_score = 0.0
        
        # ─── Component 1: Short-term momentum (1h) ── Weight: 40% ───
        # Each 0.5% move = ~10 points. Clamped to ±40.
        raw_score += _clamp(change_1h * 20.0, -40.0, 40.0)
        
        # ─── Component 2: Medium-term trend (4h) ── Weight: 40% ───
        # Each 1% move = ~10 points. Clamped to ±40.
        raw_score += _clamp(change_4h * 10.0, -40.0, 40.0)
        
        # ─── Component 3: Momentum alignment ── Weight: 20% ───
        # If 1h and 4h agree -> stronger conviction
        self._momentum_aligned = False
        if change_1h > 0.1 and change_4h > 0.2:
            raw_score += 15.0  # Bullish alignment bonus
            self._momentum_aligned = True
        elif change_1h < -0.1 and change_4h < -0.2:
            raw_score -= 15.0  # Bearish alignment bonus
            self._momentum_aligned = True
        elif (change_1h > 0.1 and change_4h < -0.1) or (change_1h < -0.1 and change_4h > 0.1):
            # Divergence: 1h and 4h disagree → dampen score toward neutral
            raw_score *= 0.6
        
        # ─── Final score ───
        self._score = int(_clamp(raw_score, -100, 100))
        self._label = self._classify(self._score)
        self._last_update = time.time()
        
        # Save to history (keep last 50 snapshots for trend analysis)
        snapshot = SentimentSnapshot(
            score=self._score,
            label=self._label,
            btc_change_1h=change_1h,
            btc_change_4h=change_4h,
            momentum_aligned=self._momentum_aligned,
        )
        self._history.append(snapshot)
        
        logger.info(
            f"🌡️ [Sentiment] Score: {self._score:+d} ({self._label}) | "
            f"BTC 1h: {change_1h:+.2f}% | 4h: {change_4h:+.2f}% | "
            f"Aligned: {'✅' if self._momentum_aligned else '❌'}"
        )
        
        return self._score
    
    @staticmethod
    def _classify(score: int) -> str:
        if score >= 50:
            return "STRONG_BULL"
        elif score >= 20:
            return "BULL"
        elif score > -20:
            return "NEUTRAL"
        elif score > -50:
            return "BEAR"
        else:
            return "STRONG_BEAR"
    
    def confidence_modifier(self, direction: str) -> float:
        """
        Returns a confidence adjustment for a given trade direction.
        
        Positive = boost. Negative = penalty.
        Range: approximately -0.25 to +0.25
        
        For LONG:
            Bullish market → positive modifier (boost)
            Bearish market → negative modifier (penalty)
        For SHORT:
            Bullish market → negative modifier (penalty)
            Bearish market → positive modifier (boost)
        """
        if self.is_stale:
            return 0.0  # No modification if data is old
        
        # Base modifier: score / 400 → range ±0.25
        base = self._score / 400.0
        
        if direction == "LONG":
            return round(base, 3)
        elif direction == "SHORT":
            return round(-base, 3)
        
        return 0.0
    
    def macro_score_adjustment(self, direction: str, max_points: int = 15) -> int:
        """
        Returns a scoring adjustment for calculate_final_score().
        Replaces the hardcoded macro_adj in scoring.py.
        
        Returns int in range [-max_points, +max_points].
        """
        if self.is_stale:
            return 0
        
        # Proportional adjustment: sentiment score mapped to ±max_points
        raw = (self._score / 100.0) * max_points
        
        if direction == "LONG":
            return int(_clamp(raw, -max_points, max_points))
        elif direction == "SHORT":
            return int(_clamp(-raw, -max_points, max_points))
        
        return 0
    
    def get_trend_string(self) -> str:
        """
        Backward-compatible: returns a trend string like the old btc_ctx['trend'].
        Use this during migration to avoid breaking existing code.
        """
        if self._score >= 50:
            return "STRONG_UP"
        elif self._score >= 20:
            return "UP"
        elif self._score > -20:
            return "FLAT"
        elif self._score > -50:
            return "DOWN"
        else:
            return "STRONG_DOWN"
    
    def get_snapshot(self) -> SentimentSnapshot:
        """Returns current state as an immutable snapshot."""
        return SentimentSnapshot(
            score=self._score,
            label=self._label,
            btc_change_1h=self._btc_change_1h,
            btc_change_4h=self._btc_change_4h,
            momentum_aligned=self._momentum_aligned,
        )
    
    def get_trend_direction(self) -> int:
        """
        Returns simple directional bias.
        +1 = bullish, -1 = bearish, 0 = neutral
        """
        if self._score >= 20:
            return 1
        elif self._score <= -20:
            return -1
        return 0


# ─── Global singleton ───
market_sentiment = MarketSentiment()
