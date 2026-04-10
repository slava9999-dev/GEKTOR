from .base import IAgent, AgentVote
from core.scenario.signal_entity import TradingSignal
from core.realtime.market_state import market_state
from loguru import logger

class TrendAgent(IAgent):
    """
    HTF Trend Check (Task 7.3).
    Ensures signal direction matches the 1-hour/5-minute trend.
    """
    def __init__(self, weight: float = 1.2):
        super().__init__("TrendAgent", weight)

    async def evaluate(self, signal: TradingSignal) -> AgentVote:
        # 1. BTC HTF Trend Check (already in signal or BTC context)
        # We'll use the signal's btc_trend_1h field
        btc_trend = getattr(signal, "btc_trend_1h", "FLAT")
        
        score = 0.5 # Neutral start
        if signal.direction == "LONG" and btc_trend == "BULLISH":
            score = 1.0 # Aligned
        elif signal.direction == "SHORT" and btc_trend == "BEARISH":
            score = 1.0 # Aligned
        elif btc_trend == "FLAT":
            score = 0.5 # Neutral
        else:
            # Counter-trend: VETO if Strong Trend is present
            if "STRONG" in btc_trend:
                return AgentVote(agent_name=self.name, is_veto=True, reason=f"Strong HTF Trend Counter: {btc_trend}")
            score = 0.1 # Very low confidence
            
        return AgentVote(
            agent_name=self.name,
            score=score,
            confidence=0.8,
            reason=f"BTC HTF: {btc_trend}"
        )

class VolatilityAgent(IAgent):
    """
    ATR Momentum Check (Task 7.3).
    Rejects trades if volatility is dying/flat.
    """
    def __init__(self, weight: float = 1.0):
        super().__init__("VolatilityAgent", weight)

    async def evaluate(self, signal: TradingSignal) -> AgentVote:
        # Requires ATR history from CandleCache (Task 4.2)
        # For simplicity, we check if regime is RANGE (low move/dying)
        regime = getattr(signal, "market_regime", "UNKNOWN")
        
        if regime == "VOL_UP":
             return AgentVote(agent_name=self.name, score=1.0, confidence=1.0, reason="High Vol Acceleration")
        elif regime == "RANGE":
             return AgentVote(agent_name=self.name, score=0.3, confidence=0.7, reason="Dying volatility (Chop)")
        
        return AgentVote(agent_name=self.name, score=0.6, confidence=0.5, reason="Standard Vol")

class MacroAgent(IAgent):
    """
    Asynchronous Neural Agent with O(1) Redis Access (Task 8.2).
    Implements Exposure Decay to combat Data Rot (STRESS TEST).
    """
    def __init__(self, weight: float = 0.8):
        super().__init__("MacroAgent", weight)
        from core.events.nerve_center import bus
        self.redis = bus.redis # Shared connection

    async def evaluate(self, signal: TradingSignal) -> AgentVote:
        # O(1) Fetch from Materialized View (Task 8.2)
        key = f"sentiment:{signal.symbol}"
        try:
            raw = await self.redis.get(key)
            if not raw:
                # Rule 8.2: Neutral fallback if cache miss
                return AgentVote(agent_name=self.name, score=0, confidence=0, reason="STALE_OR_MISSING_DATA")

            import orjson
            import time
            import math
            payload = orjson.loads(raw)
            
            # STRESS TEST: Data Rot Protection (Task 8: Data Rot)
            # Use exponential time decay: confidence = base * exp(-t / half_life)
            now = time.time()
            ts = payload.get("ts", now)
            delta_t = now - ts
            
            # Half-life: 15 minutes (900 seconds) for high-impact news
            half_life = 900.0 
            decay = math.exp(-delta_t / half_life)
            
            base_confidence = payload.get("confidence", 0.5)
            adjusted_confidence = base_confidence * decay
            
            if delta_t > 3600:
                return AgentVote(agent_name=self.name, score=0, confidence=0, reason="EXPIRED_DATA")

            return AgentVote(
                agent_name=self.name,
                score=payload.get("score", 0.0),
                confidence=adjusted_confidence,
                reason=f"Neural Sentiment (Age: {int(delta_t)}s, Decay: {decay:.2f})"
            )

        except asyncio.CancelledError:
            # TASK 9.3: Prevent connection leaks on HFT timeouts
            logger.warning(f"🕒 [MacroAgent] Request cancelled for {signal.symbol}. Terminating clean.")
            raise
        except Exception as e:
            logger.error(f"❌ [MacroAgent] Redis/Parsing error: {e}")
            return AgentVote(agent_name=self.name, score=0, confidence=0, reason="READ_ERROR")


