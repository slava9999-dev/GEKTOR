# core/radar/model_decay.py
"""
[GEKTOR v21.15.2] Model Decay Monitor (Concept Drift & Temporal Decay).

Core Philosophy:
    ML models are "frozen snapshots" of past market regimes. 
    When the regime shifts (Black Swan, Rate Hikes), the model becomes 
    a "Hallucinating Oracle" — high confidence, zero accuracy.

Metrics:
    1. Brier Score: Mean Squared Error of probability vs outcome.
       BS = (1/N) * sum((prob - outcome)^2)
       - 0.00: Perfect Clairvoyance
       - 0.25: Random Guessing (Coin flip)
       - >0.25: Inverse Signal (Systemically wrong)

    2. Temporal Decay (Time-Dislocation Protection):
       Weights samples using e^(-lambda * dt). 
       Old success stories lose influence over time.
       If No-Trade-Gap > 72h, Brier Score is artificially penalized.

Integration:
    Captures 'ADVISORY_EXIT' events from MonitoringOrchestrator.
    Emits 'MODEL_STALE_ALERT' to block Phase 4 entry logic.
"""
import time
import math
import numpy as np
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List, Tuple, Optional
from loguru import logger


@dataclass(frozen=True, slots=True)
class TradeOutcome:
    correlation_id: str
    predicted_prob: float  # 0.0 to 1.0 (from Random Forest)
    is_success: int        # 1 for TP reached, 0 for SL/Time expiry
    timestamp: float       # When the trade closed


@dataclass(frozen=True, slots=True)
class DecayVerdict:
    brier_score: float
    is_blind: bool
    sample_count: int
    last_calibration_age_h: float
    verdict_text: str


class ModelDecayMonitor:
    """
    Online Concept Drift Monitor with Exponential Time Decay.
    Ensures the ML engine stays calibrated to current market regimes.
    """
    def __init__(
        self, 
        window_size: int = 50, 
        drift_threshold: float = 0.23,  # Tight bracket: block before 0.25
        decay_half_life_days: float = 7.0
    ):
        self._history: Deque[TradeOutcome] = deque(maxlen=window_size)
        self._threshold = drift_threshold
        # Decay lambda: ln(2) / half_life_in_seconds
        self._lambda = math.log(2) / (decay_half_life_days * 86400)
        self._is_blocked = False
        
        # [GEKTOR v21.15.3] Async Batching & Thread Isolation
        self._dirty = False
        self._eval_lock = asyncio.Lock()

    def record_outcome(self, outcome: TradeOutcome) -> None:
        """Records a new calibration point. Marks as dirty for batch processing."""
        self._history.append(outcome)
        self._dirty = True
        logger.debug(f"📈 [ModelDecay] Buffered outcome: ID={outcome.correlation_id}. Queue={len(self._history)}")

    async def run_calibration_cycle(self) -> None:
        """
        [GEKTOR v21.15.3] Debounced Batch Processor.
        Designed to handle clusters of TradeClosedEvents (e.g. during a flash crash).
        """
        if not self._dirty or self._eval_lock.locked():
            return

        async with self._eval_lock:
            # 1. Debounce: wait for the storm to subside (100ms)
            await asyncio.sleep(0.1)
            
            # 2. Extract snapshot of history
            history_snapshot = list(self._history)
            self._dirty = False # Reset dirty flag after snapshot
            
            # 3. Offload heavy math to ThreadPool to keep Event Loop free (Zero Blocking)
            loop = asyncio.get_running_loop()
            verdict = await loop.run_in_executor(None, self._calculate_sync, history_snapshot)
            
            # 4. Notify Router (Update Hysteresis)
            from core.radar.router import signal_router
            await signal_router.update_calibration(verdict.brier_score)
            
            if verdict.is_blind and not self._is_blocked:
                self._is_blocked = True
                logger.critical(f"🛑 [ModelDecay] DRIFT DETECTED. BS={verdict.brier_score:.4f}. LOCKING LIVE TRACK.")
            elif not verdict.is_blind and self._is_blocked:
                self._is_blocked = False
                logger.success(f"🟢 [ModelDecay] CALIBRATED. BS={verdict.brier_score:.4f}. UNLOCKING LIVE TRACK.")

    def _calculate_sync(self, history: List[TradeOutcome]) -> DecayVerdict:
        """Synchronous part: Pure math on NumPy arrays. Runs in ThreadPool."""
        if not history:
            return DecayVerdict(0.0, False, 0, 0.0, "INITIALIZING")

        now = time.time()
        
        # Vectorized calculations
        probs = np.array([h.predicted_prob for h in history], dtype=np.float32)
        outcomes = np.array([h.is_success for h in history], dtype=np.float32)
        timestamps = np.array([h.timestamp for h in history], dtype=np.float32)
        
        dts = now - timestamps
        weights = np.exp(-self._lambda * dts)
        sq_errors = (probs - outcomes) ** 2
        
        # 3. Handle No-Trade-Gap (Entropy Injection)
        last_trade_age_h = (now - history[-1].timestamp) / 3600
        staleness_penalty = 0.0
        if last_trade_age_h > 72:
            staleness_penalty = (last_trade_age_h - 72) / 12 * 0.01

        # Weighted Mean Brier Score
        total_weight = np.sum(weights)
        if total_weight > 0:
            brier_score = (np.sum(sq_errors * weights) / total_weight) + staleness_penalty
        else:
            brier_score = 0.5 + staleness_penalty

        is_blind = brier_score > self._threshold
        
        return DecayVerdict(
            brier_score=float(np.round(brier_score, 4)),
            is_blind=is_blind,
            sample_count=len(history),
            last_calibration_age_h=float(np.round(last_trade_age_h, 1)),
            verdict_text="MODEL_HEALTHY" if not is_blind else "MODEL_DECAYED"
        )

    @property
    def is_blocked(self) -> bool:
        return self._is_blocked

    def format_alert(self, verdict: DecayVerdict) -> str:
        """Formats alert for Telegram."""
        status = "🛑 *FATAL: CONCEPT DRIFT*" if verdict.is_blind else "📉 *MODEL CALIBRATION*"
        emoji = "⚠️" if verdict.brier_score > 0.20 else "✅"
        
        return (
            f"{status}\n"
            f"Brier Score: `{verdict.brier_score:.4f}` {emoji}\n"
            f"Threshold: `{self._threshold}`\n"
            f"Calibration Age: `{verdict.last_calibration_age_h}h`\n"
            f"Samples (N): `{verdict.sample_count}`\n"
            f"Verdict: `{verdict.verdict_text}`\n"
            f"{'❌ _Signals HALTED - Model is blind._' if verdict.is_blind else '🦾 _ML engine operational._'}"
        )


# Global Singleton for Calibration
decay_monitor = ModelDecayMonitor()
