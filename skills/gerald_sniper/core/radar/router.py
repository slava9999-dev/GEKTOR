# core/radar/router.py
"""
[GEKTOR v21.15.3] Execution Router (Execution Circuit Breaker).

Core Architecture:
    Implements the 'Bridge' pattern between Signal Generation (Phase 4)
    and Signal Delivery (Phase 6).
    
    States:
    1. ARMED (Live): Brier Score < 0.20. Signals go to Telegram Operator.
    2. BLIND (Shadow): Brier Score > 0.23. Signals go to ShadowTracker 
       for virtual validation. This enables Feedback Loop Recovery 
       without risking capital.
    
    Hysteresis:
        To prevent 'flapping' (oscillation) on boundary trades,
        we use 0.23 to shut down and 0.20 to recover.
"""
import asyncio
from typing import Dict, Optional, Protocol
from loguru import logger
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class ScoredSignal:
    correlation_id: str
    symbol: str
    probability: float
    is_long: bool
    barrier_data: dict
    size_usd: float

class SignalDestination(Protocol):
    async def process_signal(self, signal: ScoredSignal) -> None: ...


class ExecutionRouter:
    """
    Transactional Switch. Decouples alpha generation from risk exposure.
    If the model is 'blind' (high Brier Score), signals are routed 
    to a ghost engine to recalibrate.
    """
    def __init__(
        self, 
        live_outbox: Optional[SignalDestination] = None, 
        shadow_engine: Optional[SignalDestination] = None,
        blindness_threshold: float = 0.23,
        recovery_threshold: float = 0.20
    ):
        self._live_outbox = live_outbox
        self._shadow_engine = shadow_engine
        self._is_blind = False
        self._threshold_halt = blindness_threshold
        self._threshold_recover = recovery_threshold
        self._current_score = 0.0
        self._lock = asyncio.Lock()

    def set_destinations(self, live: SignalDestination, shadow: SignalDestination):
        self._live_outbox = live
        self._shadow_engine = shadow

    async def update_calibration(self, brier_score: float) -> None:
        """
        [GEKTOR v21.15.3] Hysteresis Logic.
        Called by ModelDecayMonitor after batch recalculation.
        """
        async with self._lock:
            self._current_score = brier_score
            
            if not self._is_blind and brier_score >= self._threshold_halt:
                self._is_blind = True
                logger.critical(
                    f"🛑 [Router] SYSTEM BLIND! Brier Score={brier_score:.4f} >= {self._threshold_halt}. "
                    f"Switching to SHADOW_GHOST_TRACK."
                )
            
            elif self._is_blind and brier_score <= self._threshold_recover:
                self._is_blind = False
                logger.success(
                    f"🟢 [Router] SYSTEM RECOVERY! Brier Score={brier_score:.4f} <= {self._threshold_recover}. "
                    f"LIVE_ARMED."
                )

    async def route_signal(self, signal: ScoredSignal) -> None:
        """
        Standard routing entry point. Called by ScoringEngine.
        Deterministic and non-blocking.
        """
        if self._is_blind:
            logger.warning(f"👻 [Router] [{signal.symbol}] Routing to SHADOW engine (Paper Trade).")
            if self._shadow_engine:
                await self._shadow_engine.process_signal(signal)
        else:
            logger.info(f"⚔️ [Router] [{signal.symbol}] Routing to LIVE outbox (Combat Mode).")
            if self._live_outbox:
                await self._live_outbox.process_signal(signal)

    @property
    def is_blind(self) -> bool:
        return self._is_blind

    @property
    def mode(self) -> str:
        return "SHADOW" if self._is_blind else "LIVE"


# Global Routing Singleton
signal_router = ExecutionRouter()
