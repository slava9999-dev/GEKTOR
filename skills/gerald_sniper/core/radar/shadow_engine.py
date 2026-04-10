# core/radar/shadow_engine.py
"""
[GEKTOR v21.15.3] Shadow Execution Engine (Ghost Tracker).

Simulates Triple Barrier exits for signals when the ML model is 'blind'.
Enables Brier Score recovery without capital risk.
"""
import asyncio
import time
from loguru import logger
from core.radar.router import ScoredSignal
from core.radar.triple_barrier import triple_barrier, BarrierState, BarrierEvent
from core.radar.model_decay import decay_monitor, TradeOutcome

class ShadowTracker:
    """
    Simulates position management in memory.
    Listens to market ticks and evaluates virtual barriers.
    """
    def __init__(self, db_manager):
        self.db = db_manager
        self._active_shadows = {} # symbol -> BarrierState + metadata

    async def process_signal(self, signal: ScoredSignal) -> None:
        """Starts tracking a virtual position."""
        # Initialize virtual barriers
        bs = triple_barrier.init_barriers(
            entry_price=signal.barrier_data['entry_price'],
            current_sigma=signal.barrier_data.get('sigma', 0.02),
            is_long=signal.is_long
        )
        
        self._active_shadows[signal.symbol] = {
            'state': bs,
            'signal_id': signal.correlation_id,
            'prob': signal.probability
        }
        logger.warning(f"👻 [Shadow] Started tracking virtual #{signal.symbol}. ID={signal.correlation_id}")

    async def on_tick(self, symbol: str, price: float, volume_usd: float) -> None:
        """Evaluates virtual barriers on every tick (Shadow Fast-Track)."""
        data = self._active_shadows.get(symbol)
        if not data:
            return

        bs = data['state']
        # 1. Virtual Barrier Evaluation
        event = triple_barrier.evaluate(symbol, price, bs, volume_usd)
        
        if event:
            # 2. Record virtual outcome for calibration recovery
            is_success = 1 if event.event_type.startswith("UPPER") == bs.is_long else 0
            if event.event_type == "TIME_BARRIER_EXPIRATION":
                is_success = 0 # Time decay = loss of alpha
            
            decay_monitor.record_outcome(TradeOutcome(
                correlation_id=data['signal_id'],
                predicted_prob=data['prob'],
                is_success=is_success,
                timestamp=time.time()
            ))
            
            # Trigger recalibration batch cycle
            asyncio.create_task(decay_monitor.run_calibration_cycle())
            
            logger.info(f"👻 [Shadow] Virtual exit for #{symbol}: {event.event_type}. Outcome={is_success}")
            del self._active_shadows[symbol]

    async def on_macro_bar(self, symbol: str, current_price: float, sigma_db: float) -> None:
        """Breathes barriers in shadow mode."""
        data = self._active_shadows.get(symbol)
        if not data:
            return
            
        bs = data['state']
        triple_barrier.recalculate_barriers(bs, sigma_db, current_price)
        bs.bars_elapsed += 1

# Global Shadow Engine
shadow_engine = ShadowTracker(None)
