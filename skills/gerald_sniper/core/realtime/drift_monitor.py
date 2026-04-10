# skills/gerald-sniper/core/realtime/drift_monitor.py
import asyncio
import logging
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import List, Tuple, Optional
from loguru import logger

class MLMathEngine:
    """
    Isolated container for CPU-heavy math operations. 
    Runs outside the main GIL to prevent event loop starvation.
    """
    
    @staticmethod
    def calculate_decayed_brier_score(actuals: np.ndarray, probas: np.ndarray, half_life: int = 100) -> float:
        """
        CPU-Bound: Time-weighted Brier Score calculation (Rule 2.2).
        Assigns higher weight to recent outcomes using exponential decay.
        """
        if len(actuals) == 0:
            return 0.0
            
        try:
            # t=0 is the newest outcome
            decay_rates = np.log(2) / half_life
            time_steps = np.arange(len(actuals))[::-1]
            weights = np.exp(-decay_rates * time_steps)
            weights /= weights.sum() # Normalization
            
            # Brier Score: (Predict - Actual)^2
            squared_errors = (probas - actuals) ** 2
            decayed_brier = np.sum(squared_errors * weights)
            
            return float(decayed_brier)
        except Exception:
            return 1.0 # Penalize errors

class ConceptDriftMonitor:
    """
    [GEKTOR v21.18] Self-Healing ML Monitor.
    - Process Isolation: GIL-free Brier Score evaluation.
    - Anti-Zombie Protocol: Timeouts and Future cancellation.
    - Self-Healing: Automatic resurrection of BrokenProcessPool.
    """
    
    def __init__(self, max_workers: int = 2, brier_threshold: float = 0.23):
        self._max_workers = max_workers
        self.brier_threshold = brier_threshold
        self.is_model_blind = False
        
        # Internal Pool Management
        self._process_pool = ProcessPoolExecutor(max_workers=self._max_workers)
        self._lock = asyncio.Lock() # For atomic resurrection

    async def _rebuild_pool(self):
        """Resurrects the process pool after an OOM/SIGKILL event."""
        async with self._lock:
            logger.critical("🚨 [DriftMonitor] ProcessPool is BROKEN. Triggering emergency resurrection...")
            try:
                self._process_pool.shutdown(wait=False, cancel_futures=True)
            except Exception: pass
            
            self._process_pool = ProcessPoolExecutor(max_workers=self._max_workers)
            logger.success("✅ [DriftMonitor] ProcessPool resurrected. System restored.")

    async def evaluate_drift_async(self, resolved_positions: List[Tuple[float, int]]):
        """
        Hybrid Async/Process entry point.
        Protects the Event Loop from blocking math AND from broken workers.
        """
        if not resolved_positions:
            return

        # Prepare payload
        probas = np.array([x[0] for x in resolved_positions])
        actuals = np.array([x[1] for x in resolved_positions])

        loop = asyncio.get_running_loop()
        
        try:
            # 1. Dispatch with Hard Timeout (Beazley Zombie Protection)
            # We don't want a worker death to hang the awaiter forever.
            brier_score = await asyncio.wait_for(
                loop.run_in_executor(
                    self._process_pool,
                    MLMathEngine.calculate_decayed_brier_score,
                    actuals,
                    probas
                ),
                timeout=10.0 # Extreme math shouldn't take > 10s
            )
            
            # 2. Threshold Analysis
            await self._process_score(brier_score)

        except BrokenProcessPool:
            # OS/OOM Killer struck a worker
            await self._rebuild_pool()
        except asyncio.TimeoutError:
            logger.error("⚠️ [DriftMonitor] Worker TIMEOUT. Possible OOM or infinite loop. Cancelling...")
            # If the pool is still working but this one task timed out, we might want to rebuild anyway 
            # to be safe from a "zombie worker" state
            await self._rebuild_pool()
        except Exception as e:
            logger.error(f"❌ [DriftMonitor] Unexpected worker failure: {e}")

    async def _process_score(self, brier_score: float):
        """Logical processing for the calculated score."""
        logger.info(f"📊 [ML] Decayed Brier Score: {brier_score:.4f}")
        
        if brier_score > self.brier_threshold:
            if not self.is_model_blind:
                logger.critical(
                    f"💀 [CONCEPT DRIFT] Brier Score ({brier_score:.3f}) > {self.brier_threshold}! "
                    "Model is BLIND. Signals routed to SHADOW/STRICT mode."
                )
                self.is_model_blind = True
        else:
            if self.is_model_blind:
                logger.success(f"👁️ [RECOVERY] Brier Score ({brier_score:.3f}) stabilized. Model vision RESTORED.")
                self.is_model_blind = False

    async def shutdown(self):
        """Graceful transition to darkness."""
        async with self._lock:
            self._process_pool.shutdown(wait=True)
            logger.info("🔌 [DriftMonitor] Pool shut down.")

# Global Monitor Instance
drift_monitor = ConceptDriftMonitor()
