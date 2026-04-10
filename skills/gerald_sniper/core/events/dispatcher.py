# core/events/dispatcher.py

import asyncio
from typing import List, Protocol, Dict
from loguru import logger
from pydantic import BaseModel

class SignalConsumer(Protocol):
    async def process_signal(self, signal: BaseModel) -> None:
        """Process incoming signal from any source (Radar, Manual, etc)."""
        ...

class SignalDispatcher:
    """
    [GEKTOR v8.5] Signal Dispatcher — Institutional Fan-Out Bus.
    
    Implements Arjan's Context Isolation:
    - Broadcasts signals to multiple isolated consumers (Shadow vs Combat).
    - Ensures failure in one context doesn't block the other.
    - Zero-latency parallel execution via asyncio.gather.
    """
    
    def __init__(self):
        self._consumers: Dict[str, SignalConsumer] = {}
        self._running_tasks: set[asyncio.Task] = set()

    def register(self, name: str, consumer: SignalConsumer):
        """Register a new trading context or advisory listener."""
        self._consumers[name] = consumer
        logger.info(f"📡 [Dispatcher] Context '{name}' registered.")

    async def broadcast(self, signal: BaseModel):
        """
        Веерная рассылка сигнала всем потребителям.
        """
        if not self._consumers:
            logger.warning("📡 [Dispatcher] No consumers registered. Signal dropped.")
            return

        logger.info(f"📡 [Dispatcher] Broadcasting {type(signal).__name__} to {len(self._consumers)} contexts.")
        
        # Capture all tasks for parallel execution
        tasks = []
        for name, consumer in self._consumers.items():
            task = asyncio.create_task(
                self._safe_process(name, consumer, signal),
                name=f"Dispatch_{name}"
            )
            tasks.append(task)
            self._running_tasks.add(task)
            task.add_done_callback(self._running_tasks.discard)

        # Non-blocking wait for all contexts to finish handling the broadcast
        # Note: We don't await here in a blocking way if we want maximum HFT speed, 
        # but for Radar signals, we want confirmation that they reached the entry points.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for name, res in zip(self._consumers.keys(), results):
            if isinstance(res, Exception):
                logger.error(f"❌ [Dispatcher] Context '{name}' FAILED signal processing: {res}")

    async def _safe_process(self, name: str, consumer: SignalConsumer, signal: BaseModel):
        """Wraps consumer call to prevent one crash from killing the dispatcher."""
        try:
            await consumer.process_signal(signal)
        except Exception as e:
            logger.error(f"💀 [Dispatcher] CRITICAL CRASH in context '{name}': {e}")
            raise

# Global singleton for easy integration
dispatcher = SignalDispatcher()
