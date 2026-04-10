# core/execution/priority_gateway.py
import asyncio
import time
from typing import Any, Callable, Dict, Optional
from loguru import logger
from dataclasses import dataclass, field

@dataclass(order=True)
class PrioritizedTask:
    priority: int
    name: str = field(compare=False)
    coro_fn: Callable = field(compare=False)
    future: asyncio.Future = field(compare=False)
    created_at: float = field(default_factory=time.time, compare=True)

class PriorityRESTGateway:
    """
    [GEKTOR v14.8.6] Global Priority Throttle.
    Martin Kleppmann Standard: Strictly coordinates REST traffic to prevent 429 Cascades.
    0 - CRITICAL: Order Entry/Exit (ExecutionEngine)
    1 - URGENT: Portfolio Reconcilation (LimboReaper)
    2 - ROUTINE: Market Data Recovery (Reconciler/Radar)
    """
    def __init__(self, rate_limit_hz: int = 20):
        self._queue = asyncio.PriorityQueue()
        self._rate_limit_delay = 1.0 / rate_limit_hz
        self._last_call = 0.0
        self._running = False

    async def start(self):
        if self._running: return
        self._running = True
        asyncio.create_task(self._dispatcher_loop())
        logger.info(f"🚦 [PriorityGateway] REST Dispatcher active ({1.0/self._rate_limit_delay} Hz)")

    async def execute(self, priority: int, name: str, coro_fn: Callable) -> Any:
        """Wraps an async call with a priority token."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        task = PrioritizedTask(priority=priority, name=name, coro_fn=coro_fn, future=future)
        await self._queue.put(task)
        return await future

    async def _dispatcher_loop(self):
        while self._running:
            task = await self._queue.get()
            
            # Token Bucket Implementation
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self._rate_limit_delay:
                await asyncio.sleep(self._rate_limit_delay - elapsed)
            
            try:
                # Execution
                result = await task.coro_fn()
                if not task.future.done():
                    task.future.set_result(result)
            except Exception as e:
                logger.error(f"❌ [REST_GATEWAY] Fatal error in {task.name}: {e}")
                if not task.future.done():
                    task.future.set_exception(e)
            finally:
                self._last_call = time.monotonic()
                self._queue.task_done()
