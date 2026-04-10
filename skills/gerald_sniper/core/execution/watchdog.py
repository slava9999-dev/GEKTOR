import asyncio
from loguru import logger
from typing import Callable, Awaitable

class NetworkWatchdog:
    """
    [GEKTOR v14.7.4] Asynchronous Dead-Man's Switch for Network I/O.
    Protects against TCP Half-Open Sockets and silent disconnects by monitoring traffic flow.
    """
    def __init__(self, timeout_sec: float, on_timeout_callback: Callable[[], Awaitable[None]]):
        self.timeout_sec = timeout_sec
        self.on_timeout = on_timeout_callback
        self._task: asyncio.Task | None = None
        self._last_tick_time: float = 0.0

    def feed(self) -> None:
        """Resets the timer. Called on EVERY incoming message (O(1))."""
        self._last_tick_time = asyncio.get_running_loop().time()

    async def _watch(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                now = loop.time()
                elapsed = now - self._last_tick_time
                if elapsed > self.timeout_sec:
                    logger.critical(f"💀 [WATCHDOG] Network Silence detected: {elapsed:.1f}s > {self.timeout_sec}s threshold.")
                    # Trigger async recover / disconnect logic
                    asyncio.create_task(self.on_timeout())
                    return 
                
                # Sleep exactly until the potential timeout point
                await asyncio.sleep(self.timeout_sec - elapsed + 0.1)
        except asyncio.CancelledError:
            logger.debug("🛡️ [WATCHDOG] Watchdog stopped (Graceful shutdown).")

    def start(self) -> None:
        """Activates the guard."""
        if self._task is None or self._task.done():
            self._last_tick_time = asyncio.get_running_loop().time()
            self._task = asyncio.create_task(self._watch())

    def stop(self) -> None:
        """Deactivates the guard."""
        if self._task and not self._task.done():
            self._task.cancel()
