import asyncio
import time
import logging
from typing import Dict
import signal

logger = logging.getLogger("GEKTOR.Watchdog")

class SilenceWatchdog:
    """[GEKTOR v2.0] O(1) Монитор ложного штиля (Sensor Failure)."""
    __slots__ = ['timeout_sec', '_last_seen', '_is_running', '_monitor_task', 'dead_mans_switch']

    def __init__(self, dead_mans_switch, timeout_sec: int = 300):
        self.timeout_sec = timeout_sec
        self.dead_mans_switch = dead_mans_switch
        # Предаллоцированный словарь для O(1) апдейтов
        self._last_seen: Dict[str, float] = {}
        self._is_running = False

    def ping(self, symbol: str) -> None:
        """Вызывается в Ingress-слое на КАЖДЫЙ валидный тик. O(1)."""
        self._last_seen[symbol] = time.monotonic()

    async def start(self):
        self._is_running = True
        self._monitor_task = asyncio.create_task(self._scan_loop())
        logger.info("👁️ [WATCHDOG] Silence Watchdog armed. Scanning for Sensor Failure.")

    async def _scan_loop(self):
        while self._is_running:
            await asyncio.sleep(10)  # Проверка раз в 10 секунд
            now = time.monotonic()
            
            for symbol, last_ts in self._last_seen.items():
                if now - last_ts > self.timeout_sec:
                    logger.critical(f"💀 [WATCHDOG] SENSOR FAILURE ON {symbol}. No ticks for {self.timeout_sec}s.")
                    # Атомарный вызов Кнопки Мертвеца
                    await self.dead_mans_switch.trigger_shutdown(signal=signal.SIGEMT)
                    self._is_running = False
                    break
