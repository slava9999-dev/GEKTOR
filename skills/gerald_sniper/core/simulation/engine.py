# skills/gerald_sniper/core/simulation/engine.py
import asyncio
from typing import AsyncGenerator, Optional
from loguru import logger
from pydantic import BaseModel

class VirtualTimeEventLoop(asyncio.SelectorEventLoop):
    """
    [GEKTOR v21.49] Детерминированный Event Loop.
    Хак ядра asyncio для Warp-Speed симуляции. 
    Позволяет asyncio.sleep() и таймаутам срабатывать по виртуальным часам исторического потока.
    """
    def __init__(self):
        super().__init__()
        self._virtual_time = 0.0

    def time(self) -> float:
        """Перехват системного таймера."""
        return self._virtual_time

    def advance_time(self, advance_by_seconds: float):
        """
        Продвижение временного континуума. 
        Двигает таймеры asyncio корутин вперед.
        """
        self._virtual_time += advance_by_seconds
        # Принудительное выполнение шага цикла для пробуждения упавших в sleep тасков
        self._run_once()

class SimulatedTime:
    def __init__(self, start_ts_ms: int):
        self._current_ts_ms = start_ts_ms

    def now_ms(self) -> int:
        return self._current_ts_ms

    def set_time(self, ts_ms: int):
        self._current_ts_ms = ts_ms

class WarpSpeedBacktestDriver:
    """
    Объединенный драйвер симуляции. 
    Синхронизирует поток тиков и Виртуальный Event Loop.
    """
    def __init__(self, loop: VirtualTimeEventLoop, engine, shadow_tracker):
        self.loop = loop
        self.engine = engine
        self.tracker = shadow_tracker
        self.last_ts_ms = 0

    async def run(self, tick_stream: AsyncGenerator):
        async for tick in tick_stream:
            if self.last_ts_ms == 0:
                self.last_ts_ms = tick.timestamp_ms
                continue
            
            # 1. Вычисляем дельту времени
            delta_sec = (tick.timestamp_ms - self.last_ts_ms) / 1000.0
            
            # 2. Прыжок во времени в асинхронном пространстве (Сдвигаем asyncio таймеры)
            self.loop.advance_time(delta_sec)
            
            # 3. Боевая обработка тика
            await self.engine.process_tick(
                symbol=tick.symbol,
                price=tick.price,
                qty=tick.qty,
                ts_ms=tick.timestamp_ms
            )
            
            # 4. Обновление теневого трекера
            await self.tracker.apply_market_tick(
                symbol=tick.symbol,
                price=tick.price,
                ts_ms=tick.timestamp_ms
            )
            
            self.last_ts_ms = tick.timestamp_ms
