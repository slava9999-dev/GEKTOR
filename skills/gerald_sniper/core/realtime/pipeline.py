import asyncio
import numpy as np
from loguru import logger
from typing import Callable, Any, Tuple

class ZeroAllocBuffer:
    """
    [GEKTOR v13.0] Pre-allocated Numpy Circular Buffer.
    Ensures O(1) ingestion without GC pressure or memory fragmentation.
    """
    def __init__(self, window_size: int, num_assets: int):
        self.window_size = window_size
        self.num_assets = num_assets
        # Аллоцируем матрицу один раз при старте
        self.matrix = np.full((window_size, num_assets), np.nan, dtype=np.float64)
        self.cursor = 0
        self.is_full = False

    def append(self, row: np.ndarray):
        """ Вставка новой строки по кругу. Zero-allocation. """
        self.matrix[self.cursor] = row
        self.cursor = (self.cursor + 1) % self.window_size
        if self.cursor == 0:
            self.is_full = True

    def get_ordered_view(self) -> np.ndarray:
        """ 
        Возвращает матрицу в правильном временном порядке.
        Использует np.roll для выравнивания (редкая операция перед расчетом).
        """
        if not self.is_full:
            return self.matrix[:self.cursor]
        return np.roll(self.matrix, -self.cursor, axis=0)

class ResilientBackgroundWorker:
    """
    [GEKTOR v13.0] Non-blocking I/O Pipeline.
    Isolates "dirty" tasks (disk, system sounds, logs) from the main Event Loop.
    Uses a Producer-Consumer pattern to guarantee O(1) dispatch from Radar.
    """
    def __init__(self, queue_size: int = 100):
        self._queue = asyncio.Queue(maxsize=queue_size)
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def start(self):
        if self._task: return
        self._task = asyncio.create_task(self._run_loop())
        logger.info("📡 [IO WORKER] Pipeline v13.0 ACTIVE.")

    async def _run_loop(self):
        while not self._stop_event.is_set():
            func, args, kwargs = await self._queue.get()
            try:
                # Делегируем в пул потоков только внутри воркера, 
                # освобождая основной Event Loop мгновенно через submit_task.
                await asyncio.to_thread(func, *args, **kwargs)
            except Exception as e:
                logger.error(f"❌ [IO WORKER] Task failed: {e}")
            finally:
                self._queue.task_done()

    def submit_task(self, func: Callable, *args, **kwargs):
        """ Мгновенная неблокирующая отдача задачи в очередь. """
        try:
            self._queue.put_nowait((func, args, kwargs))
        except asyncio.QueueFull:
            # Если очередь забита, мы в критическом перегрузе
            logger.critical("🚨 [IO OVERLOAD] Background Queue is FULL! Dropping task.")

    async def stop(self):
        self._stop_event.set()
        if self._task:
            await self._task

# Global instances for v13.0 APEX
io_worker = ResilientBackgroundWorker()
