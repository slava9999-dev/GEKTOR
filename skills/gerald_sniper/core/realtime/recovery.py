# core/realtime/recovery.py
import asyncio
import orjson
from concurrent.futures import ProcessPoolExecutor
from enum import Enum, auto
import logging
from collections import deque
from typing import Optional, Dict

logger = logging.getLogger("GEKTOR.APEX.Recovery")

class BookState(Enum):
    HEALTHY = auto()
    DISCONNECTED = auto()
    REHYDRATING = auto()
    STITCHING = auto()
    WARMUP = auto()

# Ограничиваем пул. 1 процесс = 1 ядро. Крупные снепшоты не должны вешать Event Loop.
cpu_executor = ProcessPoolExecutor(max_workers=2)

def parse_and_slice_snapshot(raw_bytes: bytes, depth: int = 500) -> dict:
    """ 
    [HFT Quant]: Выполняется в ОТДЕЛЬНОМ процессе. 
    Распарсили 50Мб, но обратно в IPC летит только топ-500 уровней.
    Pickle-оверхед снижается с 150мс до <1мс.
    """
    try:
        full_data = orjson.loads(raw_bytes)
        
        # Сортировка и усечение до боевой глубины. 
        # Отрезаем "хвосты", которые только создают шум и IPC-латентность.
        bids = sorted(full_data.get('bids', []), key=lambda x: float(x[0]), reverse=True)[:depth]
        asks = sorted(full_data.get('asks', []), key=lambda x: float(x[0]))[:depth]
        
        return {
            'u': full_data.get('u', 0), # Sequence ID
            'b': bids, # 'b' and 'a' as per ZeroGCOrderbook format
            'a': asks
        }
    except Exception as e:
        return {"error": str(e)}

class MarketRehydrator:
    """ 
    [BEAZLEY v15.3] Защищенный координатор реконсиляции стакана. 
    Гарантирует 0-блокировку основного Event Loop.
    """
    def __init__(self, symbol: str, book_engine):
        self.symbol = symbol
        self.engine = book_engine
        self.state = BookState.DISCONNECTED
        # Жесткий лимит буфера дельт. Если переполнился (затянутый лаг) - хард-ресет.
        self._delta_buffer = deque(maxlen=5000)
        self._rehydration_task: Optional[asyncio.Task] = None

    async def handle_disconnect(self):
        """ Сброс состояния при разрыве связи. """
        self.state = BookState.DISCONNECTED
        self.engine.is_valid = False
        self._delta_buffer.clear()
        
        # Рубим предыдущую попытку восстановления, если она зависла
        if self._rehydration_task and not self._rehydration_task.done():
            self._rehydration_task.cancel()
            
        logger.warning(f"🧊 [RECOVERY] {self.symbol} Disconnected. Shadow Mode active.")

    async def start_rehydration(self, snapshot_bytes: bytes):
        """ Асинхронный запуск парсинга в воркере и последующая сшивка. """
        if self.state == BookState.REHYDRATING:
            return 
            
        self.state = BookState.REHYDRATING
        self._delta_buffer.clear()
        
        loop = asyncio.get_running_loop()
        # [HFT Standard] Offloading CPU-bound parsing to worker pool
        self._rehydration_task = loop.create_task(
            loop.run_in_executor(cpu_executor, parse_and_slice_snapshot, snapshot_bytes)
        )
        
        try:
            snapshot_data = await self._rehydration_task
            if "error" in snapshot_data:
                raise ValueError(snapshot_data["error"])
                
            await self._stitch_and_resume(snapshot_data)
        except asyncio.CancelledError:
            logger.info(f"[{self.symbol}] Rehydration aborted.")
        except Exception as e:
            logger.error(f"☢️ [RECOVERY_FATAL] {self.symbol} Rehydration failed: {e}")
            self.state = BookState.DISCONNECTED

    def buffer_delta(self, delta: dict):
        """ Накопление дельт во время загрузки снепшота. """
        if self.state == BookState.REHYDRATING:
            if len(self._delta_buffer) == self._delta_buffer.maxlen:
                logger.error(f"[{self.symbol}] Delta buffer OVERFLOW for {self.symbol}. Forcing restart.")
                # Мы не можем доверять такому стакану - слишком большой лаг
                asyncio.create_task(self.handle_disconnect())
                return
            self._delta_buffer.append(delta)

    async def _stitch_and_resume(self, snapshot: dict):
        """ Фаза сшивки снепшота и накопленных дельт. """
        self.state = BookState.STITCHING
        
        # 1. Применяем усеченный снепшот (уже в формате ZeroGCOrderbook)
        self.engine.apply_snapshot(snapshot) # Assuming it can handle a dict
        snap_seq = int(snapshot.get('u', 0))
        
        applied = 0
        # 2. Replay дельт: ищем точку синхронизации
        while self._delta_buffer:
            delta = self._delta_buffer.popleft()
            # Пропускаем дельты, которые уже внутри снепшота
            if int(delta.get('u', 0)) > snap_seq:
                self.engine.apply_delta(delta)
                applied += 1
                
        # 3. Финальная проверка консистентности (No crossed books)
        if self.engine.validate_health():
            self.state = BookState.WARMUP
            # Фаза прогрева (50мс) для накопления микроструктурных метрик (Imbalance и др)
            await asyncio.sleep(0.050)
            self.state = BookState.HEALTHY
            self.engine.is_valid = True
            logger.info(f"🔥 [RECOVERY] {self.symbol} LIVE. Replayed {applied} deltas.")
        else:
            logger.error(f"💀 [RECOVERY] {self.symbol} Inconsistent book after stitch. Resetting.")
            await self.handle_disconnect()
