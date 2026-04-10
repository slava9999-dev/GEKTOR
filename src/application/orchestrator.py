# src/application/orchestrator.py
import asyncio
import time
import os
from typing import Dict, List, Optional, Any
from loguru import logger

from src.infrastructure.config import settings
from src.infrastructure.database import DatabaseManager
from src.infrastructure.telegram_notifier import TelegramRadarNotifier
from src.infrastructure.bybit import BybitIngestor
from src.application.sentinel import BlackoutSentinel
from src.application.pool_manager import WorkerPoolManager
from src.domain.math_core import process_ticks_subroutine

class TickBatcher:
    """[GEKTOR v2.0] IPC Optimization with Memory Cycle Management."""
    def __init__(self, symbol: str, pool_manager: WorkerPoolManager, on_results_callback):
        self.symbol = symbol
        self.pool_manager = pool_manager
        self.on_results = on_results_callback
        self.buffer = []
        self.max_batch = 500
        self.last_flush = time.time()
        self._lock = asyncio.Lock()
        self._flush_task = asyncio.create_task(self._timer_flush())

    async def add_tick(self, tick_dict: dict):
        async with self._lock:
            self.buffer.append(tick_dict)
            if len(self.buffer) >= self.max_batch:
                await self._flush_buffer()

    async def flush_pending_ticks(self):
        async with self._lock: await self._flush_buffer()

    async def _timer_flush(self):
        while True:
            await asyncio.sleep(0.05)
            if time.time() - self.last_flush >= 0.1:
                async with self._lock: await self._flush_buffer()

    async def _flush_buffer(self):
        if not self.buffer: return
        
        batch = self.buffer.copy()
        self.buffer.clear()
        self.last_flush = time.time()
        
        # 1. Получаем текущее состояние из оркестратора
        current_state = self.on_results("__GET_STATE__", self.symbol)
        
        try:
            # 2. Передаем состояние в пул
            result_pkg = await self.pool_manager.execute_batch(
                process_ticks_subroutine, 
                self.symbol, batch, settings.VPIN_BUCKET_VOLUME, current_state
            )
            
            if result_pkg:
                 # 3. Сохраняем обновленное состояние и обрабатываем результаты
                 self.on_results(result_pkg["results"], result_pkg["new_state"])
        except Exception as e:
            logger.error(f"💥 [Orchestrator] Worker Process Crash for {self.symbol}: {e}")

class GektorOrchestrator:
    """[GEKTOR APEX] CHIEF ARCHITECT Monolith v2.0."""
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        self.db = DatabaseManager()
        self.pool_manager = WorkerPoolManager(max_workers=os.cpu_count() or 4)
        
        # ИСТИННЫЙ ИСТОЧНИК ПАМЯТИ (State Ownership)
        self.macro_states: Dict[str, dict] = {}
        
        self.tg = TelegramRadarNotifier(
            bot_token=settings.TG_BOT_TOKEN or "",
            chat_id=settings.TG_CHAT_ID or "",
            proxy_url=os.getenv("PROXY_URL")
        )
        self.sentinel = BlackoutSentinel(self.tg._queue, self.tg._live_allowed)
        self.batchers: Dict[str, TickBatcher] = {}
        
        self.ingestor = BybitIngestor(
            symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            on_tick_callback=self.ingest_tick,
            alert_callback=self.send_critical_alert
        )

    async def start(self):
        logger.info("🔥 [CORE] Gektor APEX v2.0 Monolith starting...")
        await self.db.initialize()
        await self.tg.start()
        asyncio.create_task(self.sentinel.watch())
        asyncio.create_task(self.ingestor.run())
        logger.success("🚀 [CORE] System ARMED. Monitoring Event Bus.")

    def on_math_results(self, results: Any, new_state: Optional[dict] = None):
        """Callback для обработки результатов и управления стейтом."""
        # Специальный вызов для получения стейта батчером
        if results == "__GET_STATE__":
            return self.macro_states.get(new_state) # Здесь new_state используется как символ
            
        # 1. Сохраняем новую память
        if results and len(results) > 0:
            symbol = results[0]["symbol"]
            self.macro_states[symbol] = new_state

        # 2. Рассылаем алерты
        for res in results:
            if res.get("vpin") and res["vpin"] > 0.7:
                self.tg.notify_event({
                    "symbol": res.get("symbol", "BTCUSDT"),
                    "price": res["price"],
                    "vpin": res["vpin"],
                    "timestamp": res["timestamp"]
                })

    async def ingest_tick(self, symbol: str, tick_data: dict):
        if symbol not in self.batchers:
            self.batchers[symbol] = TickBatcher(symbol, self.pool_manager, self.on_math_results)
        await self.batchers[symbol].add_tick(tick_data)

    async def send_critical_alert(self, text: str):
        self.tg.notify_manual(text) # Assuming notify_manual in notifier

    async def stop(self):
        logger.warning("🔌 [CORE] Initiating shutdown...")
        self._shutdown_event.set()
        await self.ingestor.stop()
        tasks = [b.flush_pending_ticks() for b in self.batchers.values()]
        if tasks: await asyncio.gather(*tasks)
        self.pool_manager.shutdown_all()
        await self.db.close()
        await self.tg.stop()
        logger.info("💤 [CORE] Offline.")
