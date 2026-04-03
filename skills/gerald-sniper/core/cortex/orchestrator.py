import asyncio
import time
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Any, Set
from loguru import logger

class LiquidityTier(IntEnum):
    S = 1
    A = 2
    B = 3
    C = 4

@dataclass(slots=True, frozen=True)
class SignalEvent:
    event_id: str
    symbol: str
    tier: LiquidityTier
    confidence: float
    timestamp_ms: int
    direction: str
    suggested_price: float
    is_exit_order: bool = False
    stop_loss: float = 0.0
    attempt: int = 0  # Retry counter

    def copy_for_retry(self, attempt: int) -> 'SignalEvent':
        return SignalEvent(
            event_id=self.event_id,
            symbol=self.symbol,
            tier=self.tier,
            confidence=self.confidence,
            timestamp_ms=self.timestamp_ms, # Оригинальное время сохраняется
            direction=self.direction,
            suggested_price=self.suggested_price,
            is_exit_order=self.is_exit_order,
            stop_loss=self.stop_loss,
            attempt=attempt
        )

@dataclass(order=True, slots=True)
class PrioritizedTask:
    priority: int
    confidence_inverted: float
    sequence: int
    event: SignalEvent = field(compare=False)

class TacticalOrchestrator:
    def __init__(
        self, 
        event_bus: Any,
        guard: Any,
        portfolio: Any,
        engine: Any,
        max_workers: int = 5,
        max_queue_size: int = 100,
        max_signal_age_ms: int = 500
    ):
        self.bus = event_bus
        self.guard = guard
        self.portfolio = portfolio
        self.engine = engine
        
        self.qos_queue: asyncio.PriorityQueue[PrioritizedTask] = asyncio.PriorityQueue(maxsize=max_queue_size)
        self.max_workers = max_workers
        self._workers = []
        self._sequence = 0
        self.max_signal_age_ms = max_signal_age_ms
        self._background_tasks: Set[asyncio.Task] = set()

    def _fire_and_forget(self, coro) -> None:
        """Безопасный запуск фоновых задач с защитой от Garbage Collector'а."""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def boot_sequence(self) -> None:
        logger.info(f"🧠 [CORTEX] Инициализация Pydantic-Core. Workers: {self.max_workers}")
        for i in range(self.max_workers):
            self._workers.append(asyncio.create_task(self._cortex_worker(i)))
            
        if hasattr(self.bus, "start_consuming"):
            await self.bus.start_consuming(self.ingest_to_qos)

    def ingest_to_qos(self, event: SignalEvent) -> None:
        self._sequence += 1
        conf_inverted = round(1.0 - event.confidence, 4)
        
        task = PrioritizedTask(
            priority=event.tier.value,
            confidence_inverted=conf_inverted,
            sequence=self._sequence,
            event=event
        )
        
        try:
            self.qos_queue.put_nowait(task)
        except asyncio.QueueFull:
            if event.tier.value <= LiquidityTier.A.value:
                logger.error(f"☢️ [CORTEX/QoS] DROPPED HIGH TIER SIGNAL {event.symbol}. QUEUE FULL!")
            else:
                logger.debug(f"🗑️ [CORTEX/QoS] Shedding load: dropped {event.symbol}")

    async def _cortex_worker(self, worker_id: int) -> None:
        while True:
            try:
                task: PrioritizedTask = await self.qos_queue.get()
                event = task.event
                
                # СТРОГИЙ TTL
                current_time = int(time.time() * 1000)
                age = current_time - event.timestamp_ms
                
                # Bypass TTL для Exit Orders (их нужно исполнить любой ценой)
                if age > self.max_signal_age_ms and not event.is_exit_order:
                    logger.warning(f"🔥 [INCINERATOR] Сигнал {event.symbol} протух (Age: {age}ms). Alpha lost.")
                    self.qos_queue.task_done()
                    if hasattr(self.bus, "ack"): await self.bus.ack(event)
                    continue
                    
                await self._process_signal_pipeline(event, worker_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"🔥 [CORTEX/Worker-{worker_id}] Необработанный сбой: {e}")
            finally:
                self.qos_queue.task_done()

    async def _process_signal_pipeline(self, event: SignalEvent, worker_id: int) -> None:
        symbol = event.symbol
        logger.debug(f"⚙️ [CORTEX/Worker-{worker_id}] Обработка {event.direction} {symbol}")

        if hasattr(self.guard, "intercept") and not await self.guard.intercept(event):
            logger.info(f"🛡️ [CORTEX] Guard отклонил {symbol}.")
            if hasattr(self.bus, "ack"): await self.bus.ack(event)
            return

        try:
            validated_signal = await self.portfolio.allocate_and_size_position(
                event=event,
                entry_price=event.suggested_price,
                stop_loss_price=event.stop_loss
            )
        except Exception as e:
            logger.error(f"❌ [CORTEX] Ошибка Risk/Portfolio для {symbol}: {e}")
            return

        if not validated_signal:
            return

        try:
            await self.engine.process_signal(validated_signal)
            logger.info(f"🚀 [CORTEX] Sign-off: {symbol} ордер отправлен.")
            if hasattr(self.bus, "ack"): await self.bus.ack(event)
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Оптимистичное освобождение маржи при любом локальном API сбое
            margin = getattr(validated_signal, 'allocated_margin', None)
            if margin is not None and hasattr(self.portfolio, "release_margin"):
                 await self.portfolio.release_margin(margin)
                 
            if "429" in error_str:
                if getattr(event, 'is_exit_order', False):
                    logger.warning(f"⚠️ [CORTEX/Worker-{worker_id}] 429 на EXIT-ордере {symbol}. Retry Protocol.")
                    self._fire_and_forget(self._delayed_retry_exit(event))
                else:
                    logger.error(f"🔥 [CORTEX] 429 Rate Limit на ENTRY {symbol}. Сигнал сожжен (рынок ушел).")
                    if hasattr(self.bus, "ack"): await self.bus.ack(event)
                    
            elif "500" in error_str or "502" in error_str:
                logger.critical(f"☢️ [CORTEX/Worker-{worker_id}] HTTP 500: Schrödinger's Order на {symbol}!")
                # Делегируем сверку фантомного ордера Инквизитору через fire_and_forget
                self._fire_and_forget(self._audit_phantom_order(event.event_id, symbol))
                if hasattr(self.bus, "ack"): await self.bus.ack(event)
                
            else:
                logger.error(f"🔥 [CORTEX] Сбой I/O {symbol}: {e}")
                if hasattr(self.bus, "ack"): await self.bus.ack(event)

    async def _delayed_retry_exit(self, event: SignalEvent) -> None:
        """Retry механизм исключительно для Экзит-Ордеров."""
        attempt = event.attempt + 1
        if attempt > 3:
            logger.critical(f"☠️ [CORTEX] Выход {event.symbol} провалился 3 раза. Panic Protocol Action Required!")
            # ЗДЕСЬ ДОЛЖЕН БЫТЬ ВЫЗОВ KILL_SWITCH ИЛИ ПАНИК-РЕКОНСАЙЛА
            if hasattr(self.bus, "ack"): await self.bus.ack(event)
            return
            
        retry_delay = 0.5 * (2 ** attempt)
        await asyncio.sleep(retry_delay)
        
        event_copy = event.copy_for_retry(attempt=attempt)
        logger.info(f"🔄 [CORTEX] Re-ingest EXIT {event.symbol} (Attempt {attempt})")
        self.ingest_to_qos(event_copy)

    async def _audit_phantom_order(self, order_link_id: str, symbol: str) -> None:
        """Делегирование проверки стейта в StateReconciler"""
        logger.warning(f"🔍 [STATE AUDIT] Запрашиваем Reconciler для проверки фантомного стейта {order_link_id} | {symbol}")
        # Заглушка: StateReconciler.verify_and_recover(order_link_id, symbol) будет вызвана
        # from core.shield.reconciler import state_reconciler
        # await state_reconciler.verify_and_recover(order_link_id, symbol)

