# src/infrastructure/event_bus.py
import asyncio
import time
from typing import Set, Any, Callable, Dict, List, Awaitable, Optional, Union
from loguru import logger
from src.domain.entities.events import ExecutionEvent, ConflatedEvent, StateInvalidationEvent

class EventBus:
    """
    [GEKTOR APEX] Institutional Event Bus v2.2.
    Architecture: Producer-Consumer with Explicit Invalidation.
    
    Safety Protocol:
    1. Causality Tracking: Dropped ticks trigger immediate State Corruption.
    2. Atomic Invalidation: Corrupted symbols are blocked until REST Re-Hydration.
    3. Emergency Escalation: High Watermark triggers both Conflation and Invalidation.
    """
    def __init__(self, max_queue_size: int = 5000):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._background_tasks: Set[asyncio.Task[Any]] = set()
        self._consumer_task: Optional[asyncio.Task] = None
        
        # [CONFLATION & CORRUPTION]
        self._high_watermark = max_queue_size * 0.8
        self._corrupted_symbols: Set[str] = set()
        self._subscribers: Dict[str, List[Callable[[Any], Awaitable[None]]]] = {}
        self._running = False

    async def start(self):
        if not self._consumer_task:
            self._running = True
            self._consumer_task = asyncio.create_task(self._consume_loop())
            logger.info("🎛️ [EventBus] Resilience Core v2.2 ARMED. Causality tracking active.")

    def subscribe(self, event_name: str, callback: Callable[[Any], Awaitable[None]]):
        if event_name not in self._subscribers:
            self._subscribers[event_name] = []
        self._subscribers[event_name].append(callback)

    def publish_fire_and_forget(self, event: Any):
        """
        Умный диспетчер задач. Гарантирует неблокирующее исполнение.
        """
        if asyncio.iscoroutine(event):
            task = asyncio.create_task(event)
            task.add_done_callback(self._task_error_handler)
            return
        elif callable(event):
            event()
            return

        symbol = getattr(event, 'symbol', 'UNKNOWN')
        
        # [CORRUPTION SHIELD] Ignore incoming data for symbols that lost causality
        if symbol in self._corrupted_symbols:
            return

        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            # [STATE CORRUPTION PROTOCOL]
            self._corrupted_symbols.add(symbol)
            logger.critical(f"🚨 [BACKPRESSURE] Queue Full. Causality broken for {symbol}. INVALIDATING STATE.")
            
            invalidation = StateInvalidationEvent(
                symbol=symbol,
                reason="QUEUE_FULL_TICK_DROPPED"
            )
            # Direct Invalidation (Bypass Queue)
            self._dispatch_task(self._publish_atomic(invalidation))
            
            # [VOIP ESCALATION] handled by subscribers (Orchestrator/RiskManager)

    def mark_recovered(self, symbol: str):
        """Restores causality for a symbol after successful REST re-hydration."""
        if symbol in self._corrupted_symbols:
            self._corrupted_symbols.remove(symbol)
            logger.success(f"🔄 [EventBus] {symbol} recovered. Pipeline visibility restored.")

    async def _consume_loop(self):
        while self._running:
            try:
                event = await self._queue.get()
                qsize = self._queue.qsize()
                
                if qsize > self._high_watermark:
                    await self._drain_and_conflate(event, qsize)
                else:
                    self._dispatch_task(self._publish_atomic(event))
                
                self._queue.task_done()
            except asyncio.CancelledError: break
            except Exception as e:
                logger.error(f"⚠️ [EventBus] Consumer Error: {e}")
                await asyncio.sleep(0.1)

    async def _drain_and_conflate(self, first_event: Any, items_to_drain: int):
        # ... logic for ExecutionEvent conflation remains, but with Invalidation check ...
        if not isinstance(first_event, ExecutionEvent):
            self._dispatch_task(self._publish_atomic(first_event))
            return

        buffer: Dict[str, ConflatedEvent] = {}
        
        def add_to_buffer(evt: Union[ExecutionEvent, Any]):
            if not isinstance(evt, ExecutionEvent):
                self._dispatch_task(self._publish_atomic(evt))
                return
            
            # If causality is broken during drain, invalidate and skip
            if evt.symbol in self._corrupted_symbols: return

            key = f"{evt.symbol}_{evt.side}"
            if key not in buffer:
                buffer[key] = ConflatedEvent(
                    symbol=evt.symbol, side=evt.side, total_volume=0.0,
                    tick_count=0, duration_ms=0.0, start_ts=evt.timestamp,
                    end_ts=evt.timestamp, avg_price=evt.price
                )
            
            c = buffer[key]
            c.total_volume += evt.volume
            c.tick_count += 1
            c.end_ts = evt.timestamp
            c.duration_ms = (c.end_ts - c.start_ts) * 1000.0
            c.avg_price = (c.avg_price + evt.price) / 2.0

        add_to_buffer(first_event)
        for _ in range(items_to_drain):
            try:
                evt = self._queue.get_nowait()
                add_to_buffer(evt)
                self._queue.task_done()
            except asyncio.QueueEmpty: break
        
        for mega_event in buffer.values():
            self._dispatch_task(self._publish_atomic(mega_event))

    async def _publish_atomic(self, event: Any):
        event_name = type(event).__name__
        if event_name not in self._subscribers: return
        handlers = [cb(event) for cb in self._subscribers[event_name]]
        if handlers: await asyncio.gather(*handlers, return_exceptions=True)

    def _dispatch_task(self, coro: Awaitable):
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _task_error_handler(self, task: asyncio.Task):
        self._background_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.critical(f"💥 [EventBus] Background task crashed: {e}")

    async def stop(self):
        self._running = False
        if self._consumer_task: self._consumer_task.cancel()
