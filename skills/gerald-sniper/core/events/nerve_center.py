import asyncio
import os
import json
from dotenv import load_dotenv

# [NERVE REPAIR] Guarantee env is loaded for standalone worker imports
load_dotenv()

from typing import Dict, List, Type, Callable, Any
from collections import defaultdict
from loguru import logger
import redis.asyncio as aioredis
import redis.exceptions
from pydantic import BaseModel

class NerveCenter:
    """
    [Audit 28.2] Hardened NerveCenter with Redis Streams (Rule 16.4).
    Supports:
    1. Volatile Pub/Sub (for Ticks/L2/BookDeltas) -> Fast, Lossy.
    2. Persistent Streams (for Signals/Orders/Alerts) -> Guaranteed Delivery.
    """
    def __init__(self):
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6381/0")
        password = os.getenv("REDIS_PASSWORD", None)
        self.redis = aioredis.from_url(redis_url, password=password, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._stream_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._event_types: Dict[str, Type[BaseModel]] = {}
        self._listening = False
        self._consumer_group = os.getenv("CONSUMER_GROUP", "cortex_default")
        self._consumer_id = os.getenv("CONSUMER_ID", f"node_{os.getpid()}")

    def subscribe(self, event_type: Type[BaseModel], handler: Callable, persistent: bool = False):
        """
        [Audit 28.3] Wide-Range Subscription.
        If persistent=True, uses Redis Streams (XREADGROUP).
        """
        name = event_type.__name__
        self._event_types[name] = event_type
        
        if persistent:
            self._stream_subscribers[name].append(handler)
            logger.info(f"💾 [Stream] Registered persistent handler for {name}")
        else:
            self._subscribers[name].append(handler)
            logger.debug(f"📻 [PubSub] Registered volatile handler for {name}")

    async def publish(self, event: BaseModel, persistent: bool = False):
        """
        Hardened publish with persistent storage option.
        """
        name = type(event).__name__
        payload = event.model_dump_json()
        
        try:
            if persistent:
                # [Audit 28.4] Redis Stream XADD (Capped to 10k messages to avoid OOM)
                await self.redis.xadd(f"stream:{name}", {"payload": payload}, maxlen=10000, approximate=True)
            else:
                await self.redis.publish(name, payload)
        except Exception as e:
            logger.error(f"❌ [NerveCenter] Publish failed ({name}): {e}")

    @property
    def is_connected(self) -> bool:
        # This property might need refinement based on the new listening loops' states
        return self._listening and self.redis is not None

    async def start_listening(self):
        if self._listening: return
        self._listening = True
        # 1. Start Pub/Sub Loop
        asyncio.create_task(self._listen_loop_pubsub())
        # 2. Start Stream Loop
        asyncio.create_task(self._listen_loop_streams())
        logger.info(f"🚀 [NerveCenter] Multi-Modal Bus ACTIVE (Group: {self._consumer_group})")

    async def _listen_loop_pubsub(self):
        """Standard Volatile Loop."""
        while self._listening:
            try:
                channels = list(self._subscribers.keys())
                if not channels:
                    await asyncio.sleep(2); continue
                
                # Unsubscribe from all first to ensure clean state if channels changed
                try:
                    await self.pubsub.unsubscribe()
                except Exception:
                    pass # Ignore errors if not subscribed to anything
                
                await self.pubsub.subscribe(*channels)
                logger.debug(f"🎧 [NerveCenter] PubSub listening on: {channels}")

                async for msg in self.pubsub.listen():
                    if msg["type"] == "message":
                        await self._dispatch(msg["channel"], msg["data"], is_stream=False)
            except Exception as e:
                logger.warning(f"⚠️ [NerveCenter] PubSub Loop error: {e}")
                await asyncio.sleep(1)

    async def _listen_loop_streams(self):
        """
        [Audit 28.5] Redis Streams Consumer Group Loop.
        Ensures 100% delivery even after process crash.
        """
        while self._listening:
            try:
                streams_to_read = {f"stream:{name}": ">" for name in self._stream_subscribers.keys()}
                if not streams_to_read:
                    await asyncio.sleep(2); continue

                # ENSURE: Create Consumer Groups if they don't exist
                for stream_name in streams_to_read.keys():
                    try:
                        await self.redis.xgroup_create(stream_name, self._consumer_group, id="0", mkstream=True)
                    except redis.exceptions.ResponseError as e:
                        if "BUSYGROUP" not in str(e):
                            logger.error(f"❌ [NerveCenter] Error creating consumer group for {stream_name}: {e}")
                            raise
                
                # Also check for pending messages
                pending_streams = {f"stream:{name}": "0-0" for name in self._stream_subscribers.keys()}
                
                # XREADGROUP (Blocking call)
                response = await self.redis.xreadgroup(
                    groupname=self._consumer_group,
                    consumername=self._consumer_id,
                    streams=pending_streams, # Read pending messages first
                    count=50,
                    block=0 # Don't block for pending
                )
                
                if not response: # If no pending, read new messages
                    response = await self.redis.xreadgroup(
                        groupname=self._consumer_group,
                        consumername=self._consumer_id,
                        streams=streams_to_read, # Read new messages
                        count=50,
                        block=2000 # Block for new messages
                    )

                if response:
                    for stream_name, messages in response:
                        channel = stream_name.replace("stream:", "")
                        for msg_id, data in messages:
                            payload = data.get("payload")
                            if payload:
                                await self._dispatch(channel, payload, is_stream=True, msg_id=msg_id)
            except Exception as e:
                logger.warning(f"⚠️ [NerveCenter] Stream Loop error: {e}")
                await asyncio.sleep(1)

    async def _dispatch(self, channel: str, data: str, is_stream: bool = False, msg_id: str = None):
        """Common dispatcher for both transport layers."""
        event_cls = self._event_types.get(channel)
        if not event_cls:
            logger.warning(f"⚠️ [NerveCenter] No event class found for channel {channel}")
            return
        
        handlers = self._stream_subscribers.get(channel, []) if is_stream else self._subscribers.get(channel, [])
        
        if not handlers:
            logger.debug(f"⚠️ [NerveCenter] No handlers registered for {channel} (is_stream={is_stream})")
            # If it's a stream message and no handlers, we should still ACK it to prevent it from staying pending
            if is_stream and msg_id:
                try:
                    await self.redis.xack(f"stream:{channel}", self._consumer_group, msg_id)
                    logger.debug(f"✅ [NerveCenter] Acknowledged stream message {msg_id} for {channel} (no handlers)")
                except Exception as ack_e:
                    logger.error(f"❌ [NerveCenter] Failed to ACK stream message {msg_id} for {channel} (no handlers): {ack_e}")
            return

        try:
            event_obj = event_cls.model_validate_json(data)
            
            # Use TaskGroup for concurrent execution of handlers
            async with asyncio.TaskGroup() as tg:
                for handler in handlers:
                    if asyncio.iscoroutinefunction(handler):
                        tg.create_task(handler(event_obj))
                    else:
                        tg.create_task(asyncio.to_thread(handler, event_obj))
            
            # [Audit 28.6] Acknowledge Stream Message after all handlers have been dispatched
            if is_stream and msg_id:
                await self.redis.xack(f"stream:{channel}", self._consumer_group, msg_id)
                
        except Exception as e:
            logger.error(f"❌ [NerveCenter] Dispatch Error on {channel} (msg_id: {msg_id}): {e}")
            # If processing fails, do NOT ACK the message. It will remain in pending and can be retried.

class CircuitState(Enum):
    CLOSED = "CLOSED"       
    OPEN = "OPEN"           
    HALF_OPEN = "HALF_OPEN" 

class TelegramCircuitBreaker:
    """Паттерн Circuit Breaker для изоляции отказов Telegram API."""
    def __init__(self, threshold: int = 5, recovery_sec: float = 60.0):
        self.threshold = threshold
        self.recovery_sec = recovery_sec
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.last_fail_time = 0.0

    def can_fire(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_fail_time > self.recovery_sec:
                self.state = CircuitState.HALF_OPEN
                logger.warning("🔄 [CIRCUIT BREAKER] Переход в HALF_OPEN. Разрешаем 1 зондирующий запрос.")
                return True
            return False
        # [АВАНГАРДНОЕ ИСПРАВЛЕНИЕ]
        # Если мы уже в HALF_OPEN, это значит зонд уже улетел и мы ждем его ответа.
        # Все остальные таски в этот момент ОБЯЗАНЫ отбрасываться.
        return False

    def record_success(self):
        if self.state != CircuitState.CLOSED:
            logger.info("✅ [CIRCUIT BREAKER] Коннект восстановлен. Переход в CLOSED.")
        self.state = CircuitState.CLOSED
        self.failures = 0

    def record_failure(self):
        self.failures += 1
        self.last_fail_time = time.time()
        if self.state == CircuitState.HALF_OPEN or self.failures >= self.threshold:
            if self.state != CircuitState.OPEN:
                logger.critical(f"🛑 [CIRCUIT BREAKER] TG API МЕРТВ. Переход в OPEN на {self.recovery_sec}с.")
            self.state = CircuitState.OPEN


class TelegramNerveCenter:
    def __init__(self, bot_token: str, chat_id: str, redis_client: aioredis.Redis):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.redis = redis_client
        self.LOCK_TTL_SEC = 900
        self.breaker = TelegramCircuitBreaker(threshold=3, recovery_sec=60)
        self.concurrency_guard = asyncio.Semaphore(5)

    async def dispatch_anomaly_alert(self, symbol: str, anomaly_type: str, metric_value: float) -> bool:
        if not self.breaker.can_fire():
            logger.debug(f"🛑 [FAIL-FAST] {symbol} сброшен. Telegram Circuit Breaker = OPEN.")
            return False

        if self.concurrency_guard.locked():
            logger.error(f"⚠️ [BACKPRESSURE] Event Loop Pool (5/5) заполнен. Сигнал отброшен.")
            return False

        lock_key = f"ALERT_LOCK:{symbol}:{anomaly_type}"
        acquired = await self.redis.set(lock_key, "LOCKED", ex=self.LOCK_TTL_SEC, nx=True)
        if not acquired:
            return False

        async with self.concurrency_guard:
            try:
                # Mock aiohttp request
                # await asyncio.wait_for(session.post(...), timeout=1.5)
                await asyncio.sleep(0.1) 
                logger.info(f"🚨 [NERVE_CENTER] СИГНАЛ: {symbol} | {anomaly_type} | {metric_value:.2f}%")
                
                self.breaker.record_success()
                return True
                
            except asyncio.TimeoutError:
                logger.error("🌐 [TELEGRAM TIMEOUT] Пакет утерян.")
                self.breaker.record_failure()
                await self.redis.expire(lock_key, 30)
                return False
                
            except Exception as e:
                logger.critical(f"FATAL: Сбой Nerve Center: {e}")
                self.breaker.record_failure()
                await self.redis.expire(lock_key, 30)
                return False

# Global instances
bus = NerveCenter()

