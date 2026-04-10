import asyncio
import time
import os
import orjson
import websockets
from typing import List, Set, Any, Optional
from loguru import logger
from datetime import datetime
from core.events.nerve_center import bus
from core.events.events import PriceUpdateEvent, RawWSEvent
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK
import concurrent.futures

class VpnTolerantClockSync:
    """
    Клеппман & Quant: Адаптивный компенсатор времени с устойчивостью к Bufferbloat (VPN).
    Разделяет аппаратный рассинхрон и сетевую задержку туннеля.
    """
    def __init__(self, ewma_alpha: float = 0.05, hard_limit_ms: float = 15000.0):
        # hard_limit_ms увеличен до 15 секунд для выживания за VPN
        self.ewma_alpha = ewma_alpha
        self.hard_limit_ms = hard_limit_ms
        self.current_lag: float = 0.0
        self.base_offset: float = 0.0
        self._is_anchored = False

    def anchor(self, exchange_server_time_ms: float, local_time_ms: float):
        # Фиксируем ТОЛЬКО разницу системных часов (ОС vs Биржа)
        self.base_offset = exchange_server_time_ms - local_time_ms
        self._is_anchored = True
        logger.info(f"⚓ [ClockSync] VPN Baseline Anchored. Static Offset: {self.base_offset:.2f}ms")

    def validate_and_update(self, packet_timestamp_ms: float, local_time_ms: float) -> bool:
        if not self._is_anchored:
            self.anchor(packet_timestamp_ms, local_time_ms)
            return True

        # Нормализуем локальное время под часовой пояс/систему биржи
        normalized_local = local_time_ms + self.base_offset
        
        # lag_ms - это чистая транспортная задержка (VPN Bufferbloat)
        lag_ms = normalized_local - packet_timestamp_ms

        # Сглаживаем шум VPN через EWMA
        self.current_lag = (self.ewma_alpha * lag_ms) + ((1 - self.ewma_alpha) * self.current_lag)

        if abs(lag_ms) > self.hard_limit_ms:
            # Сбрасываем якорь ТОЛЬКО если лаг превысил 15 секунд (полный разрыв туннеля)
            logger.critical(f"⏰ [StaleDrop] VPN Tunnel collapsed. Lag: {lag_ms:.2f}ms. RE-ANCHORING.")
            self._is_anchored = False
            return False
            
        if lag_ms > 2000.0:
            # Логируем в DEBUG, чтобы не засорять консоль, мы ЗНАЕМ что работаем через VPN
            logger.debug(f"🐢 [VPN Lag] Packet delayed by {lag_ms:.1f}ms. Absorbed by Conflator.")

        return True


class WSWorker:
    """
    Independent WebSocket Worker (Task 6.2 & 6.5).
    Handles dynamic subscriptions and sequence gap detection.
    """
    def __init__(self, worker_id: int, stream_url: str, max_topics: int = 50):
        self.worker_id = worker_id
        self.stream_url = stream_url
        self.max_topics = max_topics
        self.active_topics: Set[str] = set()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._last_u = {} 
        self._subscription_queue = asyncio.Queue()
        self._payload_queue = asyncio.Queue(maxsize=100000) # Isolation Buffer
        self.last_msg_ts = time.time() # [Audit 5.15] Zombie Socket Guard
        self.last_pong_time = time.time()
        self.bridge = None # Done in run()
        self.publish_to_bus = True 
        
        # Clock Sync (VPN Tolerant)
        self.clock_sync = VpnTolerantClockSync(ewma_alpha=0.05, hard_limit_ms=15000.0)
        
        # [GEKTOR v14.3] Pre-initialized buffer for bus publication 
        # (Using asyncio.Queue for decoupling if bus is slow, or direct create_task)
        self._publish_queue = asyncio.Queue(maxsize=10000)

    async def run(self):
        from .bridge_flusher import DataPipelineBridge
        # [Audit 27.2] Strict Dependency Injection for Redis
        redis_url = os.getenv("REDIS_URL")
        self.bridge = DataPipelineBridge(redis_url=redis_url, batch_size=200)
        try:
            await self.bridge.start()
        except Exception as e:
            logger.critical(f"🛑 [Worker {self.worker_id}] KILLED due to bridge failure: {e}")
            return # Suicide on infrastructure failure
        
        self._running = True
        _disconnect_ts = 0.0
        _reconnect_count = 0
        _reconnect_delay = 1.0
        _max_delay = 30.0
        
        # [Audit 5.15] Process signals directly or via light queue
        # No more ThreadPool - orjson is fast enough for the Event Loop.
        publish_task = asyncio.create_task(self._publish_dispatcher())
        
        while self._running:
            try:
                import socket
                proxy = None # [GEKTOR] Force DIRECT connection for BYBIT WebSocket (Proxy causes EOFError/Connection resets)
                
                async with websockets.connect(
                    self.stream_url, 
                    ping_interval=20.0,    # Пульс каждые 20 сек
                    ping_timeout=10.0,     # Если нет ответа 10 сек -> рвем
                    close_timeout=5.0,     # Защита от зависания при закрытии
                    max_size=2**24,        # 16 MB buffer
                    max_queue=2048,        # Защита от переполнения буфера
                    proxy=proxy,
                    additional_headers={"User-Agent": "Gerald-Sniper-HFT/5.15"},
                ) as ws:
                    # Enable Kernel-level KeepAlive
                    sock = ws.transport.get_extra_info('socket')
                    if sock:
                        # [HFT TUNING] TCP_NODELAY: Disable Nagle's Algorithm to prevent buffering jitter
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                        if hasattr(socket, "TCP_KEEPIDLE"):
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5)
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 1)
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                    
                    self._ws = ws
                    self.last_pong_time = time.time()
                    logger.success(f"🔌 [Worker {self.worker_id}] Connected to {self.stream_url}")
                    _reconnect_delay = 1.0  # Сброс backoff
                    
                    if _disconnect_ts > 0:
                        _reconnect_count += 1
                        downtime = time.time() - _disconnect_ts
                        _disconnect_ts = 0.0
                        try:
                            from core.events.events import ConnectionRestoredEvent
                            asyncio.create_task(bus.publish(ConnectionRestoredEvent(
                                source=f"WSWorker_{self.worker_id}",
                                downtime_seconds=downtime,
                                reconnect_count=_reconnect_count,
                            )))
                        except Exception as e:
                            logger.error(f"❌ [Worker {self.worker_id}] Failed to emit reconnect event: {e}")
                    
                    if self.active_topics:
                        await self._subscribe(list(self.active_topics))
                    
                    sub_handler = asyncio.create_task(self._handler_subscription_updates())
                    heartbeat_handler = asyncio.create_task(self._heartbeat_watchdog())
                    
                    try:
                        async for msg in ws:
                            # 1. Non-blocking Bridge Ingestion (Shard path)
                            self.bridge.ingest_raw(msg)
                            self.last_msg_ts = time.time()
                            
                            # 2. FAST PATH: Synchronous parse & DTO factory
                            # [Audit 14.3] No ThreadPool context switching overhead.
                            try:
                                payload = orjson.loads(msg)
                                await self._handle_message(payload)
                            except orjson.JSONDecodeError:
                                continue
                            except Exception as e:
                                logger.error(f"Hot Path Error: {e}")
                    except (ConnectionClosedError, ConnectionClosedOK) as e:
                        logger.warning(f"⚠️ [Worker {self.worker_id}] WebSocket closed gracefully/expectedly: {e.code} - {e.reason}")
                    except (asyncio.exceptions.IncompleteReadError, EOFError) as e:
                        logger.error(f"💀 [Worker {self.worker_id}] Proactor/OS Level Drop: {e}")
                    except Exception as e:
                        logger.exception(f"🔥 [Worker {self.worker_id}] Critical Transport Failure: {e}")
                    finally:
                        sub_handler.cancel()
                        heartbeat_handler.cancel()
                        self._ws = None
                        
            except Exception as e:
                logger.error(f"❌ [Worker {self.worker_id}] Connection error: {e}. Reconnecting in {_reconnect_delay}s...")
                if _disconnect_ts == 0.0:
                    _disconnect_ts = time.time()
                await asyncio.sleep(_reconnect_delay)
                _reconnect_delay = min(_reconnect_delay * 2, _max_delay)
        
        publish_task.cancel()
        await self.bridge.stop()

    async def _heartbeat_watchdog(self):
        """
        [GEKTOR v14.3] Advanced Zombie Socket Watchdog.
        Checks for:
        1. Pong failures (Pings are sent, but pongs didn't arrive).
        2. Event Loop Lag (Actual sleep vs Expected sleep).
        3. Message Silence.
        4. Buffer Skew (Exchange TS vs Local TS).
        """
        while self._running:
            start_time = time.monotonic()
            await asyncio.sleep(2) 
            actual_sleep = time.monotonic() - start_time
            
            # Detect Event Loop starvation (High I/O Load Guard)
            if actual_sleep > 5.0:
                logger.warning(f"⚠️ [Worker {self.worker_id}] Event Loop starving! Sleep(2s) took {actual_sleep:.2f}s")
            
            if not self._ws: continue
            
            try:
                now = time.time() # time.time() is ok here for the absolute delta vs last_pong/msg
                # But for pong/starvation measurement within the loop, monotonic is better.
                # Actually last_pong_time etc are set with time.time(). Let's stick to time.time()
                # for the absolute timestamps but keep monotonic for the sleep measurement.
                
                # 1. Active Probe
                await self._ws.send('{"op":"ping"}')
                
                # 2. Pong Timeout Check (Critical for HFT safety)
                time_since_last_pong = now - self.last_pong_time
                if time_since_last_pong > 15.0:
                    logger.critical(f"💀 [Worker {self.worker_id}] Pong Timeout ({time_since_last_pong:.1f}s). Resetting socket.")
                    await self._ws.close()
                    continue

                # 3. Silent Drop Check
                silence_duration = now - self.last_msg_ts
                if silence_duration > 10.0:
                    logger.critical(f"💀 [Worker {self.worker_id}] Zombie Socket (10s Silence).")
                    await self._ws.close()
                    continue

                # 4. Global Staleness Check (Strategy Level Safety)
                from core.realtime.market_state import market_state
                if market_state.check_global_staleness(threshold=5.0):
                    logger.critical(f"💀 [Worker {self.worker_id}] Global Market Staleness. Resetting.")
                    await self._ws.close()
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def update_topics(self, new_topics: Set[str]):
        """
        Calculates diff and queues subscription/unsubscription.
        """
        to_sub = new_topics - self.active_topics
        to_unsub = self.active_topics - new_topics
        
        if to_sub:
            await self._subscription_queue.put({"op": "subscribe", "args": list(to_sub)})
        if to_unsub:
            await self._subscription_queue.put({"op": "unsubscribe", "args": list(to_unsub)})
            
        self.active_topics = new_topics

    async def _handler_subscription_updates(self):
        """Background loop to process queue and send to WS."""
        while self._running:
            item = await self._subscription_queue.get()
            try:
                # v14.0+ uses State.OPEN instead of .open attribute
                from websockets.protocol import State
                if self._ws and self._ws.state == State.OPEN:
                    await self._ws.send(orjson.dumps(item))
                    logger.debug(f"📝 [Worker {self.worker_id}] Sent {item['op']} for {len(item['args'])} topics")
                else:
                    # Connection closed? Re-queue and wait
                    logger.warning(f"⏳ [Worker {self.worker_id}] WS closed. Re-queuing subscription update...")
                    await self._subscription_queue.put(item)
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"❌ [Worker {self.worker_id}] Failed to send subscription: {e}")
                await self._subscription_queue.put(item)
                await asyncio.sleep(1)
            finally:
                self._subscription_queue.task_done()

    async def _subscribe(self, topics: List[str]):
        if not self._ws: return
        msg = {"op": "subscribe", "args": topics}
        await self._ws.send(orjson.dumps(msg))
        logger.info(f"📝 [Worker {self.worker_id}] Subscribed to {len(topics)} topics")

    async def _publish_dispatcher(self):
        """ Consumes DTOs from queue and hits the bus/callbacks """
        while self._running:
            try:
                evt = await self._publish_queue.get()
                
                # Publish to bus
                if self.publish_to_bus:
                    asyncio.create_task(bus.publish(evt))
                
                # Local callback
                if hasattr(self, 'on_raw_event_cb') and self.on_raw_event_cb:
                    asyncio.create_task(self.on_raw_event_cb(evt))
                
                self._publish_queue.task_done()
            except Exception as e:
                logger.error(f"Publish dispatcher error: {e}")
                await asyncio.sleep(0.1)

    # _deserialization_worker REMOVED (orjson is fast enough for event loop)

    async def _handle_message(self, payload: Any):
        if isinstance(payload, list):
            # Bybit occasionally sends lists for welcome/auth status
            logger.debug(f"ℹ️ [Worker {self.worker_id}] List payload received. Skipping.")
            return

        # Audit 18.2: Support PONG for Heartbeat Watchdog
        op = payload.get("op")
        if op == "pong" or payload.get("ret_msg") == "pong":
            self.last_pong_time = time.time()
            return

        topic = payload.get("topic", "")
        data = payload.get("data")
        u = payload.get("u")
        ts = payload.get("ts") # Bybit V5 Top-level exchange TS (ms)
        msg_type = payload.get("type", "delta")
        
        is_snap = msg_type == "snapshot"
        
        # [GEKTOR v10.0] Adaptive Clock Sync Dropout (replaces hardcoded 1000ms delay block)
        if ts:
            if not self.clock_sync.validate_and_update(ts, time.time() * 1000):
                if not is_snap: # Never drop snapshots, they restore state
                    return
        
        # Sequence tracking
        if u is not None and topic:
            self.last_msg_ts = time.time() # [Audit 5.15] Data liveness
            
            if is_snap:
                self._last_u[topic] = u
            elif topic in self._last_u:
                last_u = self._last_u[topic]
                # BYBIT V5: orderbook.50 is monotonic, NOT +1
                if topic.startswith("orderbook"):
                    if u <= last_u:
                        logger.warning(f"🚨 [Worker {self.worker_id}] Out-of-order on {topic}: got {u}, last {last_u}")
                        # Don't fail immediately, just drop. market_state handles crossed book.
                        # Automatic Gap Recovery for L2 Book
                        asyncio.create_task(self.force_reconnect_topic(topic))
                else:
                    expected = last_u + 1
                    if u != expected:
                        # Gap detected (Rule 6.5)
                        logger.warning(f"🚨 [Worker {self.worker_id}] Gap on {topic}: expected {expected}, got {u}")
                        # Trigger full sub/unsub sequence to fix state
                        asyncio.create_task(self.force_reconnect_topic(topic))
            self._last_u[topic] = u

        # [GEKTOR v14.3.3] Zero-OOM Drop Policy with Rate-Limited Recovery
        # If the queue is full (congested consumers), we drop the oldest event.
        # Create immutable DTO and dispatch
        evt = RawWSEvent.from_bybit_payload(payload)
        
        # CRITICAL: For L2 Book (Deltas), a drop implies state corruption.
        try:
            self._publish_queue.put_nowait(evt)
        except asyncio.QueueFull:
            dropped = self._publish_queue.get_nowait()
            if dropped.topic.startswith("orderbook") and not dropped.is_snapshot:
                 logger.critical(f"💀 [Worker {self.worker_id}] Congestion: Evicted L2 update for {dropped.symbol}. Book Desynced.")
                 # [INSTITUTIONAL RECOVERY] Ratelimited, IDEMPOTENT recovery
                 from core.realtime.recovery import recovery_manager
                 asyncio.create_task(recovery_manager.trigger_recovery(dropped.symbol, self))
            
            # Re-queue the current event (LIFO pattern for fresh news prioritize)
            self._publish_queue.put_nowait(evt)


    async def force_reconnect_topic(self, topic: str):
        """Task 17.1: Granular Re-subscription for a single topic."""
        if not self._ws: return
        
        logger.warning(f"🔄 [Worker {self.worker_id}] Triggering Granular Re-sub for {topic}")
        # 1. Clear sequence state
        if topic in self._last_u:
            del self._last_u[topic]
            
        # 2. Queue Unsub -> Sub sequence
        await self._subscription_queue.put({"op": "unsubscribe", "args": [topic]})
        await self._subscription_queue.put({"op": "subscribe", "args": [topic]})


    def stop(self):
        self._running = False

if __name__ == "__main__":
    import os
    import yaml
    from dotenv import load_dotenv
    
    # 1. Load Environment (MUST be first)
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    env_path = os.path.join(PROJECT_ROOT, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)
        logger.info(f"📁 Loaded .env from {env_path}")
    else:
        logger.warning(f"⚠️ .env NOT FOUND at {env_path}")

    # 2. Load Config
    config_path = os.path.join(PROJECT_ROOT, "skills", "gerald-sniper", "config_sniper.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        conf = yaml.safe_load(f)
    
    # 2. Extract context
    ws_url = conf.get("bybit", {}).get("ws_url", "wss://stream.bybit.com/v5/public/linear")
    watchlist = conf.get("watchlist", ["BTCUSDT", "ETHUSDT"])
    
    # Add orderbook and trade topics for each symbol
    init_topics = set()
    for s in watchlist:
        init_topics.add(f"publicTrade.{s}")
        init_topics.add(f"orderbook.50.{s}")
    
    # 3. Boot Worker
    logger.info(f"🚀 [Standalone WSWorker] Starting with {len(watchlist)} symbols...")
    worker = WSWorker(worker_id=99, stream_url=ws_url)
    worker.active_topics = init_topics
    
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.warning("🛑 [WSWorker] Stopped by user.")
