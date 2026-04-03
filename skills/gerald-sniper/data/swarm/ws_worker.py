import asyncio
import time
import orjson
import websockets
from typing import List, Set, Any, Optional
from loguru import logger
from datetime import datetime
from core.events.nerve_center import bus
from core.events.events import PriceUpdateEvent, RawWSEvent

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
        self.last_msg_ts = time.time() # [Audit 5.15] Zombie Socket Guard
        self.last_pong_time = time.time()
        self.bridge = None # Done in run()
        self.publish_to_bus = True 

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
        
        while self._running:
            try:
                import socket
                proxy = os.getenv("PROXY_URL")
                
                async with websockets.connect(
                    self.stream_url, 
                    ping_interval=20, 
                    ping_timeout=10,
                    close_timeout=5,
                    proxy=proxy,
                    additional_headers={"User-Agent": "Gerald-Sniper-HFT/5.15"},
                ) as ws:
                    # Enable Kernel-level KeepAlive
                    sock = ws.transport.get_extra_info('socket')
                    if sock:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                        if hasattr(socket, "TCP_KEEPIDLE"):
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5)
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 1)
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                    
                    self._ws = ws
                    self.last_pong_time = time.time()
                    logger.info(f"🔌 [Worker {self.worker_id}] Connected to {self.stream_url}")
                    
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
                            # [Audit 27.1] Non-blocking Ingestion
                            self.bridge.ingest_raw(msg)
                            self.last_msg_ts = time.time()
                    except Exception as e:
                        logger.warning(f"⚠️ [Worker {self.worker_id}] Connection lost: {e}")
                    finally:
                        sub_handler.cancel()
                        heartbeat_handler.cancel()
                        self._ws = None
                        
            except Exception as e:
                logger.error(f"❌ [Worker {self.worker_id}] Connection error: {e}. Reconnecting...")
                if _disconnect_ts == 0.0:
                    _disconnect_ts = time.time()
                await asyncio.sleep(2)
        
        await self.bridge.stop()

    async def _heartbeat_watchdog(self):
        """[Audit 5.15] Zombie Socket Watchdog."""
        while self._running:
            await asyncio.sleep(2) # Frequent polling
            if not self._ws: continue
            
            try:
                now = time.time()
                # 1. Active Probe (Bybit Spec)
                await self._ws.send('{"op":"ping"}')
                
                # 2. Silent Drop Check: No messages (data or pongs) for 10s = Death
                # This catches 'Half-Open' states that standard ping_timeout might miss
                silence_duration = now - self.last_msg_ts
                if silence_duration > 10.0:
                    logger.critical(f"💀 [Worker {self.worker_id}] Zombie Socket (10s Silence). Force Reconnect.")
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
        msg_type = payload.get("type", "delta")
        
        is_snap = msg_type == "snapshot"
        
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

        # Publish raw event for consumers (CandleCache, MarketState)
        if self.publish_to_bus:
            asyncio.create_task(bus.publish(RawWSEvent(topic=topic, data=data, u=u, is_snapshot=is_snap)))
        
        # Local callback for Shard Logic
        if hasattr(self, 'on_raw_event_cb') and self.on_raw_event_cb:
            asyncio.create_task(self.on_raw_event_cb(RawWSEvent(topic=topic, data=data, u=u, is_snapshot=is_snap)))


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
