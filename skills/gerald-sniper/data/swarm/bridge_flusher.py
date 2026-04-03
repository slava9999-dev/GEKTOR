import asyncio
import orjson
from loguru import logger
from redis.asyncio import Redis, ConnectionPool
from typing import Any, List
import os

class DataPipelineBridge:
    """
    [Audit 27.1] High-performance Data Pipeline Bridge.
    Implements Non-blocking Ingestion + Micro-batch Flusher (Task 19.4).
    Decouples WebSocket reading from Redis publishing to prevent Zombie Sockets.
    """
    def __init__(self, redis_url: str = None, batch_size: int = 200, max_queue_size: int = 50000):
        # [Audit 27.2] Explicit URL injection to prevent 'Authentication required' drift
        self.redis_url = redis_url 
        if not self.redis_url:
             # Fallback to env but with strict validation
             self.redis_url = os.getenv("REDIS_URL")
        
        if not self.redis_url or ("@" not in self.redis_url and "localhost" in self.redis_url):
            logger.critical(f"🛑 [Bridge] FATAL: REDIS_URL is invalid or missing credentials. Got: {self.redis_url}")
            raise ConnectionError("Redis Authentication Required in Production/Hardened mode.")

        self.batch_size = batch_size
        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self._running = False
        self._flusher_task = None
        self.pool = None
        self.redis = None

    async def start(self):
        if self._running: return
        self._running = True
        
        # Log sanitized URL (no password)
        sanitized_url = self.redis_url.split('@')[-1] if '@' in self.redis_url else self.redis_url
        logger.info(f"⚡ [Bridge] Initializing flusher pool -> {sanitized_url}")
        
        try:
            password = os.getenv("REDIS_PASSWORD", None)
            self.pool = ConnectionPool.from_url(self.redis_url, password=password, decode_responses=False)
            self.redis = Redis(connection_pool=self.pool)
            # Connectivity Ping (Fail Fast)
            await self.redis.ping()
            
            self._flusher_task = asyncio.create_task(self._flush_worker())
            logger.info(f"⚡ [Bridge] DataPipelineBridge ACTIVE (Batch: {self.batch_size}, Queue: {self.queue.maxsize})")
        except Exception as e:
            logger.error(f"❌ [Bridge] Hard Connection Failure: {e}")
            raise

    async def stop(self):
        self._running = False
        if self._flusher_task:
            self._flusher_task.cancel()
            try: await self._flusher_task
            except asyncio.CancelledError: pass
        if self.redis:
            await self.redis.aclose()
        if self.pool:
            await self.pool.disconnect()
        logger.warning("🛀 [Bridge] DataPipelineBridge stopped.")

    def ingest_raw(self, raw_payload: bytes):
        """
        [Audit 27.2] Non-blocking synchronous ingestion.
        Safe to call from any asyncio task without blocking.
        """
        try:
            self.queue.put_nowait(raw_payload)
        except asyncio.QueueFull:
            # Drop ticks but stay alive (Backpressure)
            pass

    async def _flush_worker(self):
        """
        Background worker: Batching events and pushing to Redis Pub/Sub.
        Eliminates network overhead of individual ticks.
        """
        while self._running:
            batch = []
            try:
                # 1. Wait for at least one message
                item = await self.queue.get()
                batch.append(item)

                # 2. Greedy fetch up to batch_size
                while len(batch) < self.batch_size:
                    try:
                        batch.append(self.queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                # 3. Publish via Redis Pipeline (Atomic network operation)
                await self._publish_batch(batch)

                # 4. Mark items as processed
                for _ in range(len(batch)):
                    self.queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [Bridge] Flusher error: {e}")
                await asyncio.sleep(0.1)

    async def _publish_batch(self, batch: List[bytes]):
        """
        Publishes batch of raw messages to a specific channel.
        Using RawWSEvent as the channel target.
        """
        async with self.redis.pipeline(transaction=False) as pipe:
            for raw_data in batch:
                # We publish to 'RawWSEvent' channel for the core engine bridge to consume
                pipe.publish("RawWSEvent", raw_data)
            await pipe.execute()
