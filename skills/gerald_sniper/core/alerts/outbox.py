from __future__ import annotations
import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from enum import IntEnum
from loguru import logger
from typing import Optional, Any, TYPE_CHECKING

from core.events.events import SignalEvent, AlertEvent
from core.events.nerve_center import bus

class MessagePriority(IntEnum):
    LOW = 1       # Health reports, scans
    NORMAL = 2    # Shadow signals, level proximity
    CRITICAL = 3  # APPROVED SIGNAL (Action Required)

class DecaySentinel:
    """ HFT Quant: Анализатор протухания альфы для ручного трейдера """
    def __init__(self, max_acceptable_delay_ms: float = 8000.0):
        self.max_acceptable_delay_ms = max_acceptable_delay_ms
        
    def evaluate_signal(self, signal: SignalEvent) -> dict:
        age_ms = signal.signal_age_ms
        if age_ms == 0.0:
            age_ms = (time.time() - signal.created_at) * 1000.0
            
        decay_ratio = min(age_ms / self.max_acceptable_delay_ms, 1.0)
        is_tradable = decay_ratio < 1.0
        
        return {
            "age_seconds": round(age_ms / 1000, 2),
            "alpha_decay": f"{decay_ratio * 100:.1f}%",
            "status": "🟢 TRADABLE" if is_tradable else "🔴 ROTTEN (OLD FLOW)"
        }

class ReliableTelegramOutbox:
    """
    [GEKTOR v14.4.4] Priority-Aware Token Bucket Outbox.
    Implements:
    1. Priority Ordering: CRITICAL signals are always at the top of the batch.
    2. Strict Throttling: Enforces 1.25s cooldown between network calls.
    3. Dynamic Batching: Combines all pending queue items into a single Telegram message.
    """

    THROTTLE_INTERVAL = 1.3 # Safe margin for 1.0s limit
    
    # Priority Levels
    CRITICAL = 0
    URGENT = 1
    NORMAL = 2

    def __init__(self, redis_client=None, stream_name="stream:AlertEvent", group_name="cortex_outbox"):
        from core.events.nerve_center import bus
        self.redis = redis_client or bus.redis
        self.stream = stream_name
        self.group = group_name
        self.consumer = f"outbox_{os.getpid()}"
        
        self._running = False
        self._connector = None
        self._session = None
        self._queue = asyncio.PriorityQueue() # (priority, timestamp, msg_id, data)
        self._last_send_ts = 0.0

    async def initialize_group(self):
        try:
            await self.redis.xgroup_create(self.stream, self.group, mkstream=True, id='$')
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                 logger.error(f"❌ [OUTBOX] XGROUP error: {e}")

    async def start(self):
        self._running = True
        await self.initialize_group()
        
        # Start Worker Loops
        asyncio.create_task(self._ingestion_loop())
        asyncio.create_task(self._dispatcher_loop())
        
        logger.success(f"🚀 [OUTBOX] Priority Token Bucket Worker ACTIVE.")

    async def _ingestion_loop(self):
        """Phase 1: Ingest from Redis Streams into Priority Queue."""
        # Recover PEL (Pending Entries)
        try:
            pending = await self.redis.xreadgroup(self.group, self.consumer, {self.stream: '0'}, count=50)
            if pending:
                 for msg_id, payload in pending[0][1]:
                     await self._enqueue(msg_id, payload)
        except Exception: pass

        while self._running:
            try:
                response = await self.redis.xreadgroup(self.group, self.consumer, {self.stream: '>'}, count=20, block=2000)
                if response:
                    for msg_id, payload in response[0][1]:
                        await self._enqueue(msg_id, payload)
            except Exception as e:
                logger.error(f"❌ [OUTBOX] Ingestion error: {e}")
                await asyncio.sleep(1)

    async def _enqueue(self, msg_id, payload):
        data = self._parse_payload(payload)
        priority_val = data.get('priority', self.NORMAL)
        if "🚨" in data.get('message', ''): priority_val = self.CRITICAL
        
        # PriorityQueue uses (priority, timestamp, msg_id, data)
        # Low priority value = First out (CRITICAL=0)
        await self._queue.put((priority_val, time.time(), msg_id, data))

        import aiohttp
        from aiohttp import TCPConnector, ClientTimeout
        
        # David Beazley: Hardened Concurrency Layer
        # Limit pool, force close dead sockets, enable cleanup
        if not self._connector:
            self._connector = TCPConnector(
                limit=10, 
                force_close=True, 
                enable_cleanup_closed=True
            )
            
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=ClientTimeout(total=20, connect=5)
            )

        while self._running:
            # 1. Wait for at least one message
            p, ts, mid, data = await self._queue.get()
            batch = [(mid, data, p)]

            # 2. Aggregation: Drain everything currently in the queue
            # This handles bursts by combining them into a single summary
            while not self._queue.empty():
                 p2, ts2, mid2, data2 = self._queue.get_nowait()
                 batch.append((mid2, data2, p2))
            
            # 3. Strict Token Bucket / Throttling
            # Never exceed 1.25s per call, regardless of Priority.
            now = time.monotonic()
            elapsed = now - self._last_send_ts
            if elapsed < self.THROTTLE_INTERVAL:
                await asyncio.sleep(self.THROTTLE_INTERVAL - elapsed)

            # 4. Formatter & Delivery
            await self._deliver_batch(batch)

    async def _deliver_batch(self, batch):
        """Combines multiple alerts into a hierarchical message."""
        critical_batch = [b for b in batch if b[2] == self.CRITICAL]
        normal_batch = [b for b in batch if b[2] > self.CRITICAL]

        summary = []
        if critical_batch:
            summary.append(f"🚨 <b>EXTREME VOLATILITY DETECTED ({len(critical_batch)})</b>")
            for _, data, _ in critical_batch[:5]:
                 summary.append(f"• {data.get('message', '')}")
            if len(critical_batch) > 5:
                 summary.append(f"<i>...and {len(critical_batch)-5} more criticals.</i>")
            summary.append("")

        if normal_batch:
            summary.append(f"📦 <b>Market Scan ({len(normal_batch)})</b>")
            for _, data, _ in normal_batch[:3]:
                 summary.append(f"• {data.get('message', '')[:120]}")
            if len(normal_batch) > 3:
                 summary.append(f"<i>...and {len(normal_batch)-3} more updates.</i>")

        text = "\n".join(summary)
        
        if await self._send_to_telegram(text):
            self._last_send_ts = time.monotonic()
            for msg_id, _, _ in batch:
                await self.redis.xack(self.stream, self.group, msg_id)
        else:
            # Re-enqueue failed messages? 
            # XREADGROUP PEL will keep them for the next restart.
            pass

    def _parse_payload(self, payload) -> dict:
        try:
            import orjson
            return orjson.loads(payload.get('payload', '{}'))
        except: return {}

    async def _send_to_telegram(self, text: str) -> bool:
        from utils.config import config
        bot_token = config.telegram.bot_token
        chat_id = config.telegram.chat_id
        if not bot_token or not chat_id: return False

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        
        try:
            # GEKTOR v14.8.1: Hardened Network Dispatch
            async with self._session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return True
                logger.warning(f"⚠️ [Bot] API Status {resp.status} (ChatID: {chat_id})")
                return False
        except (asyncio.TimeoutError, aiohttp.ClientConnectorError) as e:
             logger.error(f"🛑 [Bot] Network partition to Telegram (Semafore Halt): {e}")
             return False
        except Exception as e:
             logger.critical(f"🛑 [Bot] Unexpected Telegram crash: {e}")
             return False

    async def stop(self):
        self._running = False
        if self._session: await self._session.close()

# Global singleton
telegram_outbox = ReliableTelegramOutbox()
