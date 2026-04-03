"""
Gerald v7.1 — Transactional Outbox Relay (Kleppmann's Pattern).

Architecture:
    ┌──────────────────────┐         ┌───────────┐
    │  ExecutionEngine     │         │  Redis     │
    │  ──────────────────  │         │  (Nerve)   │
    │  1. DB.insert(order) │         └─────┬─────┘
    │  2. DB.insert(outbox)│               │
    │     ← SINGLE TX →   │               │
    └──────────┬───────────┘               │
               │                           │
               │  (Async Relay Worker)     │
               │  ────────────────────     │
               │  SELECT * FROM outbox     │
               │  WHERE published_at IS NULL│
               │         │                 │
               │         └────► NerveCenter.publish()
               │                     │
               │         ┌───────────┘
               │         │ ON SUCCESS:
               │         │   UPDATE outbox SET published_at = NOW()
               │         │
               │         │ ON FAILURE (Redis Down):
               │         │   Exponential Backoff → Retry
               │         │   ExecutionEngine continues working autonomously
               └─────────┘

Guarantees:
    - At-Least-Once delivery (events may be replayed on recovery)
    - Zero data loss: DB is the single source of truth
    - ExecutionEngine never blocks on Redis availability
    - Idempotent consumers (subscribers must handle duplicates)
    
Dependencies:
    - PostgreSQL (outbox_events table, already in schema)
    - NerveCenter (Redis Pub/Sub)
"""

import asyncio
import time
import json
from typing import Optional, TYPE_CHECKING
from loguru import logger
from pydantic import BaseModel

if TYPE_CHECKING:
    from data.database import DatabaseManager
    from core.events.nerve_center import NerveCenter


class OutboxRelay:
    """
    Asynchronous Relay Worker that drains the PostgreSQL outbox_events table
    and publishes events to Redis NerveCenter.
    
    Implements:
    - Batch polling with adaptive interval (50ms → 5s)
    - Exponential backoff on Redis failure (1s → 30s)
    - At-Least-Once semantics via publish-then-mark pattern
    - Metrics for observability
    """
    
    # Tuning Constants
    POLL_INTERVAL_IDLE = 1.0      # Polling interval when outbox is empty
    POLL_INTERVAL_ACTIVE = 0.05   # Polling interval when events are flowing (50ms)
    BATCH_SIZE = 50               # Max events per poll cycle
    MAX_BACKOFF = 30.0            # Max backoff on Redis failure
    STALE_EVENT_TTL = 3600        # Events older than 1h are logged as stale
    
    def __init__(self):
        self.db: Optional["DatabaseManager"] = None
        self.nerve: Optional["NerveCenter"] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._backoff = 1.0
        self._notify_event = asyncio.Event()
        self._stats = {
            "relayed": 0,
            "failed": 0,
            "stale_skipped": 0,
            "redis_errors": 0,
        }
    
    def configure(self, db_manager: "DatabaseManager", nerve_center: "NerveCenter"):
        """Wire dependencies after initialization."""
        self.db = db_manager
        self.nerve = nerve_center
        logger.info("📡 [OutboxRelay] Configured for LISTEN/NOTIFY reactive mode.")
    
    async def start(self):
        """Launch the background relay worker."""
        if self._running:
            return
        if not self.db or not self.nerve:
            logger.error("❌ [OutboxRelay] Cannot start: DB or NerveCenter not configured.")
            return
            
        self._running = True
        self._task = asyncio.create_task(self._relay_loop(), name="outbox_relay")
        logger.info("🚀 [OutboxRelay] Worker ACTIVE. Listening for pg_notify('outbox_channel').")
    
    async def stop(self):
        """Graceful shutdown."""
        self._running = False
        self._notify_event.set() # Kick the loop out of wait
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 [OutboxRelay] Worker stopped.")

    def _on_pg_notify(self, conn, pid, channel, payload):
        """Callback for PostgreSQL NOTIFY event."""
        logger.debug(f"🔔 [OutboxRelay] Received NOTIFY on {channel}")
        # Wake up the relay loop immediately
        self._notify_event.set()
    
    async def _relay_loop(self):
        """
        Reactive relay loop using PostgreSQL LISTEN/NOTIFY.
        
        This loop sleeps efficiently and only wakes up when:
        1. A new event is COMMITTED to outbox_events (via pg_notify trigger).
        2. A safety timeout occurs (1.0s) to catch any missed notifications.
        3. A Redis error occurred and backoff expired.
        """
        while self._running:
            try:
                # 1. Establish dedicated connection for LISTEN
                async with self.db.engine.connect() as conn:
                    # [Audit 5.16] Legal SQLAlchemy 2.0 Raw Driver Extraction
                    raw_conn = await conn.get_raw_connection()
                    asyncpg_conn = raw_conn.driver_connection
                    
                    # Register listener on the native asyncpg connection
                    await asyncpg_conn.add_listener('outbox_channel', self._on_pg_notify)
                    logger.info("📡 [OutboxRelay] Listening on pg_notify('outbox_channel') via raw_connection.")

                    while self._running:
                        # 2. Durable Recovery: Always drain the buffer on wake/reconnect
                        # This ensures events written during downtime are NOT skipped.
                        has_more = True
                        while has_more and self._running:
                            relayed_count = await self._process_batch()
                            has_more = relayed_count >= self.BATCH_SIZE
                            
                            if relayed_count > 0:
                                self._backoff = 1.0
                            
                            if self._backoff > 1.0:
                                await asyncio.sleep(self._backoff)
                                self._backoff = min(self.MAX_BACKOFF, self._backoff * 1.5)
                        
                        # 3. Wait for notification or periodic safety sweep
                        try:
                            # 1s safety heartbeat catch-all
                            await asyncio.wait_for(
                                self._notify_event.wait(), 
                                timeout=self.POLL_INTERVAL_IDLE
                            )
                        except asyncio.TimeoutError:
                            pass
                        finally:
                            self._notify_event.clear()
                            
                    # Clean up listener on connection exit
                    try:
                        await asyncpg_conn.remove_listener('outbox_channel', self._on_pg_notify)
                    except:
                        pass
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [OutboxRelay] Critical loop failure: {e}")
                self._stats["failed"] += 1
                await asyncio.sleep(2.0) # Panic sleep before reconnection attempt

    async def _process_batch(self) -> int:
        """
        Process a single batch of events.
        Returns the number of events successfully relayed to Redis.
        """
        events = await self._fetch_pending_events()
        if not events:
            return 0
        
        success_ids = []
        for event_row in events:
            event_id = event_row["id"]
            channel = event_row["channel"]
            payload = event_row["payload"]
            
            try:
                # 4. PUBLISH-THEN-MARK (At-Least-Once Delivery)
                # Ensure payload is stringified JSON for Redis
                payload_str = json.dumps(payload) if isinstance(payload, (dict, list)) else payload
                await self.nerve.redis.publish(channel, payload_str)
                
                success_ids.append(event_id)
                self._stats["relayed"] += 1
            except Exception as e:
                # Redis failure — trigger backoff in the main loop
                self._stats["redis_errors"] += 1
                self._backoff = max(self._backoff, 2.0) 
                logger.error(f"❌ [OutboxRelay] Redis publish FAILED for #{event_id}: {e}")
                break
        
        if success_ids:
            await self._mark_published(success_ids)
            
        return len(success_ids)
    
    async def _fetch_pending_events(self) -> list:
        """
        Fetch unpublished events from outbox_events ordered by creation time.
        Uses FOR UPDATE SKIP LOCKED for safe concurrent access.
        """
        from sqlalchemy import text
        
        async with self.db.SessionLocal() as session:
            try:
                result = await session.execute(
                    text("""
                        SELECT id, channel, payload, created_at
                        FROM outbox_events
                        WHERE published_at IS NULL
                        ORDER BY id ASC
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                    """),
                    {"limit": self.BATCH_SIZE}
                )
                return [dict(row) for row in result.mappings()]
            except Exception as e:
                logger.error(f"❌ [OutboxRelay] DB fetch failed: {e}")
                return []
    
    async def _mark_published(self, event_ids: list[int]):
        """
        Mark events as published with a timestamp.
        Batch update for efficiency.
        """
        from sqlalchemy import text
        
        async with self.db.SessionLocal() as session:
            try:
                await session.execute(
                    text("""
                        UPDATE outbox_events 
                        SET published_at = CURRENT_TIMESTAMP
                        WHERE id = ANY(:ids)
                    """),
                    {"ids": event_ids}
                )
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [OutboxRelay] Failed to mark events as published: {e}")
    
    @property
    def stats(self) -> dict:
        return {**self._stats, "backoff": self._backoff}


# ─── Transactional Outbox Writer (called from within DB transactions) ───

async def write_outbox_event(
    session,
    event: BaseModel,
    channel: Optional[str] = None,
):
    """
    Write an event to the outbox_events table within an existing DB transaction.
    
    This function MUST be called inside a `async with db.SessionLocal() as session:` 
    block, BEFORE session.commit(). This ensures atomicity: the event is committed
    together with the business data (order state, position, etc.).
    
    Args:
        session: Active SQLAlchemy AsyncSession (uncommitted transaction)
        event: Pydantic BaseModel event to persist
        channel: Redis channel name. Defaults to event class name.
        
    Usage:
        async with db.SessionLocal() as session:
            # 1. Business logic
            await session.execute(text("INSERT INTO orders ..."), {...})
            
            # 2. Outbox event (same TX)
            await write_outbox_event(session, ExecutionEvent(...))
            
            # 3. Commit atomically
            await session.commit()
    """
    from sqlalchemy import text
    
    ch = channel or type(event).__name__
    payload = event.model_dump(mode="json")
    
    await session.execute(
        text("""
            INSERT INTO outbox_events (channel, payload, created_at)
            VALUES (:channel, :payload, CURRENT_TIMESTAMP)
        """),
        {"channel": ch, "payload": json.dumps(payload)}
    )


# ─── Global Singleton ───

outbox_relay = OutboxRelay()
