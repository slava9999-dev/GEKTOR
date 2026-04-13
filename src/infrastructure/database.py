# src/infrastructure/database.py
import os
import asyncio
import json
import time
from datetime import datetime
from typing import Any, List, Optional
from loguru import logger
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from redis.asyncio import Redis

from .config import settings

class ReliableIngestionBuffer:
    """
    Nerve Center Reliable Queue (GEKTOR v2.0 CLEAN)
    Implements At-Least-Once delivery via modern Redis BLMOVE.
    """
    def __init__(self, db_manager):
        self.db = db_manager
        self.redis = Redis(
            host=settings.REDIS_HOST, 
            port=settings.REDIS_PORT, 
            password=settings.REDIS_PASSWORD, 
            decode_responses=True
        )
        self.queue_key = "gektor:ingest:queue"
        self.processing_key = "gektor:ingest:processing"
        self.dlq_key = "gektor:ingest:dlq"
        self._worker_task = None
        self._sweeper_task = None
        self._running = False

    async def start(self):
        # 1. Healthcheck: Redis Version Verification (GEKTOR Protocol)
        info = await self.redis.info("server")
        version = info.get("redis_version", "0.0.0")
        major, minor = map(int, version.split(".")[:2])
        if major < 6 or (major == 6 and minor < 2):
            logger.critical(f"❌ [DB] Redis 6.2+ required for BLMOVE. Found: {version}")
            raise RuntimeError(f"Incompatible Redis version: {version}")
        
        logger.info(f"✅ [DB] Redis Version Check Passed: {version}")

        if not self._running:
            # Crash Recovery
            await self._recover_stranded_tasks()
            self._running = True
            self._worker_task = asyncio.create_task(self._process_queue())
            self._sweeper_task = asyncio.create_task(self._active_sweeper())
            logger.info(f"🚀 [DB] Reliable Buffer active.")

    async def _recover_stranded_tasks(self):
        recovered = 0
        while True:
            item = await self.redis.lmove(self.processing_key, self.queue_key, "RIGHT", "LEFT")
            if not item: break
            recovered += 1
        if recovered > 0:
            logger.warning(f"♻️ [DB] Recovered {recovered} stranded tasks.")

    async def push_query(self, query: str, params: Any = None):
        try:
            payload = json.dumps({
                "query": query, 
                "params": params,
                "ts": time.time()
            }, default=str)
            await self.redis.lpush(self.queue_key, payload)
        except Exception as e:
            logger.error(f"🚨 [DB] Redis WAL failure: {e}")

    async def _process_queue(self):
        while self._running:
            try:
                payload_str = await self.redis.blmove(self.queue_key, self.processing_key, 5, "LEFT", "RIGHT")
                if not payload_str: continue
                
                data = json.loads(payload_str)
                params = data["params"]
                
                # ISO DateTime rehydration
                if isinstance(params, dict):
                    for k, v in params.items():
                        if isinstance(v, str) and len(v) >= 19:
                            try: params[k] = datetime.fromisoformat(v.replace(' ', 'T'))
                            except: pass

                async with self.db.SessionLocal() as session:
                    try:
                        await session.execute(text(data["query"]), params)
                        await session.commit()
                        await self.redis.lrem(self.processing_key, 1, payload_str)
                    except Exception as e:
                        await session.rollback()
                        logger.error(f"❌ [DB] Write Failed: {e}. Moving to DLQ.")
                        pipe = self.redis.pipeline()
                        pipe.lrem(self.processing_key, 1, payload_str)
                        pipe.lpush(self.dlq_key, payload_str)
                        await pipe.execute()
            except asyncio.CancelledError: break
            except Exception as e:
                logger.error(f"⚠️ [DB] Worker Error: {e}")
                await asyncio.sleep(1)

    async def _active_sweeper(self):
        while self._running:
            try:
                await asyncio.sleep(30)
                processing_items = await self.redis.lrange(self.processing_key, 0, -1)
                now = time.time()
                for payload_str in processing_items:
                    data = json.loads(payload_str)
                    if now - data.get("ts", now) > 60:
                        pipe = self.redis.pipeline()
                        pipe.lrem(self.processing_key, 1, payload_str)
                        data["ts"] = now
                        pipe.lpush(self.queue_key, json.dumps(data))
                        await pipe.execute()
                        logger.warning(f"♻️ [DB] Reclaimed stalled task.")
            except asyncio.CancelledError: break
            except Exception as e: logger.error(f"⚠️ [DB] Sweeper Error: {e}")

    async def stop(self):
        self._running = False
        if self._worker_task: self._worker_task.cancel()
        if self._sweeper_task: self._sweeper_task.cancel()
        await self.redis.close()

class DatabaseManager:
    """SQLAlchemy 2.0 + asyncpg Database Manager."""
    def __init__(self):
        self.engine = create_async_engine(
            settings.ASYNC_DATABASE_URL,
            pool_size=50,
            max_overflow=20,
            pool_recycle=300,
            pool_pre_ping=True
        )
        self.SessionLocal = async_sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False
        )
        self.buffer = ReliableIngestionBuffer(self)

    async def initialize(self):
        await self.buffer.start()
        
        # Schema Initialization (GEKTOR Protocol v2.0)
        # We split commands because asyncpg/Postgres doesn't allow multiple cmds in one prepared statement.
        ddl_commands = [
            """
            CREATE TABLE IF NOT EXISTS signals (
                id SERIAL PRIMARY KEY,
                signal_id TEXT UNIQUE,
                symbol TEXT,
                state TEXT,
                exit_price DOUBLE PRECISION,
                exit_vpin DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS outbox_events (
                id SERIAL PRIMARY KEY,
                payload TEXT,
                status TEXT DEFAULT 'PENDING',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                execute_after TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                retry_count INTEGER DEFAULT 0
            );
            """,
            """
            ALTER TABLE outbox_events 
            ADD COLUMN IF NOT EXISTS execute_after TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_outbox_events_pending 
            ON outbox_events (execute_after ASC) 
            WHERE status = 'PENDING';
            """,
            """
            ALTER TABLE outbox_events SET (
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 50,
                autovacuum_analyze_scale_factor = 0.0,
                autovacuum_analyze_threshold = 50
            );
            """
        ]
        
        try:
            async with self.engine.begin() as conn:
                for cmd in ddl_commands:
                    await conn.execute(text(cmd.strip()))
            
            logger.success("✅ [DB] Infrastructure stabilized. Schema & Vacuum tuned.")
        except Exception as e:
            logger.error(f"🚨 [DB] Schema initialization failed: {e}")
            raise

    async def push_query(self, query: str, params: dict = None):
        """Atomic execution of a query within a transaction."""
        async with self.engine.begin() as conn:
            await conn.execute(text(query), params or {})

    async def close(self):
        await self.buffer.stop()
        await self.engine.dispose()
