import os
import asyncio
from typing import Any, List, Tuple, Optional
from loguru import logger
from dotenv import load_dotenv

load_dotenv() # Load institutional configuration
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from sqlalchemy.orm import declarative_base

Base = declarative_base()

import json
from redis.asyncio import Redis

import time

class ReliableIngestionBuffer:
    """
    Nerve Center Reliable Queue (Gerald v2.1.2)
    Implements At-Least-Once delivery via modern Redis BLMOVE.
    Active Sweeper monitors for 'Stranded' tasks 'in-flight'.
    """
    def __init__(self, db_manager):
        self.db = db_manager
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6381"))
        password = os.getenv("REDIS_PASSWORD", None)
        self.redis = Redis(host=host, port=port, password=password, decode_responses=True)
        self.queue_key = "tekton:ingest:queue"
        self.processing_key = "tekton:ingest:processing" # Atomic WAL 'In-Flight'
        self.dlq_key = "tekton:ingest:dlq"
        self._worker_task = None
        self._sweeper_task = None
        self._running = False

    async def start(self):
        if not self._running:
            # 1. CRASH RECOVERY: One-time sweep at startup
            await self._recover_stranded_tasks()
            
            self._running = True
            self._worker_task = asyncio.create_task(self._process_queue())
            self._sweeper_task = asyncio.create_task(self._active_sweeper())
            logger.info(f"🚀 [DB] Reliable 'Reaper' Buffer started ({self.queue_key})")

    async def _active_sweeper(self):
        """
        Smart Oversight (Gerald v2.1.3)
        Monitors the Processing queue for stagnant tasks with TTL check.
        Ensures Idempotency: Reclaims only if 'In-Flight' > 60s.
        """
        while self._running:
            try:
                await asyncio.sleep(30) # Efficient check every 30s
                
                # Scan entire processing queue
                processing_items = await self.redis.lrange(self.processing_key, 0, -1)
                now = time.time()
                
                for payload_str in processing_items:
                    try:
                        data = json.loads(payload_str)
                        dispatched_at = data.get("ts", now)
                        
                        # Only reclaim if task has been stuck for > 60s
                        if now - dispatched_at > 60:
                            pipe = self.redis.pipeline()
                            # Atomic removal and re-lpush to main queue
                            pipe.lrem(self.processing_key, 1, payload_str)
                            
                            # Reset timestamp to prevent immediate re-sweep upon return
                            data["ts"] = now # Refresh TTL
                            pipe.lpush(self.queue_key, json.dumps(data))
                            
                            await pipe.execute()
                            logger.warning(f"♻️ [DB] Smart Reaper reclaimed stalled task for: {data.get('query')[:50]}...")
                    except Exception as e:
                        logger.error(f"⚠️ [DB] Sweeper Item Error: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"⚠️ [DB] Smart Sweeper Error: {e}")

    async def _recover_stranded_tasks(self):
        """Initial Cleanup: Picks up stranded tasks from previous process restarts."""
        recovered = 0
        while True:
            # Atomic move back to main queue: Reliability Pattern (Redis 6.2+)
            # Moving from processing_key to queue_key to retry stranded tasks
            item = await self.redis.lmove(
                self.processing_key, 
                self.queue_key, 
                "RIGHT", 
                "LEFT"
            )
            if not item: break
            recovered += 1
        if recovered > 0:
            logger.warning(f"♻️ [DB] Recovered {recovered} stranded tasks from previous session.")

    async def stop(self):
        self._running = False
        if self._worker_task: self._worker_task.cancel()
        if self._sweeper_task: self._sweeper_task.cancel()
        
        try:
            await asyncio.gather(self._worker_task, self._sweeper_task, return_exceptions=True)
        except Exception: pass
        
        await self.redis.close()
        logger.info("🔌 [DB] Persistent Buffer stopped.")

    async def push_query(self, query: str, params: Any = None):
        """Persistent LPUSH with Timestamp payload."""
        try:
            payload = json.dumps({
                "query": query, 
                "params": params,
                "ts": time.time() # TTL verification for Sweeper
            }, default=str)
            await self.redis.lpush(self.queue_key, payload)
        except Exception as e:
            logger.error(f"🚨 [DB] Redis WAL failure: {e}")

    async def _process_queue(self):
        """Standard HFT Consumer: BLMOVE (L-R) Reliable Pattern."""
        while self._running:
            try:
                # BLMOVE: Modern replacement for BRPOPLPUSH (Redis 6.2+)
                # Moving from the RIGHT side of Queue to LEFT side of Processing
                payload_str = await self.redis.blmove(
                    self.queue_key, 
                    self.processing_key, 
                    5, 
                    "LEFT", 
                    "RIGHT"
                )
                if not payload_str: continue
                
                data = json.loads(payload_str)
                params = data["params"]
                
                # Deserialization Fix: Restore datetime instances from ISO strings
                if isinstance(params, dict):
                    for k, v in params.items():
                        if isinstance(v, str) and len(v) >= 19: # YYYY-MM-DD HH:MM:SS
                            try:
                                params[k] = datetime.fromisoformat(v.replace(' ', 'T'))
                            except: pass
                elif isinstance(params, list):
                    for i, v in enumerate(params):
                        if isinstance(v, str) and len(v) >= 19:
                            try:
                                params[i] = datetime.fromisoformat(v.replace(' ', 'T'))
                            except: pass

                async with self.db.SessionLocal() as session:
                    try:
                        await session.execute(text(data["query"]), params)
                        await session.commit()
                        # SUCCESS ACK: Remove from processing list
                        await self.redis.lrem(self.processing_key, 1, payload_str)
                    except Exception as e:
                        await session.rollback()
                        logger.error(f"❌ [DB] Write Failed: {e}. Moving to DLQ.")
                        # ATOMIC FAILOVER: Move from processing to DLQ via Script/Pipeline
                        pipe = self.redis.pipeline()
                        pipe.lrem(self.processing_key, 1, payload_str)
                        pipe.lpush(self.dlq_key, payload_str)
                        await pipe.execute()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"⚠️ [DB] Reliable Worker Error: {e}")
                await asyncio.sleep(1)

class DatabaseManager:
    """
    SQLAlchemy 2.0 + asyncpg Database Manager.
    - Integrated Ingestion Buffer (DLQ Enabled).
    - TimescaleDB Retention Policies.
    - Pool Optimized for high-concurrency HFT streams.
    """
    def __init__(self):
        # Using asyncpg driver for PostgreSQL
        db_url = os.getenv("ASYNC_DATABASE_URL", "postgresql+asyncpg://tekton_admin:password@localhost:5433/tekton_alpha")
        
        self.engine = create_async_engine(
            db_url,
            pool_size=50,      # Institutional grade: Handle AI + HFT NerveCenter
            max_overflow=20,   # Peak capacity for 'Poison Stream' scenarios
            pool_recycle=300,
            pool_pre_ping=True # Health check before each acquire
        )
        self.SessionLocal = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False
        )
        self.buffer = ReliableIngestionBuffer(self)

    async def initialize(self):
        """
        [GEKTOR v21.28] Deterministic Schema Bootstrap.
        Guarantees table existence before applying migrations.
        """
        await self.buffer.start()
        
        # Start background retention worker
        asyncio.create_task(self.run_retention_worker())
        
        try:
            async with self.engine.begin() as conn:
                # 0. Clean session state
                try:
                    await conn.execute(text("ROLLBACK"))
                except: pass

                # 1. CORE: Alerts table
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS alerts (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        symbol VARCHAR(20) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        signal_type VARCHAR(50) NOT NULL,
                        level_price DOUBLE PRECISION,
                        entry_price DOUBLE PRECISION,
                        stop_suggestion DOUBLE PRECISION,
                        target_suggestion DOUBLE PRECISION,
                        total_score INTEGER,
                        score_breakdown JSONB,
                        rvol DOUBLE PRECISION,
                        delta_oi_pct DOUBLE PRECISION,
                        funding_rate DOUBLE PRECISION,
                        btc_trend VARCHAR(20),
                        ob_imbalance DOUBLE PRECISION DEFAULT 1.0,
                        liquidity_tier VARCHAR(10) DEFAULT 'C'
                    )
                """))

                # 2. METRICS: Telemetry
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS telemetry_metrics (
                        timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        metric_name VARCHAR(255) NOT NULL,
                        value DOUBLE PRECISION NOT NULL,
                        tags JSONB
                    )
                """))

                # 3. INFRA: Outbox
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS outbox_events (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(20),
                        channel VARCHAR(255) NOT NULL,
                        payload JSONB NOT NULL,
                        priority INTEGER DEFAULT 10,
                        attempt_count INTEGER DEFAULT 0,
                        next_try_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        published_at TIMESTAMP NULL,
                        status VARCHAR(20) DEFAULT 'PENDING'
                    );
                """))

                # 4. WATCHLIST
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS watchlist_history (
                        timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        symbol VARCHAR(20) NOT NULL,
                        score INTEGER,
                        price DOUBLE PRECISION,
                        volume_24h DOUBLE PRECISION,
                        volume_spike DOUBLE PRECISION,
                        liquidity_tier VARCHAR(10),
                        metrics JSONB
                    )
                """))

                # 5. EXECUTION: Orders
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS orders (
                        order_id VARCHAR(100) PRIMARY KEY,
                        symbol VARCHAR(20) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        state VARCHAR(30) DEFAULT 'IDLE',
                        entry_price DOUBLE PRECISION,
                        filled_qty DOUBLE PRECISION DEFAULT 0.0,
                        target_qty DOUBLE PRECISION DEFAULT 0.0,
                        tp_price DOUBLE PRECISION,
                        sl_price DOUBLE PRECISION,
                        exchange_updated_time_ms BIGINT DEFAULT 0,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    )
                """))

                # 6. MONITORING: Active Contexts (Phase 5)
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS monitoring_contexts (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(20) NOT NULL,
                        signal_id VARCHAR(50) NOT NULL,
                        state VARCHAR(20) DEFAULT 'ACTIVE',
                        sigma_db DOUBLE PRECISION DEFAULT 0.02,
                        context JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """))

                # 6.1 ANALYTICS: Signal Performance
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS signal_stats (
                        id SERIAL PRIMARY KEY,
                        signal_id VARCHAR(50) UNIQUE,
                        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        symbol VARCHAR(20) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        state VARCHAR(20) NOT NULL, 
                        entry_price DOUBLE PRECISION,
                        detectors TEXT,
                        confidence DOUBLE PRECISION,
                        rejection_reason TEXT,
                        metadata JSONB
                    )
                """))
                
                # 6.3 INFRA: Radar State (Causal Recovery)
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS radar_states (
                        instrument VARCHAR(20) PRIMARY KEY,
                        vpin DOUBLE PRECISION NOT NULL,
                        sigma DOUBLE PRECISION DEFAULT 0.0,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """))
                
                # 6.4 IDEMPOTENCY: Egress Guard (Exactly-Once delivery)
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS delivered_fingerprints (
                        fingerprint VARCHAR(255) PRIMARY KEY,
                        event_id INTEGER,
                        delivered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_delivered_at ON delivered_fingerprints (delivered_at);"))

                logger.success("✅ [DB] Infrastructure stabilized. Schema is consistent.")
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS paper_positions (
                        id SERIAL PRIMARY KEY,
                        alert_id INTEGER,
                        symbol VARCHAR(20) NOT NULL,
                        side VARCHAR(10) NOT NULL,
                        entry_price DOUBLE PRECISION NOT NULL,
                        entry_qty DOUBLE PRECISION NOT NULL,
                        sl_price DOUBLE PRECISION NOT NULL,
                        tp_price DOUBLE PRECISION NOT NULL,
                        upper_barrier DOUBLE PRECISION NOT NULL,
                        lower_barrier DOUBLE PRECISION NOT NULL,
                        expiration_ts DOUBLE PRECISION NOT NULL,
                        status VARCHAR(20) DEFAULT 'OPEN',
                        fee_paid DOUBLE PRECISION DEFAULT 0,
                        pnl_usd DOUBLE PRECISION DEFAULT 0,
                        pnl_pct DOUBLE PRECISION DEFAULT 0,
                        opened_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        closed_at TIMESTAMPTZ,
                        exit_price DOUBLE PRECISION,
                        exit_ts DOUBLE PRECISION,
                        exit_reason VARCHAR(50),
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    )
                """))

                # 8. REALITY METRICS
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS reality_metrics (
                        timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        symbol VARCHAR(20) NOT NULL,
                        signal_id VARCHAR(50),
                        direction VARCHAR(10),
                        latency_ms INTEGER,
                        slippage_pct DOUBLE PRECISION,
                        signal_price DOUBLE PRECISION,
                        executed_price DOUBLE PRECISION,
                        is_rogue BOOLEAN DEFAULT FALSE
                    )
                """))

                # 9. IDEMPOTENCY: Signal Reconciliation (Idempotency Guard)
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS signal_reconciliations (
                        id SERIAL PRIMARY KEY,
                        signal_id VARCHAR(100) UNIQUE NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_recon_time ON signal_reconciliations (created_at);"))

                # 9.1 PORTFOLIO: State persistence
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS portfolio_state (
                        current_balance DOUBLE PRECISION NOT NULL,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """))
                # Seed if empty
                await conn.execute(text("INSERT INTO portfolio_state (current_balance) SELECT 100000.0 WHERE NOT EXISTS (SELECT 1 FROM portfolio_state);"))

                # --- MIGRATIONS & SCHEMA PATCHES ---
                # Guarantee column existence before indexing
                logger.info("🧬 [DB] Applying Causal Migrations (HotPatcher)...")
                
                # Outbox Extensions
                await conn.execute(text("ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'PENDING';"))
                await conn.execute(text("ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS symbol VARCHAR(20);"))
                await conn.execute(text("ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS channel VARCHAR(50) DEFAULT 'telegram';"))
                await conn.execute(text("ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS event_type VARCHAR(50) DEFAULT 'UNKNOWN';"))
                await conn.execute(text("ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS prob DOUBLE PRECISION DEFAULT 0.0;"))
                await conn.execute(text("ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS vol DOUBLE PRECISION DEFAULT 0.0;"))
                await conn.execute(text("ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();"))
                
                # Context Extensions
                await conn.execute(text("ALTER TABLE monitoring_contexts ADD COLUMN IF NOT EXISTS sigma_db DOUBLE PRECISION DEFAULT 0.02;"))
                
                # Watchlist Extensions
                await conn.execute(text("ALTER TABLE watchlist_history ADD COLUMN IF NOT EXISTS velocity DOUBLE PRECISION DEFAULT 1.0;"))
                await conn.execute(text("ALTER TABLE watchlist_history ADD COLUMN IF NOT EXISTS momentum_pct DOUBLE PRECISION DEFAULT 0.0;"))

                # Triple Barrier Extensions (Shadow Ledger)
                logger.info("🧬 [DB] Patching 'paper_positions' for Triple Barrier logic...")
                await conn.execute(text("ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS upper_barrier DOUBLE PRECISION DEFAULT 0;"))
                await conn.execute(text("ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS lower_barrier DOUBLE PRECISION DEFAULT 0;"))
                await conn.execute(text("ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS expiration_ts DOUBLE PRECISION DEFAULT 0;"))
                await conn.execute(text("ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS side VARCHAR(10) DEFAULT 'BUY';"))
                await conn.execute(text("ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'OPEN';"))
                await conn.execute(text("ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS exit_price DOUBLE PRECISION;"))
                await conn.execute(text("ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS exit_ts DOUBLE PRECISION;"))
                await conn.execute(text("ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS exit_reason VARCHAR(50);"))

                # --- INDEXES & DB EXTENSIONS ---
                logger.info("⚡ [DB] Optimizing Query Paths (Indexing)...")
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (id) WHERE published_at IS NULL;"))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_outbox_symbol_pending ON outbox_events (symbol, status) WHERE status = 'PENDING';"))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_watchlist_symbol_ts ON watchlist_history (symbol, timestamp DESC);"))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_paper_status ON paper_positions (status);"))

                # Trigger for Outbox
                await conn.execute(text("""
                    CREATE OR REPLACE FUNCTION notify_outbox_event() RETURNS trigger AS $$
                    BEGIN
                        PERFORM pg_notify('outbox_channel', 'new_event');
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """))
                await conn.execute(text("DROP TRIGGER IF EXISTS trigger_notify_outbox ON outbox_events;"))
                await conn.execute(text("""
                    CREATE TRIGGER trigger_notify_outbox
                    AFTER INSERT ON outbox_events
                    FOR EACH STATEMENT EXECUTE FUNCTION notify_outbox_event();
                """))

                # TimescaleDB Extensions (Optional)
                try:
                    await conn.execute(text("SELECT create_hypertable('telemetry_metrics', 'timestamp', if_not_exists => TRUE);"))
                    await conn.execute(text("SELECT create_hypertable('watchlist_history', 'timestamp', if_not_exists => TRUE);"))
                    await conn.execute(text("SELECT create_hypertable('reality_metrics', 'timestamp', if_not_exists => TRUE);"))
                    await conn.execute(text("""
                        -- [GEKTOR v21.49] Теневые миссии для Triple Barrier Method
                        CREATE TABLE IF NOT EXISTS shadow_missions (
                            signal_id TEXT PRIMARY KEY,
                            symbol TEXT NOT NULL,
                            side TEXT NOT NULL,
                            entry_price DOUBLE PRECISION NOT NULL,
                            pessimistic_entry DOUBLE PRECISION NOT NULL,
                            tp_price DOUBLE PRECISION NOT NULL,
                            sl_price DOUBLE PRECISION NOT NULL,
                            expiration_ms BIGINT NOT NULL,
                            volume_usd DOUBLE PRECISION NOT NULL,
                            status TEXT NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, PROFIT, LOSS, TIMEOUT
                            exit_price DOUBLE PRECISION,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                        CREATE INDEX IF NOT EXISTS idx_shadow_active ON shadow_missions(status) WHERE status = 'ACTIVE';
                    """))
                    logger.info("✅ [DB] Infrastructure Schemas (Outbox/Fingerprints/Shadow) Synchronized.")
                except: pass

        except Exception as e:
            logger.critical(f"💥 [DB] Terminal initialization FAILED: {e}")
            raise

    async def insert_reality_metric(self, data: dict):
        """Non-blocking ingest for behavioral metrics."""
        query = """
            INSERT INTO reality_metrics 
            (symbol, signal_id, direction, latency_ms, slippage_pct, signal_price, executed_price, is_rogue)
            VALUES (:sym, :sid, :dir, :lat, :slip, :sp, :ep, :rogue)
        """
        params = {
            "sym": data["symbol"],
            "sid": data.get("signal_id"),
            "dir": data.get("direction"),
            "lat": data.get("latency_ms", 0),
            "slip": data.get("slippage_pct", 0.0),
            "sp": data.get("signal_price", 0.0),
            "ep": data.get("executed_price", 0.0),
            "rogue": data.get("is_rogue", False)
        }
        await self.buffer.push_query(query, params)

    async def get_daily_reality_check(self):
        """Aggregates daily behavioral delta."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN is_rogue THEN 1 ELSE 0 END) as rogue_trades,
                        AVG(latency_ms) as avg_latency,
                        AVG(slippage_pct) as avg_slippage,
                        SUM(slippage_pct) as total_slippage_cost
                    FROM reality_metrics
                    WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '24 hours')
                """)
                result = await session.execute(stmt)
                return result.mappings().one()
            except Exception as e:
                logger.error(f"❌ [DB] Reality Check Stats Failed: {e}")
                return None

    async def run_retention_worker(self, interval_sec: int = 21600):
        """
        [GEKTOR v21.49] Proactive Data Retention & Anti-Bloat Worker.
        Prevents SSD saturation by purging non-critical historic data.
        """
        logger.info(f"🧹 [DB] Retention Policy monitoring active (Interval: {interval_sec/3600:.0f}h)")
        while True:
            await asyncio.sleep(interval_sec)
            try:
                async with self.engine.begin() as conn:
                    # 1. Purge Telemetry (7 days)
                    await conn.execute(text("DELETE FROM telemetry_metrics WHERE timestamp < NOW() - INTERVAL '7 days'"))
                    
                    # 2. Purge Watchlist Snapshots (3 days - high frequency)
                    await conn.execute(text("DELETE FROM watchlist_history WHERE timestamp < NOW() - INTERVAL '3 days'"))
                    
                    # 3. Purge Processed Outbox Events (2 days)
                    # We only need them briefly for auditing, then they are just bloat.
                    await conn.execute(text("""
                        DELETE FROM outbox_events 
                        WHERE status IN ('DELIVERED', 'SUPERSEDED', 'FAILED', 'COMPACTED') 
                          AND created_at < NOW() - INTERVAL '2 days'
                    """))

                    # 4. Purge Old Reality Metrics (14 days)
                    await conn.execute(text("DELETE FROM reality_metrics WHERE timestamp < NOW() - INTERVAL '14 days'"))
                    
                    logger.info("🧹 [DB] Housekeeping complete. Disk space reclaimed from historic logs.")
            except Exception as e:
                logger.error(f"❌ [DB] Housekeeping failed: {e}")
            except Exception as e:
                logger.error(f"❌ [DB] Retention Worker failed: {e}")

    async def update_order_state_cas(self, order_id: str, expected_state: str, new_state: str, filled_inc: float = 0.0, exch_time: int = 0) -> bool:
        """
        Distributed Causality CAS (v2.1.6).
        Only updates if exch_time is strictly greater than current record.
        Prevents old REST data from overwriting fresh WebSocket events.
        """
        async with self.SessionLocal() as session:
            try:
                query = text("""
                    UPDATE orders 
                    SET state = :new_state, 
                        filled_qty = filled_qty + :filled_inc,
                        exchange_updated_time_ms = :exch_time,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE order_id = :order_id 
                      AND state = :expected_state
                      AND exchange_updated_time_ms <= :exch_time
                """)
                result = await session.execute(query, {
                    "new_state": new_state,
                    "filled_inc": filled_inc,
                    "order_id": order_id,
                    "expected_state": expected_state,
                    "exch_time": exch_time
                })
                await session.commit()
                return result.rowcount == 1
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [DB] Causality CAS Failed for {order_id}: {e}")
                return False


    # =========================================================================
    # PHASE 5: MONITORING & ABORT (Rehydration Standard)
    # =========================================================================

    async def fetch_active_contexts(self) -> List[Any]:
        """Fetches all active monitoring contexts for rehydration."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("SELECT * FROM monitoring_contexts WHERE state = 'ACTIVE'")
                result = await session.execute(stmt)
                return result.mappings().all()
            except Exception as e:
                logger.error(f"❌ [DB] Failed to fetch active contexts: {e}")
                return []

    async def update_monitoring_context(self, symbol: str, context_data: dict):
        """Updates the JSONB state of an active context (EMA/Pulse persistence)."""
        async with self.SessionLocal() as session:
            try:
                await session.execute(
                    text("UPDATE monitoring_contexts SET context = :ctx, updated_at = CURRENT_TIMESTAMP WHERE symbol = :sym AND state = 'ACTIVE'"),
                    {"sym": symbol, "ctx": json.dumps(context_data)}
                )
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [DB] Failed to update monitoring context for {symbol}: {e}")

    async def atomic_emergency_abort(self, symbol: str, alert_payload: dict):
        """
        [GEKTOR v21.12] Causal Consistency Protocol.
        Atomics:
        1. Closes monitoring context for the symbol.
        2. Supercedes (Cancels) all pending Tier 2/3 notifications for this symbol.
        3. Enqueues a TIER 1 Abort Alert.
        """
        async with self.engine.begin() as conn:
             try:
                 # 1. Close context (State shift)
                 await conn.execute(
                     text("UPDATE monitoring_contexts SET state = 'CLOSED', updated_at = NOW() WHERE symbol = :sym AND state = 'ACTIVE'"),
                     {"sym": symbol}
                 )
                 
                 # 2. COMPACTING OUTBOX: Cancel outdated advice for this asset
                 await conn.execute(
                     text("""
                         UPDATE outbox_events 
                         SET status = 'SUPERSEDED' 
                         WHERE symbol = :sym AND status = 'PENDING' AND priority < 100
                     """),
                     {"sym": symbol}
                 )
                 
                 # 3. Enqueue Tier 1 Alert
                 await conn.execute(
                     text("""
                         INSERT INTO outbox_events (symbol, channel, payload, priority)
                         VALUES (:sym, 'TELEGRAM_ALERTS', :payload, 100)
                     """),
                     {
                         "sym": symbol,
                         "payload": json.dumps(alert_payload)
                     }
                 )
                 logger.critical(f"🛑 [DB] [{symbol}] CAUSAL ABORT: Old alerts superseded. Exit mission enqueued.")
             except Exception as e:
                 logger.error(f"❌ [DB] Atomic Abort failed for {symbol}: {e}")
                 raise

    async def atomic_abort_context(self, symbol: str, alert_payload: dict):
        """
        Atomic Abort: Closes Monitoring Context + Registers Outbox Alert.
        Prevents orphaned signals and ensures 'At-Least-Once' notification of escape.
        """
        async with self.SessionLocal() as session:
            try:
                # 1. Close the monitoring context
                await session.execute(
                    text("UPDATE monitoring_contexts SET state = 'CLOSED', updated_at = CURRENT_TIMESTAMP WHERE symbol = :sym AND state = 'ACTIVE'"),
                    {"sym": symbol}
                )

                # 2. Register 'ABORT_MISSION' in outbox_events (Priority: 100)
                await session.execute(
                    text("""
                        INSERT INTO outbox_events (channel, payload, priority) 
                        VALUES ('TELEGRAM_ALERTS', :payload, 100)
                    """),
                    {
                        "payload": json.dumps(alert_payload),
                    }
                )

                await session.commit()
                logger.warning(f"🛑 [DB] Atomic Abort successful for {symbol}.")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [DB] Atomic Abort failed for {symbol}: {e}")
                raise

    # -------------------------------------------------------------------------
    # SMART OUTBOX METHODS (Phase 2.9 - 2.10)
    # -------------------------------------------------------------------------

    async def fetch_outbox_by_priority(self, min_priority: int, max_priority: int, limit: int = 10) -> List[dict]:
        """Fetches prioritized pending messages that are ready for retry."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    SELECT id, channel, payload, EXTRACT(EPOCH FROM created_at) as created_at, 
                           priority, attempt_count, created_at as created_at_precise
                    FROM outbox_events
                    WHERE status = 'PENDING' 
                      AND priority BETWEEN :min_p AND :max_p
                      AND next_try_at <= NOW()
                    ORDER BY priority DESC, created_at ASC
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                """)
                result = await session.execute(stmt, {"min_p": min_priority, "max_p": max_priority, "limit": limit})
                return [dict(r) for r in result.mappings().all()]
            except Exception as e:
                logger.error(f"❌ [DB] Failed to fetch outbox batch: {e}")
                return []

    async def mark_retry(self, event_id: int, wait_seconds: float):
        """Updates attempt count and schedules next try (Exponential Backoff)."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    UPDATE outbox_events 
                    SET attempt_count = attempt_count + 1,
                        next_try_at = NOW() + INTERVAL '1 second' * :wait
                    WHERE id = :id
                """)
                await session.execute(stmt, {"id": event_id, "wait": wait_seconds})
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [DB] Failed to mark retry for {event_id}: {e}")

    async def is_signal_still_active(self, signal_id: str) -> bool:
        """Verifies if the signal has not been aborted in Phase 5."""
        if not signal_id: return False
        async with self.SessionLocal() as session:
            try:
                stmt = text("SELECT state FROM monitoring_contexts WHERE signal_id = :sid")
                result = await session.execute(stmt, {"sid": signal_id})
                row = result.fetchone()
                return row is not None and row[0] == 'ACTIVE'
            except Exception as e:
                logger.error(f"❌ [DB] Failed to verify signal active: {e}")
                return False

    async def get_oldest_pending_emergency_ts(self) -> Optional[float]:
        """Check for jammed Emergency lane (STP Tier 1)."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    SELECT MIN(EXTRACT(EPOCH FROM created_at)) 
                    FROM outbox_events 
                    WHERE status = 'PENDING' AND priority = 100
                """)
                result = await session.execute(stmt)
                return result.scalar()
            except Exception as e:
                logger.error(f"❌ [DB] Failed to fetch oldest pending emergency: {e}")
                return None

    async def close(self):
        await self.buffer.stop()
        await self.engine.dispose()
        logger.info("🔌 [DB] Buffer stopped & Engine disposed.")

    # =========================================================================
    # PAPER TRACKER (Positions & Alerts)
    # =========================================================================

    async def insert_alert(
        self, symbol: str, direction: str, signal_type: str,
        level_price: float, entry_price: float,
        stop_price: float, target_price: float,
        total_score: int, score_breakdown: dict,
        rvol: float, delta_oi_pct: float, funding_rate: float,
        btc_trend: str, liquidity_tier: str = "C"
    ):
        """Saves a fired alert using async session."""
        import json
        
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    INSERT INTO alerts 
                    (timestamp, symbol, direction, signal_type, level_price, entry_price,
                     stop_suggestion, target_suggestion, total_score, score_breakdown,
                     rvol, delta_oi_pct, funding_rate, btc_trend, liquidity_tier)
                    VALUES (:ts, :sym, :dir, :sig, :lp, :ep, :sp, :tp, :tot, :sb, :rvol, :oi, :fr, :btc, :tier)
                    RETURNING id
                """)
                
                result = await session.execute(stmt, {
                    "ts": datetime.utcnow().isoformat(), "sym": symbol, "dir": direction,
                    "sig": signal_type, "lp": level_price, "ep": entry_price,
                    "sp": stop_price, "tp": target_price, "tot": total_score,
                    "sb": json.dumps(score_breakdown), "rvol": rvol, "oi": delta_oi_pct,
                    "fr": funding_rate, "btc": btc_trend, "tier": liquidity_tier
                })
                
                # Transactional Outbox implementation logic conceptual implementation:
                # An execute to `outbox_events` could be done HERE within the same session
                # to guarantee atomicity of position saving and event propagation.
                
                await session.commit()
                row = result.fetchone()
                return row[0] if row else None
                
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Failed to insert alert for {symbol}: {e}")
                return None

    async def insert_paper_position(
        self, alert_id: int, symbol: str, side: str, 
        entry_price: float, entry_qty: float, 
        sl_price: float, tp_price: float, fee: float = 0.0
    ):
        """Saves a new virtual position."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    INSERT INTO paper_positions 
                    (alert_id, symbol, side, entry_price, entry_qty, sl_price, tp_price, fee_paid, status)
                    VALUES (:aid, :sym, :side, :ep, :eq, :sl, :tp, :fee, 'OPEN')
                    RETURNING id
                """)
                result = await session.execute(stmt, {
                    "aid": alert_id, "sym": symbol, "side": side,
                    "ep": entry_price, "eq": entry_qty, "sl": sl_price, "tp": tp_price, "fee": fee
                })
                await session.commit()
                row = result.fetchone()
                return row[0] if row else None
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Failed to insert paper position for {symbol}: {e}")
                return None

    async def update_paper_position(
        self, pos_id: int, status: str, pnl_usd: float, pnl_pct: float, fee: float = 0.0
    ):
        """Updates status of a paper position on close."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    UPDATE paper_positions 
                    SET status = :status, pnl_usd = :pnl_u, pnl_pct = :pnl_p, 
                        fee_paid = fee_paid + :fee, closed_at = :now
                    WHERE id = :id
                """)
                await session.execute(stmt, {
                    "status": status, "pnl_u": pnl_usd, "pnl_p": pnl_pct, 
                    "fee": fee, "now": datetime.utcnow(), "id": pos_id
                })
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Failed to update paper position {pos_id}: {e}")
                return False

    async def get_active_paper_positions(self):
        """Returns all positions currently with status 'OPEN'."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("SELECT * FROM paper_positions WHERE status = 'OPEN'")
                result = await session.execute(stmt)
                return result.mappings().all()
            except Exception as e:
                logger.error(f"❌ Failed to fetch active paper positions: {e}")
                return []

    # =========================================================================
    # METRICS ENGINE (Batch Processing)
    # =========================================================================

    async def insert_metrics_batch(self, metrics_batch: list[dict]):
        """Batch insert telemetry metrics to prevent DB ping-pong per tick!"""
        if not metrics_batch: return
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    INSERT INTO telemetry_metrics (timestamp, metric_name, value, tags)
                    VALUES (:timestamp, :metric_name, :value, :tags)
                """)
                await session.execute(stmt, metrics_batch)
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [DB] Batch metric insert failed: {e}")

    async def insert_watchlist_history(self, candidates: list[dict]):
        """Non-blocking ingest: Top candidates from radar scan with strict normalization."""
        if not candidates: return
        import json
        from dataclasses import asdict
        
        # candidates is list of {"symbol": str, "score": int, "metrics": RadarV2Metrics}
        now = datetime.utcnow()
        query = """
            INSERT INTO watchlist_history 
            (timestamp, symbol, score, price, volume_24h, volume_spike, 
             velocity, momentum_pct, atr_ratio, orderflow_imbalance,
             liquidity_tier, metrics)
            VALUES (:ts, :sym, :score, :price, :vol, :spike, :vel, :mom, :atr, :imb, :tier, :metrics)
        """
        
        for c in candidates:
            m = c["metrics"]
            m_dict = asdict(m) if hasattr(m, "__dataclass_fields__") else {}
            
            params = {
                "ts": now,
                "sym": c["symbol"],
                "score": c["score"],
                "price": getattr(m, 'price', 0.0),
                "vol": getattr(m, 'volume_24h', 0.0),
                "spike": getattr(m, 'volume_spike', 0.0),
                "vel": getattr(m, 'velocity', 1.0),
                "mom": getattr(m, 'momentum_pct', 0.0),
                "atr": getattr(m, 'atr_ratio', 1.0),
                "imb": getattr(m, 'orderflow_imbalance', 1.0),
                "tier": getattr(m, 'liquidity_tier', 'D'),
                "metrics": json.dumps(m_dict)
            }
            
            # Offload to background buffer (DLQ-protected)
            await self.buffer.push_query(query, params)

    async def insert_signal_stat(
        self, signal_id: str, symbol: str, direction: str, state: str,
        entry_price: float, detectors: str, confidence: float = 0.0,
        rejection_reason: str = None, metadata: dict = None
    ):
        """Persistent log of signal lifecycle (Roadmap Step 2)."""
        import json
        query = """
            INSERT INTO signal_stats 
            (signal_id, symbol, direction, state, entry_price, detectors, confidence, rejection_reason, metadata)
            VALUES (:sid, :sym, :dir, :st, :ep, :det, :conf, :rr, :meta)
            ON CONFLICT (signal_id) DO UPDATE SET 
                state = EXCLUDED.state, 
                rejection_reason = EXCLUDED.rejection_reason,
                metadata = signal_stats.metadata || EXCLUDED.metadata
        """
        params = {
            "sid": signal_id,
            "sym": symbol,
            "dir": direction,
            "st": state,
            "ep": entry_price,
            "det": detectors,
            "conf": confidence,
            "rr": rejection_reason,
            "meta": json.dumps(metadata or {})
        }
        await self.buffer.push_query(query, params)

    # =========================================================================
    # ANALYTICS & MONITORING (Gerald Bridge Integration)
    # =========================================================================

    async def get_recent_alerts(self, days: int = 7, limit: int = 50):
        """Returns the most recent alerts for human analysis."""
        async with self.SessionLocal() as session:
            try:
                stmt = text("""
                    SELECT * FROM alerts 
                    WHERE timestamp > (CURRENT_TIMESTAMP - (INTERVAL '1 day' * :days))
                    ORDER BY timestamp DESC
                    LIMIT :limit
                """)
                result = await session.execute(stmt, {"days": days, "limit": limit})
                return [dict(row) for row in result.mappings()]
            except Exception as e:
                logger.error(f"❌ [DB] Failed to fetch recent alerts: {e}")
                return []

    async def get_alert_stats(self, days: int = 30):
        """Computes summary statistics of system performance."""
        async with self.SessionLocal() as session:
            try:
                # Count alerts by direction and signal type
                stmt = text("""
                    SELECT 
                        direction,
                        COUNT(*) as count,
                        AVG(total_score) as avg_score,
                        COUNT(DISTINCT symbol) as unique_symbols
                    FROM alerts
                    WHERE timestamp > (CURRENT_TIMESTAMP - (INTERVAL '1 day' * :days))
                    GROUP BY direction
                """)
                result = await session.execute(stmt, {"days": days})
                stats = [dict(row) for row in result.mappings()]
                
                # Count current open positions
                pos_stmt = text("SELECT COUNT(*) FROM paper_positions WHERE status = 'OPEN'")
                pos_res = await session.execute(pos_stmt)
                open_pos = pos_res.scalar()
                
                return {
                    "period_days": days,
                    "direction_breakdown": stats,
                    "open_positions": open_pos,
                    "last_updated": datetime.utcnow().isoformat()
                }
            except Exception as e:
                logger.error(f"❌ [DB] Failed to compute alert stats: {e}")
                return {}

    async def update_alert_result(self, alert_id: int, result: str, pnl: float = 0.0, notes: str = ""):
        """Manually updates the performance outcome of an alert (WIN/LOSS/SKIP)."""
        async with self.SessionLocal() as session:
            try:
                # In Postgres v5.23, metrics and results might be in sidecar tables or JSONB.
                # Adding 'result' and 'pnl_pct' columns if they don't exist.
                await session.execute(text("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS result VARCHAR(20)"))
                await session.execute(text("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS pnl_pct DOUBLE PRECISION"))
                
                stmt = text("UPDATE alerts SET result = :res, pnl_pct = :pnl WHERE id = :id")
                await session.execute(stmt, {"res": result, "pnl": pnl, "id": alert_id})
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [DB] Failed to update alert result: {e}")
                return False

    async def execute_read_safe(self, query: str, params: dict = None, timeout_ms: int = 2000):
        """
        Production-grade Safe execution for LLM-generated queries.
        - READ ONLY transaction
        - Strict statement_timeout protection
        - Automated parameter binding
        """
        async with self.engine.connect() as conn:
            try:
                # 1. Set protections on the connection
                await conn.execute(text(f"SET local statement_timeout = '{timeout_ms}ms'"))
                await conn.execute(text("SET TRANSACTION READ ONLY"))
                
                # 2. Execute
                result = await conn.execute(text(query), params or {})
                return result.mappings().all()
            except Exception as e:
                logger.error(f"⚠️ [DB] Safe Read Error: {e}")
                raise

    async def get_weekly_summary(self):
        """Generates a performance report for the last 7 days."""
        try:
            rows = await self.execute_read_safe("""
                SELECT 
                    result,
                    COUNT(*) as count,
                    AVG(pnl_pct) as avg_pnl,
                    SUM(pnl_pct) as total_pnl
                FROM alerts
                WHERE timestamp > (CURRENT_TIMESTAMP - INTERVAL '7 days')
                  AND result IS NOT NULL
                GROUP BY result
            """)
            
            if not rows:
                return "📈 <b>WEEKLY REPORT:</b> No evaluated trades in the last 7 days."
            
            report = "📈 <b>WEEKLY PERFORMANCE REPORT</b>\n\n"
            for r in rows:
                report += f" • {r['result']}: {r['count']} trades | PNL: {r['total_pnl']:+.2f}%\n"
            
            return report
        except Exception as e:
            return f"⚠️ Report Error: {e}"
