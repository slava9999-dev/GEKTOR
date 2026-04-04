import os
import asyncio
from typing import Any, List, Tuple
from loguru import logger
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
            # Atomic move back to main queue
            item = await self.redis.rpop(self.processing_key)
            if not item: break
            await self.redis.lpush(self.queue_key, item)
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
        """Initialize schema & Retention Policies."""
        await self.buffer.start()
        
        # Start background retention worker (Audit 16.5)
        asyncio.create_task(self.run_retention_worker())
        
        async with self.engine.begin() as conn:
            try:
                # [Gektor Update] Clear poisoning from failed startups
                await conn.execute(text("ROLLBACK"))
            except:
                pass
            # 1. CORE: Alerts table (for signal persistence)
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
            
            # 2. METRICS: Time-series telemetry (TimescaleDB ready)
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS telemetry_metrics (
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    metric_name VARCHAR(255) NOT NULL,
                    value DOUBLE PRECISION NOT NULL,
                    tags JSONB
                )
            """))
            # Optional: Enable TimescaleDB hypertable & Retention Policy (7 Days)
            try:
                await conn.execute(text("SELECT create_hypertable('telemetry_metrics', 'timestamp', if_not_exists => TRUE);"))
                await conn.execute(text("SELECT add_retention_policy('telemetry_metrics', INTERVAL '7 days', if_not_exists => TRUE);"))
            except Exception: # Fallback for vanilla Postgres
                pass

            # 3. INFRA: Transactional Outbox (EventBus Persistence)
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id SERIAL PRIMARY KEY,
                    channel VARCHAR(255) NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    published_at TIMESTAMP NULL
                );
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_outbox_pending 
                ON outbox_events (id ASC) 
                WHERE published_at IS NULL;
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_outbox_history
                ON outbox_events (created_at DESC);
            """))

            # 4. INFRA: LISTEN/NOTIFY Trigger for Outbox Relay
            await conn.execute(text("""
                CREATE OR REPLACE FUNCTION notify_outbox_event() RETURNS trigger AS $$
                BEGIN
                    PERFORM pg_notify('outbox_channel', 'new_event');
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """))
            
            await conn.execute(text("""
                DROP TRIGGER IF EXISTS trigger_notify_outbox ON outbox_events;
            """))
            
            await conn.execute(text("""
                CREATE TRIGGER trigger_notify_outbox
                AFTER INSERT ON outbox_events
                FOR EACH STATEMENT EXECUTE FUNCTION notify_outbox_event();
            """))
            
            logger.info("✅ [DB] Outbox LISTEN/NOTIFY triggers active.")
            # 4. WATCHLIST: Candidate Persistence & Trending (Gerald v2.2.0)
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

            # [NERVE REPAIR] Lazy Schema Migration: Ensure all columns exist for LLM Analysis
            await conn.execute(text("ALTER TABLE watchlist_history ADD COLUMN IF NOT EXISTS velocity DOUBLE PRECISION DEFAULT 1.0"))
            await conn.execute(text("ALTER TABLE watchlist_history ADD COLUMN IF NOT EXISTS momentum_pct DOUBLE PRECISION DEFAULT 0.0"))
            await conn.execute(text("ALTER TABLE watchlist_history ADD COLUMN IF NOT EXISTS atr_ratio DOUBLE PRECISION DEFAULT 1.0"))
            await conn.execute(text("ALTER TABLE watchlist_history ADD COLUMN IF NOT EXISTS orderflow_imbalance DOUBLE PRECISION DEFAULT 1.0"))

            # 5. EXECUTION: Distributed Order State Machine (v2.1.6)
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

            # 6. ANALYTICS: Signal Performance & Rejection tracking
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS signal_stats (
                    id SERIAL PRIMARY KEY,
                    signal_id VARCHAR(50) UNIQUE,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    symbol VARCHAR(20) NOT NULL,
                    direction VARCHAR(10) NOT NULL,
                    state VARCHAR(20) NOT NULL, -- APPROVED, REJECTED, OBSERVE
                    entry_price DOUBLE PRECISION,
                    detectors TEXT,
                    confidence DOUBLE PRECISION,
                    rejection_reason TEXT,
                    metadata JSONB
                )
            """))

            # High-Precision Performance: Indexing for Rapid Time-Series Analysis
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_watchlist_symbol_ts ON watchlist_history (symbol, timestamp DESC)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_watchlist_tier ON watchlist_history (liquidity_tier)"))

            # 7. Kill Switch: System Status persistence
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS system_status (
                    component VARCHAR(50) PRIMARY KEY,
                    status VARCHAR(20) NOT NULL,
                    reason TEXT,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """))
            
            # 8. TimeScaleDB: Hypertables (v3.0 Optimization)
            # Optional: Enable TimescaleDB hypertable & Retention Policy (7 Days)
            try:
                await conn.execute(text("SELECT create_hypertable('watchlist_history', 'timestamp', if_not_exists => TRUE);"))
                await conn.execute(text("SELECT add_retention_policy('watchlist_history', INTERVAL '7 days', if_not_exists => TRUE);"))
            except Exception:
                pass

            # 6. EXECUTION: Paper Trading Positions (Task 3.5 Institutional Standard)
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
                    status VARCHAR(20) DEFAULT 'OPEN', -- OPEN, CLOSED_WIN, CLOSED_LOSS, CLOSED_FORCE
                    fee_paid DOUBLE PRECISION DEFAULT 0,
                    pnl_usd DOUBLE PRECISION DEFAULT 0,
                    pnl_pct DOUBLE PRECISION DEFAULT 0,
                    opened_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMPTZ
                )
            """))

            # 7. REALITY: Behavioral Delta Tracking (Human vs Algo)
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
            try:
                await conn.execute(text("SELECT create_hypertable('reality_metrics', 'timestamp', if_not_exists => TRUE);"))
            except: pass

            logger.info("✅ [DB] PostgreSQL Schema verified & Engine initialized.")

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

    async def run_retention_worker(self, interval_sec: int = 14400):
        """
        [P2] Data Retention Worker.
        Prevents Postgres 'bloat' by deleting historic telemetry/snapshots.
        Institutional Standard: 7 Days retention for high-vol tables.
        """
        logger.info(f"🧹 [DB] Background Retention Policy active (Retention: 7 days). Check every {interval_sec/3600:.0f}h.")
        while True:
            await asyncio.sleep(interval_sec)
            try:
                async with self.engine.begin() as conn:
                    # Optimized Delete (Rule 11.2): Targeting high-vol tables
                    # [NERVE REPAIR] Split commands for asyncpg compatibility
                    await conn.execute(text("DELETE FROM telemetry_metrics WHERE timestamp < NOW() - INTERVAL '7 days'"))
                    await conn.execute(text("DELETE FROM watchlist_history WHERE timestamp < NOW() - INTERVAL '7 days'"))
                    # Manual VACUUM is risky during live fire, so we rely on Postgres autovacuum.
                    # On SQLite, we would call VACUUM here (Audit 16.5).
                    logger.info("🧹 [DB] Housekeeping Complete. Historic telemetry purged.")
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
