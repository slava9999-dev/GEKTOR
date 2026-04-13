# scripts/telemetry_probe.py
import asyncio
import asyncpg
import logging
from redis.asyncio import Redis
import time
import os

logger = logging.getLogger("GEKTOR.Probe")
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

async def run_telemetry_probe(pg_dsn: str, redis_url: str):
    """
    [GEKTOR v2.0] EXTERNAL HEALTH PROBE (O1 Observability).
    Monitors infrastructure without touching the Monolith's memory.
    Ensures Outbox performance and State Persistence health.
    """
    logger.info("📡 [Probe] Initializing external telemetry hub...")
    
    # Simple retry logic for initial connection
    redis = None
    conn = None
    
    try:
        redis = await Redis.from_url(redis_url)
        conn = await asyncpg.connect(pg_dsn)
        logger.success("✅ [Probe] Connections established. Monitoring O(1) metrics.")
        
        while True:
            start_ts = time.monotonic()
            
            # 1. Check Outbox Stagnation (Database Bloat risk)
            outbox_size = await conn.fetchval(
                "SELECT count(*) FROM outbox WHERE status = 'PENDING';"
            )
            
            # 2. Check Active Tracks in Redis (State Consistency)
            # Assuming keys pattern for individual or hash sets
            active_signals = await redis.execute_command('DBSIZE') # Total keys as a rough health check
            
            latency = (time.monotonic() - start_ts) * 1000
            
            # Clean logging
            lvl = logging.INFO
            if outbox_size > 5: lvl = logging.WARNING
            if outbox_size > 20: lvl = logging.CRITICAL
            
            logger.log(lvl, f"📊 [TELEMETRY] PENDING: {outbox_size} | DB_KEYS: {active_signals} | PROBE_LATENCY: {latency:.2f}ms")
            
            if outbox_size > 10:
                logger.critical("🚨 [BOTTLENECK] Outbox Relay LAGGING. Check Networking/VoIP status.")
                
            await asyncio.sleep(10) # 10s sampling window
            
    except asyncio.CancelledError:
        logger.info("🛑 [Probe] Telemetry session ended.")
    except Exception as e:
        logger.error(f"💥 [Probe] Fatal telemetry failure: {e}")
    finally:
        if conn: await conn.close()
        if redis: await redis.close()

if __name__ == "__main__":
    # Settings pulled from Environment or defaults
    PG_DSN = os.getenv("DATABASE_URL", "postgresql://quant:pass@localhost:5432/gektor_db")
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    try:
        asyncio.run(run_telemetry_probe(PG_DSN, REDIS_URL))
    except KeyboardInterrupt:
        pass
