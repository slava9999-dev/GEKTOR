import asyncio
import os
import sys
import time
import aiohttp
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

try:
    import asyncpg
except ImportError:
    asyncpg = None

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

class PreFlightDiagnostic:
    def __init__(self):
        self.failed = False

    async def check_env(self):
        logger.info("[1/4] Checking Environment Variables...")
        required_keys = ['DB_PASSWORD', 'KILL_SWITCH_KEY']
        for key in required_keys:
            if not os.getenv(key):
                logger.error(f"ENV CHECK FAILED: Missing absolute requirement: {key}")
                self.failed = True
                return
        logger.success("ENV CHECK: NOMINAL")

    async def check_postgres(self):
        if not asyncpg:
            logger.error("POSTGRES CHECK FAILED: 'asyncpg' library not found.")
            self.failed = True
            return

        logger.info("[2/4] Checking PostgreSQL Blackbox DB (Warming up)...")
        db_user = os.getenv("DB_USER", "tekton_admin")
        db_pass = os.getenv("DB_PASSWORD", "CRITICAL_SECURE_PASSWORD_CHANGE_IMMEDIATELY_9912")
        db_host = os.getenv("DB_HOST", "localhost")
        db_name = os.getenv("DB_NAME", "tekton_alpha")
        db_port = os.getenv("DB_PORT", "5433")
        
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                conn = await asyncio.wait_for(
                    asyncpg.connect(user=db_user, password=db_pass, host=db_host, port=db_port, database=db_name),
                    timeout=3.0
                )
                await conn.execute("SELECT 1;")
                
                timescale_check = await conn.fetch("SELECT extname FROM pg_extension WHERE extname = 'timescaledb';")
                if not timescale_check:
                    logger.warning("DB CHECK WARNING: 'timescaledb' extension not found. Metric compression is unavailable.")
                    
                await conn.close()
                logger.success(f"POSTGRES CHECK: NOMINAL (Connected on attempt {attempt})")
                return
                
            except Exception as e:
                if attempt == max_retries:
                    logger.error(f"POSTGRES CHECK FAILED (Timeout or Refused after {max_retries} attempts): {e}")
                    self.failed = True
                else:
                    logger.warning(f"POSTGRES starting... Attempt {attempt} failed: {e}. Retrying in 3s...")
                    await asyncio.sleep(3.0)

    async def check_redis(self):
        if not redis:
            logger.error("REDIS CHECK FAILED: 'redis.asyncio' library not found.")
            self.failed = True
            return

        logger.info("[3/4] Checking Redis Nerve Center...")
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6381))
        redis_password = os.getenv("REDIS_PASSWORD", None)
        redis_db = int(os.getenv("REDIS_DB", 0))
        
        try:
            r = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                password=redis_password,
                db=redis_db,
                decode_responses=True
            )
            await asyncio.wait_for(r.ping(), timeout=3.0)
            
            config = await asyncio.wait_for(r.config_get("maxmemory-policy"), timeout=3.0)
            policy = config.get("maxmemory-policy", "unknown")
            if policy not in ("noeviction", "allkeys-lru"):
                logger.warning(f"REDIS WARNING: maxmemory-policy is '{policy}'. Potential data loss. Recommend 'noeviction'.")
            
            await r.aclose()
            logger.success("REDIS CHECK: NOMINAL")
        except Exception as e:
            logger.error(f"REDIS CHECK FAILED: {e}")
            self.failed = True

    async def check_bybit_api(self):
        logger.info("[4/4] Checking Bybit Network, Proxy Latency & NTP Drift...")
        try:
            async with aiohttp.ClientSession() as session:
                start_time = time.time()
                async with session.get("https://api.bybit.com/v5/market/time", timeout=3.0) as resp:
                    resp_json = await resp.json()
                    end_time = time.time()
                    
                    latency_ms = (end_time - start_time) * 1000
                    
                    if latency_ms > 2000:
                        logger.error(f"API CHECK FAILED: Proxy latency too high ({latency_ms:.2f}ms). Must be < 2000ms for Sandbox.")
                        self.failed = True
                        return
                    elif latency_ms > 100:
                        logger.warning(f"API CHECK WARNING: High latency detected ({latency_ms:.2f}ms). Not viable for live production but allowed for testing.")
                    
                    server_time_ms = int(resp_json["time"])
                    local_time_ms = int(time.time() * 1000)
                    drift = abs(server_time_ms - local_time_ms) - (latency_ms / 2)
                    
                    if drift > 2000:
                        logger.error(f"API CHECK FAILED: Clock Drift detected ({drift:.2f}ms). Must be < 2000ms to avoid signature rejection.")
                        self.failed = True
                    else:
                        logger.success(f"API CHECK: NOMINAL (Latency: {latency_ms:.2f}ms, NTP Drift: ~{abs(drift):.2f}ms)")
                        
        except asyncio.TimeoutError:
            logger.error("API CHECK FAILED: Connection timed out (>3000ms). Dead proxy or DNS failure.")
            self.failed = True
        except Exception as e:
            logger.error(f"API CHECK FAILED: {e}")
            self.failed = True

    async def launch_sequence(self):
        logger.info("🚀 INITIATING PRE-FLIGHT DIAGNOSTICS FOR TEKTON_ALPHA 🚀")
        await asyncio.gather(
            self.check_env(),
            self.check_postgres(),
            self.check_redis(),
            self.check_bybit_api(),
            return_exceptions=True
        )
        
        if self.failed:
            logger.critical("❌ PRE-FLIGHT FAILED. LAUNCH SEQUENCE ABORTED. ❌")
            sys.exit(1)
        else:
            logger.success("✅ ALL SYSTEMS NOMINAL. GATEWAY OPEN. ✅")
            sys.exit(0)

if __name__ == "__main__":
    checker = PreFlightDiagnostic()
    asyncio.run(checker.launch_sequence())
