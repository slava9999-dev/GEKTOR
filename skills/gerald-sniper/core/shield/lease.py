import asyncio
import time
from loguru import logger

class LeaseService:
    """
    Distributed Lease Manager (Leadership Heartbeat).
    Main server must maintain a pulse in Redis.
    If the pulse stops, the Watcher assumes zombification or network tear.
    """
    def __init__(self, redis_client, lease_key: str = "tekton:leader:lease", ttl_ms: int = 1000):
        self.redis = redis_client
        self.lease_key = lease_key
        self.ttl_ms = ttl_ms
        self._running = False

    async def run(self, orchestrator=None):
        """High-priority heartbeat loop with Self-Termination (Audit 6.10)."""
        self._running = True
        logger.info(f"⏳ [Lease] Starting Leadership heartbeat (TTL: {self.ttl_ms}ms)")
        
        failure_start = None
        
        while self._running:
            try:
                # Update lease with millisecond precision
                await self.redis.set(self.lease_key, "ACTIVE", px=self.ttl_ms)
                # Success -> Reset failure timer
                failure_start = None
                await asyncio.sleep((self.ttl_ms / 1000.0) * 0.6)
            except Exception as e:
                if failure_start is None:
                    failure_start = time.time()
                
                elapsed = time.time() - failure_start
                logger.error(f"⚠️ [Lease] Heartbeat failure: {e} ({elapsed:.1f}s)")
                
                # [Audit 6.13] ZERO-AGONY SUICIDE:
                # If we lose connection to the Nerve Center for > 5s, we must 
                # step aside and let the Sentinel handle the risk. 
                # NO I/O. NO AWAIT. JUST EXIT.
                if elapsed > 5.0:
                    import os
                    # Log to stdout directly to avoid any blocking in logger queue
                    print(f"\n☢️ [FENCING] Redis unreachable for {elapsed:.1f}s. ABORTING PID {os.getpid()}.")
                    os._exit(1)
                
                await asyncio.sleep(0.5)

    def stop(self):
        self._running = False
