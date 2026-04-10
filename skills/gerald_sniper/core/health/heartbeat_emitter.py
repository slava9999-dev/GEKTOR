# core/health/heartbeat_emitter.py
"""
[Audit 25.15] Cloud Heartbeat Emitter (Dead Man's Switch — Client Side).

Architecture:
    Sniper → HTTP PUT → API Gateway → DynamoDB (TTL=30s)
    
    If Sniper dies, the DynamoDB item expires automatically.
    Sentinel Lambda checks for item existence every 15s.
    No item = Server dead ≥ 30s = NUCLEAR_NUKE.

Design Decisions:
    - Uses aiohttp (already a project dependency) for non-blocking HTTP.
    - 2-second hard timeout per request (never blocks event loop).
    - Failures are silent (logged at DEBUG). Heartbeat is fire-and-forget.
    - Payload is minimal: just timestamp + active positions count.
"""

import os
import time
import asyncio
import json
from loguru import logger


class HeartbeatEmitter:
    """
    Lightweight async heartbeat that pings an external endpoint.
    The endpoint (API Gateway + DynamoDB) acts as the Dead Man's Switch store.
    """
    
    def __init__(
        self,
        endpoint_url: str | None = None,
        interval: float = 5.0,
        timeout: float = 2.0,
    ):
        # URL of API Gateway that writes to DynamoDB
        self.endpoint_url = endpoint_url or os.getenv("SENTINEL_HEARTBEAT_URL", "")
        self.interval = interval
        self.timeout = timeout
        self._session = None
        self._running = False
    
    async def _ensure_session(self):
        """Lazy-init aiohttp session (reuses TCP connections)."""
        if self._session is None or self._session.closed:
            try:
                import aiohttp
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                )
            except ImportError:
                # Fallback: no aiohttp available, use asyncio.to_thread + urllib
                self._session = None
    
    async def _send_ping(self, payload: dict) -> bool:
        """Send a single heartbeat ping. Returns True on success."""
        if not self.endpoint_url:
            return False
        
        try:
            await self._ensure_session()
            
            if self._session:
                # Primary path: aiohttp (zero-block)
                async with self._session.put(
                    self.endpoint_url,
                    json=payload,
                    ssl=True
                ) as resp:
                    return resp.status == 200
            else:
                # Fallback: urllib in thread pool (won't block event loop)
                import urllib.request
                data = json.dumps(payload).encode()
                req = urllib.request.Request(
                    self.endpoint_url, 
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="PUT"
                )
                await asyncio.to_thread(urllib.request.urlopen, req, timeout=self.timeout)
                return True
                
        except asyncio.TimeoutError:
            logger.debug("💓 [Heartbeat] Ping timeout (network lag). Non-critical.")
            return False
        except Exception as e:
            logger.debug(f"💓 [Heartbeat] Ping failed: {e}. Non-critical.")
            return False
    
    async def run(self, stop_event: asyncio.Event):
        """Main heartbeat loop. Runs until stop_event is set."""
        
        # 0. Sentinel Guard (Audit 25.17)
        if not self.endpoint_url or "your_sentinel_url" in self.endpoint_url:
            logger.warning("🔕 [Heartbeat] Sentinel URL not configured. Pulse DISENGAGED (Local/GHOST Mode).")
            return

        self._running = True
        consecutive_failures = 0
        MAX_SILENT_FAILURES = 10
        
        logger.info(f"💓 [Heartbeat] Emitter ACTIVE | Interval: {self.interval}s | Target: {self.endpoint_url[:40]}...")
        
        while not stop_event.is_set():
            try:
                # Build minimal payload
                from core.scenario.tactical_orchestrator import tactical_orchestrator
                active_positions = sum(
                    1 for s in tactical_orchestrator.active_signals.values()
                    if hasattr(s, 'state') and s.state.value == 'POSITION_OPEN'
                )
                
                payload = {
                    "ts": time.time(),
                    "pid": os.getpid(),
                    "pos": active_positions,
                    "mode": tactical_orchestrator.execution_mode,
                }
                
                success = await self._send_ping(payload)
                
                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    if consecutive_failures == MAX_SILENT_FAILURES:
                        logger.warning(
                            f"⚠️ [Heartbeat] {consecutive_failures} consecutive failures. "
                            f"Sentinel may trigger false positive!"
                        )
                
            except Exception as e:
                logger.error(f"❌ [Heartbeat] Loop error: {e}")
            
            await asyncio.sleep(self.interval)
        
        # Cleanup
        if self._session and not self._session.closed:
            await self._session.close()
        self._running = False
        logger.info("💓 [Heartbeat] Emitter stopped.")


# Global singleton
heartbeat_emitter = HeartbeatEmitter()
