import asyncio
import json
from loguru import logger
from core.events.nerve_center import bus
from core.events.events import EmergencyAlertEvent

class RuntimeConfigController:
    """
    [Gektor v6.20] Async Hot Swap Controller via Redis Pub/Sub.
    Atomic mode switching without disk I/O or process restart.
    """
    def __init__(self, redis_client, execution_engine):
        self.redis = redis_client
        self.engine = execution_engine
        self._task = None
        self._running = False

    async def start(self):
        """Starts the background Pub/Sub listener."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._listen_loop())
        logger.info("🎧 [ModeControl] Hot Swap Controller ACTIVE. Waiting for Redis Pub/Sub...")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _listen_loop(self):
        """Listens for 'tekton:command:exec_mode' overrides."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("tekton:command:exec_mode")
        
        try:
            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] == "message":
                    payload = message["data"].decode("utf-8").upper()
                    # Accepted modes: ADVISORY (HIL), AUTO (Autonomous), PANIC (Force Close + Exit)
                    if payload in ["ADVISORY", "AUTO", "PANIC"]:
                        self.engine.execution_mode = payload
                        logger.critical(f"🔄 [HOT SWAP] Execution mode atomically switched to: {payload}")
                        
                        # Trigger system-wide alert
                        bus.publish(EmergencyAlertEvent(
                            message=f"🛡️ EXECUTION MODE CHANGED: {payload}",
                            severity="CRITICAL"
                        ))
                    else:
                        logger.error(f"⚠️ [ModeControl] Invalid mode payload received: {payload}")
        except Exception as e:
            logger.error(f"❌ [ModeControl] Pub/Sub listener error: {e}")
            await asyncio.sleep(5)
            # Auto-restart loop
            if self._running:
                asyncio.create_task(self._listen_loop())
