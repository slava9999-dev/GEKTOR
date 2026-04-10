# core/infrastructure/supervisor.py
import asyncio
import time
import aiohttp
import logging
from loguru import logger

class OOB_HeartbeatSupervisor:
    """
    [GEKTOR v21.13] Out-of-Band System Guardian.
    Standalone task that monitors Outbox health and reports to external
    watchdogs (Healthchecks.io) and secondary push channels (NTFY/Pushover).
    Threshold for 'Silent Death': 90 seconds.
    """
    def __init__(self, db, secondary_api, ping_url: str = None, alert_threshold: int = 90):
        self.db = db
        self.secondary_api = secondary_api
        self.ping_url = ping_url
        self.alert_threshold = alert_threshold
        self.is_silence_alerted = False

    async def run(self, shutdown_event: asyncio.Event):
        logger.info(f"🦾 [Supervisor] OOB Heartbeat Active. External: {self.ping_url is not None}")
        
        async with aiohttp.ClientSession() as session:
            while not shutdown_event.is_set():
                try:
                    # 1. Internal Health Audit: Check for jammed Emergency Lane
                    oldest_ts = await self.db.get_oldest_pending_emergency_ts()
                    
                    if oldest_ts:
                        staleness = time.time() - oldest_ts
                        if staleness > self.alert_threshold and not self.is_silence_alerted:
                            await self._send_sos_alert(staleness)
                        elif staleness < self.alert_threshold:
                            # Reset alert if queue cleared
                            if self.is_silence_alerted:
                                 logger.info("🦾 [Supervisor] Emergency Queue Cleared. Status Green.")
                                 self.is_silence_alerted = False

                    # 2. External Heartbeat Pulse (Healthchecks.io / UptimeRobot)
                    # If this fails for N cycles, the external service will alert the User.
                    if self.ping_url:
                        async with session.get(self.ping_url, timeout=5.0) as resp:
                            if resp.status != 200:
                                logger.error(f"🦾 [Supervisor] Heartbeat Ping Failed: HTTP {resp.status}")

                except Exception as e:
                    logger.error(f"🦾 [Supervisor] Heartbeat Loop Exception: {e}")

                await asyncio.sleep(30) # High-precision 30s check

    async def _send_sos_alert(self, staleness: float):
        """Dispatches Emergency SOS via Secondary Channel (Bypassing Telegram)."""
        logger.critical(f"🆘 [Supervisor] CRITICAL SILENCE DETECTED: {staleness:.0f}s. TELEGRAM_BLOCK suspected.")
        
        msg = (
            f"🚨 GEKTOR SOS: Emergency Alert BLOCKED (>90s)! "
            f"Telegram Blackout suspected. Use Exchange App / API for manual exit NOW!"
        )
        
        success = await self.secondary_api.send_message(msg)
        if success:
            self.is_silence_alerted = True
            logger.warning("🆘 [Supervisor] SOS Alert dispatched via Secondary Channel.")
