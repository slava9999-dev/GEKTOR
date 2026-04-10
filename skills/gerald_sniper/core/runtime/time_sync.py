import asyncio
import time
from loguru import logger
from typing import Optional

class TimeSynchronizer:
    """
    [Audit 5.10] Dynamic Calibration of Physical Clock.
    Eliminates NTP Clock Drift and incorporates RTT-Awareness (Latency-as-Age).
    """
    def __init__(self, rest_client):
        self.rest = rest_client
        self.clock_offset_ms = 0.0
        self.current_rtt_ms = 0.0
        self._lock = asyncio.Lock()
        self._started = False
        self._sync_task: Optional[asyncio.Task] = None

    async def start(self):
        if self._started: return
        self._started = True
        # Initial sync before background loop
        await self._sync_once()
        self._sync_task = asyncio.create_task(self.sync_loop())
        logger.info("⏱ [TimeSync] Continuous Clock Calibration ACTIVE.")

    async def _sync_once(self):
        try:
            t1 = time.time() * 1000
            # Bybit v5: GET /v5/market/time
            server_time_response = await self.rest.get_server_time()
            t2 = time.time() * 1000
            
            if not server_time_response or 'time' not in server_time_response:
                return

            exchange_ts = float(server_time_response['time'])
            
            async with self._lock:
                self.current_rtt_ms = t2 - t1
                # Real server time at t2 is estimated as (exchange_ts + half RTT)
                estimated_server_time = exchange_ts + (self.current_rtt_ms / 2.0)
                
                # Deviation: Estimated Server Time - Local System Time
                new_offset = estimated_server_time - t2
                
                # EMA filter to avoid jitter-induced oscillation (alpha=0.2)
                if self.clock_offset_ms == 0.0:
                    self.clock_offset_ms = new_offset
                else:
                    self.clock_offset_ms = (self.clock_offset_ms * 0.8) + (new_offset * 0.2)
        except Exception as e:
            logger.warning(f"⚠️ [TimeSync] Single sync failed: {e}")

    async def sync_loop(self, interval: int = 10):
        """Background heartbeat for clock drift compensation."""
        while self._started:
            await asyncio.sleep(interval)
            await self._sync_once()

    def get_calibrated_age(self, exchange_ts_ms: float) -> float:
        """
        O(1) calculation of true message age including server drift.
        age = (LocalNow + Drift) - ExchangeTS
        """
        local_now = time.time() * 1000
        calibrated_local = local_now + self.clock_offset_ms
        return calibrated_local - exchange_ts_ms

    @property
    def network_latency_ms(self) -> float:
        """Half of current RTT."""
        return self.current_rtt_ms / 2.0

# Global Singleton (Inject client during main bootstrap)
time_sync: Optional[TimeSynchronizer] = None
