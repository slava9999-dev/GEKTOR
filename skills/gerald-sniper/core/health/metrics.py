# core/health/metrics.py

import time
import asyncio
from typing import Dict
from loguru import logger
from core.events.nerve_center import bus
from core.events.events import DetectorEvent, SignalEvent

class SystemMetrics:
    """
    Real-time telemetry and metrics collector (Roadmap Step 6).
    """
    
    def __init__(self):
        self.counters: Dict[str, int] = {
            "events_total": 0,
            "signals_detected": 0,
            "signals_approved": 0,
            "positions_opened": 0,
            "telegram_sent": 0,
            "errors": 0
        }
        self.detector_counts: Dict[str, int] = {}
        self._start_ts = time.time()
        self._task = None

    async def start(self):
        """Register subscribers and start reporting loop."""
        bus.subscribe(DetectorEvent, self._on_detector_event)
        bus.subscribe(SignalEvent, self._on_signal_event)
        self._task = asyncio.create_task(self._report_loop())
        logger.info("📊 Metrics Telemetry System ACTIVE")

    async def _on_detector_event(self, event: DetectorEvent):
        self.counters["events_total"] += 1
        d_type = event.detector
        self.detector_counts[d_type] = self.detector_counts.get(d_type, 0) + 1

    async def _on_signal_event(self, event: SignalEvent):
        self.counters["signals_approved"] += 1

    def increment(self, key: str):
        if key in self.counters:
            self.counters[key] += 1

    async def _report_loop(self):
        """Log health and traffic metrics every 5 minutes."""
        while True:
            await asyncio.sleep(300)
            uptime_min = (time.time() - self._start_ts) / 60
            logger.info(
                f"📊 [TELEMETRY] Uptime: {uptime_min:.1f}m | "
                f"Events: {self.counters['events_total']} | "
                f"Approved: {self.counters['signals_approved']} | "
                f"Errors: {self.counters['errors']}"
            )
            if self.detector_counts:
                logger.debug(f"📊 [D-STATS] {self.detector_counts}")

# Global singleton
metrics = SystemMetrics()
