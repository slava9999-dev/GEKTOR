# core/metrics/metrics.py

import time
import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from loguru import logger
from collections import defaultdict
from core.events.nerve_center import bus
from core.events.events import DetectorEvent, SignalEvent, ExecutionEvent, BaseEvent

@dataclass
class RadarScanEvent(BaseEvent):
    elapsed: float = 0.0
    found_count: int = 0
    symbols: List[str] = field(default_factory=list)

@dataclass
class RejectionEvent(BaseEvent):
    """Published when a signal is rejected (for metrics tracking)."""
    symbol: str = ""
    direction: str = ""
    reason: str = ""

@dataclass
class SystemMetrics:
    signals_generated: int = 0
    signals_approved: int = 0
    signals_rejected: int = 0
    executions_success: int = 0
    executions_failed: int = 0
    
    # v5.0: Extended metrics
    approved_long: int = 0
    approved_short: int = 0
    rejected_reasons: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    hourly_approved: List[int] = field(default_factory=lambda: [0] * 24)  # 24-hour slots
    confidence_buckets: Dict[str, int] = field(default_factory=lambda: defaultdict(int))  # "0.6-0.7": count
    
    detector_hits: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    avg_confidence: float = 0.0
    radar_scan_times: List[float] = field(default_factory=list)
    
    start_time: float = field(default_factory=time.time)

class MetricsEngine:
    """
    Roadmap Step 6: Telemetry & Metrics Layer (v5.0 Edition).
    Extended with direction ratio, rejection breakdown, hourly rates.
    """
    def __init__(self):
        self.metrics = SystemMetrics()
        self.metrics.detector_hits = defaultdict(int)
        self.metrics.rejected_reasons = defaultdict(int)
        self.metrics.confidence_buckets = defaultdict(int)
        self._conf_sum = 0.0
        self._task = None

    async def start(self):
        bus.subscribe(DetectorEvent, self._on_detector)
        bus.subscribe(SignalEvent, self._on_signal)
        bus.subscribe(ExecutionEvent, self._on_execution)
        bus.subscribe(RadarScanEvent, self._on_radar)
        bus.subscribe(RejectionEvent, self._on_rejection)
        
        self._task = asyncio.create_task(self._report_loop())
        logger.info("📊 [Metrics] Telemetry Layer v5.0 ACTIVE")

    def _on_detector(self, event: DetectorEvent):
        self.metrics.detector_hits[event.detector] += 1
        self.metrics.signals_generated += 1

    def _on_signal(self, event: SignalEvent):
        self.metrics.signals_approved += 1
        self._conf_sum += event.confidence
        self.metrics.avg_confidence = self._conf_sum / self.metrics.signals_approved
        
        # v5.0: Track direction ratio
        if event.direction == "LONG":
            self.metrics.approved_long += 1
        else:
            self.metrics.approved_short += 1
        
        # v5.0: Track hourly distribution
        try:
            from datetime import datetime
            hour = datetime.now().hour
            self.metrics.hourly_approved[hour] += 1
        except Exception:
            pass
        
        # v5.0: Confidence distribution
        conf = event.confidence
        if conf < 0.65:
            bucket = "<0.65"
        elif conf < 0.70:
            bucket = "0.65-0.70"
        elif conf < 0.75:
            bucket = "0.70-0.75"
        elif conf < 0.80:
            bucket = "0.75-0.80"
        else:
            bucket = "0.80+"
        self.metrics.confidence_buckets[bucket] += 1

    def _on_execution(self, event: ExecutionEvent):
        if event.status == "SUCCESS":
            self.metrics.executions_success += 1
        else:
            self.metrics.executions_failed += 1

    def _on_rejection(self, event: RejectionEvent):
        """v5.0: Track rejection reasons."""
        self.metrics.signals_rejected += 1
        # Categorize reason
        reason = event.reason
        if "BTC" in reason:
            self.metrics.rejected_reasons["BTC Filter"] += 1
        elif "Range Market" in reason:
            self.metrics.rejected_reasons["Range Filter"] += 1
        elif "Confluence Timeout" in reason:
            self.metrics.rejected_reasons["Confluence Timeout"] += 1
        elif "Regime Conflict" in reason:
            self.metrics.rejected_reasons["Regime Conflict"] += 1
        elif "Radar Score" in reason:
            self.metrics.rejected_reasons["Low Radar Score"] += 1
        elif "Level Proximity" in reason:
            self.metrics.rejected_reasons["Level Too Far"] += 1
        elif "Cooldown" in reason or "watchlist" in reason.lower():
            self.metrics.rejected_reasons["Cooldown/Watchlist"] += 1
        else:
            self.metrics.rejected_reasons["Other"] += 1
            
    def _on_radar(self, event: RadarScanEvent):
        self.metrics.radar_scan_times.append(event.elapsed)
        if len(self.metrics.radar_scan_times) > 50:
            self.metrics.radar_scan_times.pop(0)

    async def _report_loop(self):
        """Standard reporting cycle (15 minutes)."""
        while True:
            await asyncio.sleep(900) # Every 15 mins for production stability
            try:
                report = self.get_report()
                for line in report.split("\n"):
                    logger.info(line)
            except Exception as e:
                logger.error(f"Metrics reporting error: {e}")

    def get_report(self) -> str:
        m = self.metrics
        uptime_h = (time.time() - m.start_time) / 3600
        hit_rate = (m.signals_approved / m.signals_generated * 100) if m.signals_generated > 0 else 0
        avg_scan = sum(m.radar_scan_times) / len(m.radar_scan_times) if m.radar_scan_times else 0
        
        # v5.0: LONG/SHORT ratio
        total_dir = m.approved_long + m.approved_short
        long_pct = (m.approved_long / total_dir * 100) if total_dir > 0 else 0
        short_pct = (m.approved_short / total_dir * 100) if total_dir > 0 else 0
        
        # Hourly rate
        signals_per_hour = m.signals_approved / uptime_h if uptime_h > 0 else 0
        
        # v5.1: Sentiment snapshot
        try:
            from core.realtime.sentiment import market_sentiment
            sentiment_line = f"Sentiment: {market_sentiment.score:+d} ({market_sentiment.label})"
        except Exception:
            sentiment_line = "Sentiment: N/A"
        
        report = [
            "📈 --- GERALD PRODUCTION METRICS (v5.1) ---",
            f"Uptime: {uptime_h:.2f}h",
            f"{sentiment_line}",
            f"Signals: {m.signals_generated} generated | {m.signals_approved} approved ({hit_rate:.1f}%) | {m.signals_rejected} rejected",
            f"Avg Confidence: {m.avg_confidence:.2f}",
            f"Direction: LONG {m.approved_long} ({long_pct:.0f}%) | SHORT {m.approved_short} ({short_pct:.0f}%)",
            f"Hourly Rate: {signals_per_hour:.1f} approved/hour",
            f"Executions: ✅ {m.executions_success} | ❌ {m.executions_failed}",
            f"Avg Radar Scan: {avg_scan:.2f}s",
        ]
        
        # Rejection breakdown
        if m.rejected_reasons:
            report.append("Rejection Breakdown:")
            total_rej = sum(m.rejected_reasons.values())
            for reason, count in sorted(m.rejected_reasons.items(), key=lambda x: x[1], reverse=True):
                pct = count / total_rej * 100 if total_rej > 0 else 0
                report.append(f"  - {reason}: {count} ({pct:.0f}%)")
        
        # Confidence distribution
        if m.confidence_buckets:
            report.append("Confidence Distribution:")
            for bucket in ["<0.65", "0.65-0.70", "0.70-0.75", "0.75-0.80", "0.80+"]:
                count = m.confidence_buckets.get(bucket, 0)
                if count > 0:
                    report.append(f"  - {bucket}: {count}")
        
        # Detector hits
        report.append("Detector Breakdown:")
        for det, count in sorted(m.detector_hits.items(), key=lambda x: x[1], reverse=True):
            report.append(f"  - {det}: {count}")
            
        return "\n".join(report)

# Global instances
metrics = MetricsEngine()
RadarScanEvent = RadarScanEvent # Export for scanner
RejectionEvent = RejectionEvent # Export for orchestrator
