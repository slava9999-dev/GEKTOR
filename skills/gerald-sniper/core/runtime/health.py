# core/runtime/health.py

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from collections import deque
from typing import Dict, List, Optional


@dataclass
class LogicGateStats:
    """Statistics for a specific evaluation gate."""
    total_received: int = 0
    rejected_regime: int = 0
    rejected_confidence: int = 0
    rejected_risk: int = 0
    rejected_volatility: int = 0
    approved: int = 0

@dataclass
class SystemHealth:
    """
    [Audit v6.0] Observability & Logic Pipeline Monitoring.
    Tracks HFT health from raw network to final execution logic.
    """
    # Network Health
    ws_reconnects: deque = field(default_factory=lambda: deque(maxlen=100))
    ws_parse_errors: deque = field(default_factory=lambda: deque(maxlen=100))
    stale_events: deque = field(default_factory=lambda: deque(maxlen=200))

    # Logic Health (The "Silent Failure" Dashboard)
    gates: LogicGateStats = field(default_factory=LogicGateStats)
    
    # Latency Tracking (Roadmap v1.2: Microsecond Precision)
    # Stores (timestamp, latency_ms)
    pipeline_latency: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    # Debugging on the Fly
    _debug_symbols: Dict[str, float] = field(default_factory=dict) # symbol -> expiry_ts

    def enable_debug(self, symbol: str, duration_sec: int = 300):
        """Enable verbose log injection for a specific symbol without restart."""
        self._debug_symbols[symbol] = time.time() + duration_sec

    def is_debug_active(self, symbol: str) -> bool:
        if symbol not in self._debug_symbols: return False
        if time.time() > self._debug_symbols[symbol]:
            del self._debug_symbols[symbol]
            return False
        return True

    def record_ws_reconnect(self) -> None:
        self.ws_reconnects.append(datetime.now(timezone.utc))

    def record_ws_parse_error(self) -> None:
        self.ws_parse_errors.append(datetime.now(timezone.utc))
        
    def record_latency(self, latency_ms: float) -> None:
        self.pipeline_latency.append((time.time(), latency_ms))

    def record_gate_event(self, gate_name: str, status: str):
        """
        Records where in the pipeline a signal died or passed.
        status: 'received', 'rejected_regime', 'rejected_conf', 'rejected_risk', 'approved'
        """
        if status == 'received': self.gates.total_received += 1
        elif status == 'rejected_regime': self.gates.rejected_regime += 1
        elif status == 'rejected_conf': self.gates.rejected_confidence += 1
        elif status == 'rejected_risk': self.gates.rejected_risk += 1
        elif status == 'approved': self.gates.approved += 1

    def get_avg_latency(self, window_sec: int = 300) -> float:
        if not self.pipeline_latency: return 0.0
        cutoff = time.time() - window_sec
        recent = [l for t, l in self.pipeline_latency if t > cutoff]
        return sum(recent) / len(recent) if recent else 0.0

    @property
    def status_report(self) -> dict:
        total = self.gates.total_received or 1
        return {
            "health": "DEGRADED" if self.is_ws_degraded else "OK",
            "latency_avg_ms": round(self.get_avg_latency(), 2),
            "throughput": {
                "received": self.gates.total_received,
                "approved": self.gates.approved,
                "conversion_pct": round((self.gates.approved / total) * 100, 1)
            },
            "rejections": {
                "regime": self.gates.rejected_regime,
                "confidence": self.gates.rejected_confidence,
                "risk": self.gates.rejected_risk
            },
            "network": {
                "reconnects_10m": self.count_last_n_minutes(self.ws_reconnects, 10),
                "parse_errors_10m": self.count_last_n_minutes(self.ws_parse_errors, 10)
            }
        }

    def count_last_n_minutes(self, bucket: deque, minutes: int) -> int:
        cutoff = time.time() - minutes * 60
        return sum(1 for ts in bucket if (ts.timestamp() if hasattr(ts, 'timestamp') else ts) > cutoff)

    @property
    def is_ws_degraded(self) -> bool:
        return self.count_last_n_minutes(self.ws_reconnects, 10) > 5

# Global Instance
health_monitor = SystemHealth()
