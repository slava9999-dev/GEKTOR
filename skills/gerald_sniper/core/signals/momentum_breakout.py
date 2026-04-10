"""
Gerald v4 — Momentum Breakout Detector (Radar-based).

Uses RadarV2Metrics to detect momentum breakouts.
Formatting delegated to unified formatter.
"""
from core.alerts.models import AlertEvent, AlertType
from core.radar.models import RadarV2Metrics


def check_momentum_breakout(m: RadarV2Metrics) -> AlertEvent | None:
    """
    Condition:
    volume_spike >= 4  AND  momentum >= 2%  AND  velocity >= 2
    """
    if m.volume_spike >= 4.0 and m.momentum_pct >= 2.0 and m.velocity >= 2.0:
        return AlertEvent(
            type=AlertType.TRIGGER_FIRED,
            symbol=m.symbol,
            payload={
                "type": "MOMENTUM_BREAKOUT",
                "pattern": "MOMENTUM_BREAKOUT",
                "level_price": m.price,
                "volume_spike": m.volume_spike,
                "momentum": m.momentum_pct,
                "velocity": m.velocity,
                "score": m.final_score,
            },
        )
    return None
