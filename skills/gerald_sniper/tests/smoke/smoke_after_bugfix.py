"""
Smoke тест после исправления трёх багов.
Запускается вручную.
"""
import asyncio
import sys
import os
from dataclasses import dataclass

# Add skills/gerald-sniper to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.alerts.formatters import format_alert_to_telegram
from core.alerts.models import AlertEvent, AlertType
from core.levels.models import get_level_price, get_level_side, LevelCluster, LevelSide
from services.level_service import LevelService

@dataclass
class FakeStoredLevel:
    price: float
    side:  str

def test_smoke():
    print("\n=== SMOKE TEST BUG FIXES ===\n")

    # BUG-1 check
    event = AlertEvent(
        type=AlertType.LEVEL_PROXIMITY,
        symbol="SUIUSDT",
        level_id="test",
        severity="INFO",
        payload={
            "level_price":    1.23,
            "side":           "RESISTANCE",
            "distance_pct":   0.18,
            "confluence":     0.0, # float - should not crash
            "touches":        2,
            "sources":        ["SWING:1h"],
            "strength":       3.5,
            "trade_velocity": 1.8,
            "buy_pressure":   0.62,
            "atr_pct":        3.2,
            "scenarios":      []
        }
    )
    result = format_alert_to_telegram(event)
    assert result, "BUG-1: formatter returned empty string"
    assert "Alert format error" not in result, "BUG-1: Format error detected"
    print(f"✅ BUG-1 OK: {result[:50]}...")

    # BUG-2 check
    level = FakeStoredLevel(price=1.23, side="SUPPORT")
    price = get_level_price(level)
    side  = get_level_side(level)
    assert price == 1.23, "BUG-2: price incorrect"
    assert side == "SUPPORT", "BUG-2: side incorrect"
    print(f"✅ BUG-2 OK: price={price}, side={side}")

    # BUG-3 check
    svc = LevelService(None)
    clusters = [
        LevelCluster(
            symbol="X",
            price=float(i),
            side=LevelSide.RESISTANCE,
            sources=[],
            touches_total=1, # Should be filtered
            confluence_score=0,
            strength=0.5, # Should be filtered
            tolerance_pct=0.003,
            level_id=str(i)
        )
        for i in range(20)
    ]
    # Add one good level
    clusters.append(LevelCluster(
        symbol="X", price=10.0, side=LevelSide.RESISTANCE, 
        sources=["S:1h"], touches_total=3, confluence_score=1, 
        strength=5.0, tolerance_pct=0.001, level_id="good"
    ))
    
    result = svc._filter_levels(clusters, 10.0)
    assert len(result) <= 8, "BUG-3: too many levels"
    assert any(c.price == 10.0 for c in result), "BUG-3: good level missing"
    print(f"✅ BUG-3 OK: levels after filtering={len(result)}")

    print("\n=== ALL SMOKE TESTS PASSED ===")

if __name__ == "__main__":
    test_smoke()
