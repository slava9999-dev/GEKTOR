import sys
import os
from datetime import datetime

# Add skills/gerald-sniper to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.alerts.models import AlertEvent, AlertType
from core.alerts.formatters import format_alert_to_telegram

def make_proximity_event(**overrides) -> AlertEvent:
    payload = {
        "level_price":    0.27,
        "side":           "RESISTANCE",
        "distance_pct":   0.18,
        "confluence":     0.0,   # float - баг
        "touches":        2,
        "sources":        ["SWING:1h"],
        "strength":       3.5,
        "trade_velocity": 1.8,
        "buy_pressure":   0.62,
        "atr_pct":        3.2,
        "scenarios":      ["compression"]
    }
    payload.update(overrides)
    return AlertEvent(
        type=AlertType.LEVEL_PROXIMITY,
        symbol="SUIUSDT",
        level_id="test123",
        severity="INFO",
        payload=payload
    )

def test_proximity_float_confluence():
    """BUG-1: confluence=0.0 не должно крашить"""
    event = make_proximity_event(confluence=0.0)
    result = format_alert_to_telegram(event)
    assert result is not None
    assert "SUIUSDT" in result
    assert "Alert format error" not in result

def test_proximity_int_confluence():
    """confluence=2 должно давать 2 звезды"""
    event = make_proximity_event(confluence=2)
    result = format_alert_to_telegram(event)
    assert "⭐⭐" in result

def test_proximity_missing_fields():
    """Пустой payload не должен крашить"""
    event = AlertEvent(
        type=AlertType.LEVEL_PROXIMITY,
        symbol="TESTUSDT",
        level_id="test",
        severity="INFO",
        payload={}
    )
    result = format_alert_to_telegram(event)
    assert result is not None
    assert "TESTUSDT" in result

def test_proximity_none_values():
    """None значения не должны крашить"""
    event = make_proximity_event(
        confluence=None,
        distance_pct=None,
        atr_pct=None
    )
    result = format_alert_to_telegram(event)
    assert result is not None

def test_confluence_max_stars():
    """confluence=10 должно давать max 3 звезды"""
    event = make_proximity_event(confluence=10)
    result = format_alert_to_telegram(event)
    assert result.count("⭐") <= 3

if __name__ == "__main__":
    # Manual run support
    try:
        test_proximity_float_confluence()
        test_proximity_int_confluence()
        test_proximity_missing_fields()
        test_proximity_none_values()
        test_confluence_max_stars()
        print("✅ BUG-1 Unit Tests Passed!")
    except Exception as e:
        print(f"❌ BUG-1 Unit Tests Failed: {e}")
        import traceback
        traceback.print_exc()
