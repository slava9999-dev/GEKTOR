from core.trigger_schema import validate_trigger_schema
from core.alert_circuit_breaker import AlertCircuitBreaker


def test_invalid_schema_rejected():
    ok, reason = validate_trigger_schema({
        "symbol": "BTCUSDT",
        "interval": "5",
        "candle_start_ts": 0,
        "trigger_type": "BREAKOUT",
        "level": 100.0,
    })
    assert ok is False
    assert reason == "invalid_candle_start_ts"


def test_circuit_breaker_blocks_repeated_alerts():
    cb = AlertCircuitBreaker(max_hits=1, window_seconds=60)
    fp = "BTCUSDT:5:1000:BREAKOUT:100.0"
    assert cb.hit(fp) is False
    assert cb.hit(fp) is True
