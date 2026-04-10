from core.trigger_schema import validate_trigger_schema


def test_valid_trigger_schema():
    ok, reason = validate_trigger_schema({
        "symbol": "BTCUSDT",
        "interval": "5",
        "candle_start_ts": 1000,
        "trigger_type": "BREAKOUT",
        "level": 60000.0,
    })
    assert ok is True
    assert reason == "ok"


def test_missing_field_fails():
    ok, reason = validate_trigger_schema({
        "symbol": "BTCUSDT",
        "interval": "5",
        "trigger_type": "BREAKOUT",
        "level": 60000.0,
    })
    assert ok is False
    assert reason == "missing_candle_start_ts"


def test_nonpositive_level_fails():
    ok, reason = validate_trigger_schema({
        "symbol": "BTCUSDT",
        "interval": "5",
        "candle_start_ts": 1000,
        "trigger_type": "BREAKOUT",
        "level": 0,
    })
    assert ok is False
    assert reason == "nonpositive_level"
