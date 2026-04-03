from core.alert_circuit_breaker import AlertCircuitBreaker


def test_circuit_breaker_trips_after_limit():
    cb = AlertCircuitBreaker(max_hits=2, window_seconds=60)
    fp = "BTCUSDT:5:1000:BREAKOUT:60000.0"

    assert cb.hit(fp) is False
    assert cb.hit(fp) is False
    assert cb.hit(fp) is True
