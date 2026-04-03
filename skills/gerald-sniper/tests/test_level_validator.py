from core.level_validator import is_price_near_level


def test_price_near_level_true():
    assert is_price_near_level(100.0, 100.5, 1.0, 0.8) is True


def test_price_near_level_false():
    assert is_price_near_level(100.0, 101.0, 1.0, 0.8) is False


def test_price_near_level_invalid_atr():
    assert is_price_near_level(100.0, 100.1, 0.0) is False
    assert is_price_near_level(100.0, 100.1, -1.0) is False
    assert is_price_near_level(100.0, 100.1, None) is False
