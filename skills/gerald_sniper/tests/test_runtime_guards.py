from core.runtime_guards import is_valid_atr_abs, is_suspicious_atr_abs


def test_valid_atr_abs():
    assert is_valid_atr_abs(1.0) is True
    assert is_valid_atr_abs("2.5") is True


def test_invalid_atr_abs():
    assert is_valid_atr_abs(0) is False
    assert is_valid_atr_abs(-1) is False
    assert is_valid_atr_abs(None) is False


def test_suspicious_atr_abs():
    assert is_suspicious_atr_abs(100.0, 40.0) is True
    assert is_suspicious_atr_abs(100.0, 5.0) is False
