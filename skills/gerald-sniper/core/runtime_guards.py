def is_valid_atr_abs(atr_abs) -> bool:
    try:
        value = float(atr_abs)
    except Exception:
        return False

    return value > 0.0

def is_suspicious_atr_abs(price: float, atr_abs: float) -> bool:
    """
    Heuristic only.
    Suspicious if ATR is absurdly large relative to price.
    """
    try:
        price = float(price)
        atr_abs = float(atr_abs)
    except Exception:
        return True

    if price <= 0 or atr_abs <= 0:
        return True

    ratio = atr_abs / price
    return ratio > 0.30
