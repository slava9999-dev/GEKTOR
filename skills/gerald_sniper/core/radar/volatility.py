def calculate_atr_ratio(atr_5m: float, atr_1h: float) -> float:
    """
    Расчет:
    ATR_ratio = ATR_5m / ATR_1h
    """
    if atr_1h <= 0: return 1.0
    return atr_5m / atr_1h

def score_volatility_expansion(ratio: float) -> float:
    """
    Score:
    <1.1 → 0
    1.1-1.3 → 5
    1.3-1.6 → 10
    >1.6 → 15
    """
    if ratio < 1.1: return 0.0
    if ratio < 1.3: return 5.0
    if ratio < 1.6: return 10.0
    return 15.0
