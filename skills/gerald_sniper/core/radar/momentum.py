def calculate_momentum_pct(open_p: float, close_p: float) -> float:
    """
    Расчет:
    momentum = abs(price_change_5m)
    """
    if open_p <= 0: return 0.0
    return abs(close_p - open_p) / open_p * 100

def score_momentum(momentum_pct: float) -> float:
    """
    Score:
    <0.5% → 0
    0.5-1% → 5
    1-2% → 10
    >2% → 15
    """
    if momentum_pct < 0.5: return 0.0
    if momentum_pct < 1.0: return 5.0
    if momentum_pct < 2.0: return 10.0
    return 15.0
