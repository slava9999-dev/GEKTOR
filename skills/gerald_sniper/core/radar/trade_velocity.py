import numpy as np

def calculate_trade_velocity(trades_1m: int, avg_trades_10m: float) -> float:
    """
    Расчет:
    velocity = trades_1m / avg_trades_10m
    """
    if avg_trades_10m <= 0: return 0.0
    return trades_1m / avg_trades_10m

def score_trade_velocity(velocity: float) -> float:
    """
    Score:
    <1.2 → 0
    1.2-1.5 → 5
    1.5-2 → 10
    2-3 → 20
    >3 → 25
    """
    if velocity < 1.2: return 0.0
    if velocity < 1.5: return 5.0
    if velocity < 2.0: return 10.0
    if velocity < 3.0: return 20.0
    return 25.0
