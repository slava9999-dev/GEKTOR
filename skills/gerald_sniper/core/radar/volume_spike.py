import numpy as np

def calculate_volume_spike(v5_candles_turnover: list[float]) -> float:
    """
    Расчет:
    volume_5m = sum(volume last 5m)
    avg_volume_1h = mean(volume 5m candles last hour)
    volume_spike = volume_5m / avg_volume_1h
    """
    if len(v5_candles_turnover) < 2: return 0.0
    
    volume_5m = v5_candles_turnover[-1]
    avg_volume_1h = np.mean(v5_candles_turnover[:-1])
    
    if avg_volume_1h <= 0: return 0.0
    return volume_5m / avg_volume_1h

def score_volume_spike(spike: float) -> float:
    """
    Score:
    <1.2 → 0
    1.2-1.5 → 5
    1.5-2 → 10
    2-3 → 20
    3-5 → 30
    >5 → 40
    """
    if spike < 1.2: return 0.0
    if spike < 1.5: return 5.0
    if spike < 2.0: return 10.0
    if spike < 3.0: return 20.0
    if spike < 5.0: return 30.0
    return 40.0
