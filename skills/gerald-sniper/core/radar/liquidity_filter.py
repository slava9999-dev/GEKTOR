import math

def calculate_liquidity_score(volume_24h: float) -> float:
    """
    liquidity_score = log10(volume_24h)
    """
    if volume_24h <= 0: return 0.0
    return math.log10(volume_24h)

def is_liquid(volume_24h: float) -> bool:
    """
    Фильтр: volume_24h >= 10M
    """
    return volume_24h >= 10_000_000
