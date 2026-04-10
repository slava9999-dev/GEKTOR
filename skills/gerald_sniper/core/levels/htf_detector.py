import numpy as np
from loguru import logger
from typing import List
from core.levels.models import HTFLevel, LevelSide, LevelSource

TF_WEIGHTS = {
    "15m": 1.0,
    "60":  1.5,
    "240": 2.5,
    "D":   4.0,
}

def detect_swings(
    highs: List[float],
    lows: List[float],
    symbol: str,
    timeframe: str,
    window: int = 3,
    min_touches: int = 1
) -> List[HTFLevel]:
    """
    Implements Fractal Swing detection (Scenario 1 core).
    Swing High: high[i] is highest in its neighborhood sized 2*window.
    """
    if len(highs) < window * 2 + 1:
        return []

    results: List[HTFLevel] = []
    tf_weight = TF_WEIGHTS.get(timeframe, 1.0)

    swing_highs: List[float] = []
    swing_lows:  List[float] = []

    for i in range(window, len(highs) - window):
        # Swing High
        is_sh = all(highs[i] >= highs[i - k] for k in range(1, window + 1)) and \
                all(highs[i] >= highs[i + k] for k in range(1, window + 1))
        if is_sh:
            swing_highs.append(highs[i])

        # Swing Low
        is_sl = all(lows[i] <= lows[i - k] for k in range(1, window + 1)) and \
                all(lows[i] <= lows[i + k] for k in range(1, window + 1))
        if is_sl:
            swing_lows.append(lows[i])

    # Group swing highs into resistance levels
    for group in _group_prices(swing_highs, tolerance=0.003):
        price = float(np.median(group))
        touches = len(group)
        if touches < min_touches: continue
        results.append(HTFLevel(
            symbol=symbol, timeframe=timeframe, side=LevelSide.RESISTANCE,
            price=price, touches=touches, source=LevelSource.SWING,
            strength=touches * tf_weight, raw_prices=group
        ))

    # Group swing lows into support levels
    for group in _group_prices(swing_lows, tolerance=0.003):
        price = float(np.median(group))
        touches = len(group)
        if touches < min_touches: continue
        results.append(HTFLevel(
            symbol=symbol, timeframe=timeframe, side=LevelSide.SUPPORT,
            price=price, touches=touches, source=LevelSource.SWING,
            strength=touches * tf_weight, raw_prices=group
        ))

    return results


def detect_kde(closes: List[float], symbol: str, timeframe: str) -> List[HTFLevel]:
    """
    Finds value areas using a simple density histogram (Digash Scenario 1 logic).
    Equivalent to KDE but more performant for H1/H4.
    """
    if len(closes) < 20: return []
    
    # 50 bins over the range
    low, high = min(closes), max(closes)
    if high == low: return []
    
    hist, bin_edges = np.histogram(closes, bins=50)
    
    # Threshold for a peak: > (mean + 1.5 * std)
    mean_h = np.mean(hist)
    std_h = np.std(hist)
    threshold = mean_h + 1.5 * std_h
    
    results = []
    for i in range(len(hist)):
        if hist[i] > threshold:
            # Peak detected in this bin
            price = float((bin_edges[i] + bin_edges[i+1]) / 2)
            results.append(HTFLevel(
                symbol=symbol, timeframe=timeframe, side=LevelSide.SUPPORT, # Density is neutral
                price=price, touches=int(hist[i]), source=LevelSource.KDE,
                strength=float(hist[i]), raw_prices=[price]
            ))
            
    return results


def detect_week_extremes(highs: List[float], lows: List[float], symbol: str) -> List[HTFLevel]:
    """Detects 7-day high and low points."""
    if not highs or not lows: return []
    
    w_high = max(highs)
    w_low = min(lows)
    
    return [
        HTFLevel(
            symbol=symbol, timeframe="WEEK", side=LevelSide.RESISTANCE,
            price=float(w_high), touches=1, source=LevelSource.WEEK_EXT,
            strength=40.0, raw_prices=[w_high]
        ),
        HTFLevel(
            symbol=symbol, timeframe="WEEK", side=LevelSide.SUPPORT,
            price=float(w_low), touches=1, source=LevelSource.WEEK_EXT,
            strength=40.0, raw_prices=[w_low]
        )
    ]


def detect_round_numbers(current_price: float, symbol: str) -> List[HTFLevel]:
    """Generates psychological round number levels nearby."""
    if current_price <= 0: return []
    
    # Simple step logic based on price magnitude
    if current_price > 10000: step = 1000
    elif current_price > 1000: step = 100
    elif current_price > 100: step = 10
    elif current_price > 1: step = 1
    elif current_price > 0.1: step = 0.1
    else: step = 0.01
    
    above = (int(current_price / step) + 1) * step
    below = int(current_price / step) * step
    
    return [
        HTFLevel(
            symbol=symbol, timeframe="PSY", side=LevelSide.RESISTANCE,
            price=float(above), touches=1, source=LevelSource.ROUND,
            strength=25.0, raw_prices=[above]
        ),
        HTFLevel(
            symbol=symbol, timeframe="PSY", side=LevelSide.SUPPORT,
            price=float(below), touches=1, source=LevelSource.ROUND,
            strength=25.0, raw_prices=[below]
        )
    ]

def _group_prices(prices: List[float], tolerance: float) -> List[List[float]]:
    """Groups close price points into potential value zones."""
    # Filter out invalid prices (<= 0)
    valid_prices = [p for p in prices if p > 0]
    if not valid_prices: return []
    
    sorted_p = sorted(valid_prices)
    groups = [[sorted_p[0]]]
    
    for p in sorted_p[1:]:
        ref = groups[-1][0]
        # ref is guaranteed > 0 because of filtering
        if abs(p - ref) / ref < tolerance:
            groups[-1].append(p)
        else:
            groups.append([p])
    return groups
