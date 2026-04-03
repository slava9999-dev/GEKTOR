import numpy as np

def calculate_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int) -> float:
    """Calculates ATR using numpy."""
    if len(highs) < period + 1: return 0.0
    
    tr = np.maximum(highs[1:] - lows[1:], 
                    np.maximum(abs(highs[1:] - closes[:-1]), 
                               abs(lows[1:] - closes[:-1])))
    return float(np.mean(tr[-period:]))

def safe_float(val) -> float:
    try:
        return float(val) if val else 0.0
    except:
        return 0.0
