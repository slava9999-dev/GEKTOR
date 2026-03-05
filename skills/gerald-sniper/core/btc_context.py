import numpy as np
from utils.safe_math import safe_float

def calculate_rsi(prices: list[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    
    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    
    if down == 0:
        return 100.0
        
    rs = up / down
    rsi = np.zeros_like(prices)
    rsi[:period] = 100. - 100. / (1. + rs)
    
    for i in range(period, len(prices)):
        delta = deltas[i - 1]
        
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta
            
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        
        rs = up / down if down > 0 else 0
        rsi[i] = 100. - 100. / (1. + rs) if down > 0 else 100.
        
    return safe_float(rsi[-1])

def get_btc_context(btc_candles_h1: list[dict]) -> dict:
    if len(btc_candles_h1) < 14:
        return {'trend': 'FLAT', 'change_1h': 0.0, 'change_4h': 0.0, 'rsi': 50.0}
        
    closes = [c['close'] for c in btc_candles_h1]
    current = closes[-1]
    
    change_1h = (current - closes[-2]) / closes[-2] * 100 if len(closes) > 1 else 0
    change_4h = (current - closes[-5]) / closes[-5] * 100 if len(closes) > 4 else 0
    
    rsi = calculate_rsi(closes, 14)
    
    if change_4h > 2.0:
        trend = 'STRONG_UP'
    elif change_4h > 0.5:
        trend = 'UP'
    elif change_4h < -2.0:
        trend = 'STRONG_DOWN'
    elif change_4h < -0.5:
        trend = 'DOWN'
    else:
        trend = 'FLAT'
        
    return {
        'trend': trend,
        'change_1h': round(change_1h, 2),
        'change_4h': round(change_4h, 2),
        'rsi': round(rsi, 2)
    }
