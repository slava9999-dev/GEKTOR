def calculate_proximity_threshold(atr: float, price: float) -> float:
    """
    New Proximity Threshold:
    threshold = ATR * 0.6
    Clamp: min = 0.2%, max = 1.2%
    """
    if price <= 0: return 0.005 # 0.5% default
    
    # ATR based threshold
    base_threshold_pct = (atr * 0.6 / price) * 100
    
    # Clamp
    return max(0.2, min(1.2, base_threshold_pct))
