def is_price_near_level(
    current_price: float,
    level_price: float,
    atr_abs: float,
    proximity_atr_mult: float = 0.8,
) -> bool:
    """
    Distance check in ABSOLUTE price units.
    Example:
    current_price=100, level=101, atr_abs=2 -> near if distance <= 1.6
    """
    if atr_abs is None or atr_abs <= 0:
        return False
    return abs(current_price - level_price) <= atr_abs * proximity_atr_mult
