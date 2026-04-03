from typing import Optional, Dict
from loguru import logger

def calculate_trade_setup(
    symbol: str, 
    entry_price: float, 
    level_side: str, 
    atr_val: float,
    next_level_price: Optional[float] = None
) -> Dict[str, float]:
    """
    Calculates Stop Loss and Take Profit based on ATR and Structure.
    """
    # SL = 1.0 * ATR away from level
    if level_side == "SUPPORT":
        # Long entry
        sl = entry_price - (atr_val * 1.0)
        # TP: Use next level if available, otherwise 2.0 * ATR
        if next_level_price and next_level_price > entry_price:
            tp = next_level_price
        else:
            tp = entry_price + (atr_val * 2.5)
            
        risk = entry_price - sl
        reward = tp - entry_price
    else:
        # Short entry
        sl = entry_price + (atr_val * 1.0)
        if next_level_price and next_level_price < entry_price:
            tp = next_level_price
        else:
            tp = entry_price - (atr_val * 2.5)
            
        risk = sl - entry_price
        reward = entry_price - tp
        
    rr = reward / risk if risk > 0 else 0
    
    return {
        "entry": entry_price,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "risk_pct": (risk / entry_price) * 100 if entry_price > 0 else 0
    }
