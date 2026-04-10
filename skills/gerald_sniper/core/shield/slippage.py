from decimal import Decimal
from typing import List, Tuple, Union
from loguru import logger

class DynamicSlippageEstimator:
    """
    Gerald v4 — Institutional Slippage Estimation.
    Calculates VWAP fill based on order book depth to prevent 'Equity Illusion' in GHOST_MODE.
    """
    
    @staticmethod
    def calculate_vwap_fill(
        order_qty: float, 
        orderbook_levels: List[List[float]], # [[Price, Volume], ...]
        is_long: bool = True
    ) -> float:
        """
        Calculates VWAP price for a given quantity across L2 orderbook levels.
        """
        if not orderbook_levels or order_qty <= 0:
            return 0.0

        remaining_qty = order_qty
        total_cost = 0.0

        for level in orderbook_levels:
            if len(level) < 2: continue
            
            level_price = float(level[0])
            level_vol = float(level[1])
            
            if remaining_qty <= 0:
                break
                
            fill_vol = min(remaining_qty, level_vol)
            total_cost += fill_vol * level_price
            remaining_qty -= fill_vol

        if remaining_qty > 0:
            # OB DEPTH EXHAUSTED: Apply a severe penalty for the remainder
            # v4.1: Assume rest fills 5% worse than the last visible price
            last_price = float(orderbook_levels[-1][0])
            penalty_multiplier = 1.05 if is_long else 0.95
            penalty_price = last_price * penalty_multiplier
            
            total_cost += remaining_qty * penalty_price
            
            if remaining_qty > (order_qty * 0.5):
                logger.warning(f"⚠️ [GHOST] Liquidity Crisis! Filling {remaining_qty/order_qty*100:.1f}% of order beyond observed depth.")

        return total_cost / order_qty

    @staticmethod
    def get_slippage_bps(expected_price: float, actual_price: float) -> int:
        """Helper to calculate slippage in basis points."""
        if expected_price <= 0: return 0
        return int(abs((actual_price - expected_price) / expected_price) * 10000)
