# skills/gerald-sniper/core/realtime/invariants.py
import logging
from dataclasses import dataclass
from loguru import logger

@dataclass(frozen=True)
class CapacityInvariant:
    """
    [GEKTOR v21.30] Immutable Physical Laws of the Execution Engine.
    Ensures that Alpha is not destroyed by the system's own market footprint.
    - Max Consumption: Limits slippage by capping order size relative to L1 Liquidity.
    - Slippage Threshold: Hard abort if the gap between signal price and fill price is too wide.
    """
    MAX_L1_DEPTH_CONSUMPTION_PCT: float = 0.01  # Hard Cap: 1.0% of Order Book Depth
    CRITICAL_SLIPPAGE_THRESHOLD: float = 0.005  # 0.5% Slippage = Signal Invalidation

class InvariantEnforcer:
    """
    The 'Final Sieve' before signal routing.
    Protects the system from 'Self-Induced Liquidity Shock'.
    """
    @staticmethod
    def validate_execution_capacity(signal_ev: float, 
                                   capital_to_deploy: float, 
                                   current_l1_depth: float) -> bool:
        """
        O(1) Physical Validation. 
        Checks if the market can absorb our 'Snipe' without collapsing.
        """
        if current_l1_depth <= 0:
            logger.error("🛑 [CAPACITY] Zero L1 Liquidity detected! Execution impossible.")
            return False

        projected_consumption = capital_to_deploy / current_l1_depth
        
        # 1. LIQUIDITY CAPACITY CHECK
        if projected_consumption > CapacityInvariant.MAX_L1_DEPTH_CONSUMPTION_PCT:
            logger.critical(
                f"🔥 [CAPACITY BREACH] Capital ${capital_to_deploy:,.0f} | "
                f"Consumption: {projected_consumption:.2%} (Limit: {CapacityInvariant.MAX_L1_DEPTH_CONSUMPTION_PCT:.2%}). "
                f"Signal EV {signal_ev:.2f} is NOISE due to expected Slippage. SUPPRESSING."
            )
            # We raise a controlled error to stop the Egress flow for this specific signal
            return False
        
        logger.info(
            f"🟢 [CAPACITY CLEARED] Order safe: {projected_consumption:.2%} occupancy of L1 Depth. "
            f"Alpha integrity maintained."
        )
        return True

# Singleton Enforcer
enforcer = InvariantEnforcer()
