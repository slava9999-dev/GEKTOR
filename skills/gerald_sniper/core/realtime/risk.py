# skills/gerald-sniper/core/realtime/risk.py
import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Any, Optional
from loguru import logger

@dataclass
class PortfolioConfig:
    """
    [GEKTOR v21.22] Global Risk Management Parameters.
    Targets Risk-Adjusted Returns through Volatility Parity.
    """
    capital_base_usd: float = 100_000.0  # Virtual or real equity
    target_annual_volatility: float = 0.20 # Focus: 20% annual SD on portfolio
    max_position_weight: float = 0.25      # Hard Cap: No more than 25% allocation per asset
    min_allocation_usd: float = 500.0      # Ignore signals that size too small

class VolatilitySizingEngine:
    """
    [GEKTOR v21.22] Risk-Parity Allocation Engine.
    - Sizing: Inversely proportional to realized asset volatility (Sigma).
    - Execution Instructions: Calculates Alpha Decay (TTL) and Slippage Boundaries.
    """
    def __init__(self, config: PortfolioConfig):
        self.config = config

    def calculate_risk_adjusted_signal(self, 
                                     asset: str, 
                                     current_price: float, 
                                     annualized_vol: float, 
                                     ev_score: float,
                                     l2_imbalance_ratio: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        Enriches a raw Alpha-Signal with Institutional Execution Directives.
        """
        if annualized_vol <= 0:
            logger.error(f"⚠️ [Risk] Invalid volatility {annualized_vol} for {asset}.")
            return None

        # 1. VOLATILITY PARITY (Risk-Adjusted Weight)
        # Weight = (Target Portfolio Vol) / (Current Asset Vol)
        base_weight = self.config.target_annual_volatility / annualized_vol
        
        # 2. CONVICTION SCALING
        # Scale weight by EV (Expected Value / Probability)
        adjusted_weight = base_weight * ev_score
        
        # 3. RISK CAPS & LIMITS
        capped_weight = min(adjusted_weight, self.config.max_position_weight)
        allocation_usd = capped_weight * self.config.capital_base_usd
        
        if allocation_usd < self.config.min_allocation_usd:
            logger.info(f"⏭️ [Risk] {asset} Sizing too small (${allocation_usd:.1f}). Skipping.")
            return None

        # 4. EXECUTION INSTRUCTIONS (The Bridge from Logic to Reality)
        # Calculate Microstructural Decay (TTL) and Entry Boundaries
        is_momentum_signal = l2_imbalance_ratio > 3.0
        
        # ALPHA VELOCITY (TTL): How long before this setup becomes 'Toxic Noise'?
        # Momentum signals decay fast (60s), mean-reversion slower (300s)
        ttl_seconds = 60 if is_momentum_signal else 300
        
        # SLIPPAGE BOUNDARY (Walk-away Price)
        # 0.25% Buffer or volatility-adjusted band
        slippage_buffer = 0.0025 
        max_entry = current_price * (1 + slippage_buffer) if ev_score > 0 else current_price * (1 - slippage_buffer)
        
        # EXECUTION ADVICE
        # If L2 is imbalanced, we must be Aggressive (Taker)
        suggested_method = "AGGRESSIVE_TAKER" if is_momentum_signal else "POST_ONLY_LIMIT"

        return {
            "asset": asset,
            "allocation_usd": round(allocation_usd, 2),
            "suggested_weight": round(capped_weight * 100, 2),
            "asset_vol": round(annualized_vol, 4),
            "execution": {
                "method": suggested_method,
                "max_entry_price": round(max_entry, 6),
                "ttl_seconds": ttl_seconds,
                "abort_condition": f"Price > {max_entry:.6f}" if ev_score > 0 else f"Price < {max_entry:.6f}"
            }
        }

# Global Risk Singleton
risk_engine = VolatilitySizingEngine(PortfolioConfig())
