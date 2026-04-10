# core/radar/consensus.py
import asyncio
import logging
import time
from typing import Dict, Optional
from dataclasses import dataclass
from loguru import logger

@dataclass(slots=True, frozen=True)
class ShadowPulse:
    symbol: str
    exchange: str
    z_score_vol: float
    price_change_pct: float
    timestamp: float = field(default_factory=time.time)

class CrossExchangeValidator:
    """
    [GEKTOR v21.6] Consensus Gateway.
    Prevents entering signals driven by single-exchange API hallucinations.
    Uses Multiplexed Shadow Feeds (Binance/OKX).
    """
    def __init__(self, threshold_z: float = 1.2, max_shadow_age: float = 10.0):
        self.threshold_z = threshold_z
        self.max_shadow_age = max_shadow_age
        self.shadow_data: Dict[str, Dict[str, ShadowPulse]] = {}

    async def update_shadow(self, pulse: ShadowPulse):
        """Async update from Shadow Worker (WS Stream)."""
        if pulse.symbol not in self.shadow_data:
            self.shadow_data[pulse.symbol] = {}
        self.shadow_data[pulse.symbol][pulse.exchange] = pulse

    async def validate(self, symbol: str, local_vpin: float) -> bool:
        """
        GEKTOR Quorum: A signal is real if at least one external Top-Tier 
        exchange confirms anomalous volume activity.
        """
        shadows = self.shadow_data.get(symbol, {})
        now = time.time()
        
        if not shadows:
            # SAFETY: If Binance/OKX feeds are down, stay isolated but alert.
            logger.warning(f"⚠️ [Consensus] NO SHADOW DATA for {symbol}. Running isolated (High Risk).")
            return True 

        # Filter out stale shadow pulses (Data Age Guard)
        valid_shadows = [
            s for s in shadows.values() 
            if (now - s.timestamp) < self.max_shadow_age
        ]
        
        if not valid_shadows:
            logger.warning(f"⚠️ [Consensus] All shadows for {symbol} are STALE. Reverting to isolation.")
            return True

        # [V-Score Consensus]
        confirmed = any(s.z_score_vol > self.threshold_z for s in valid_shadows)
        
        if not confirmed:
            # Hallucination Detected: Local spike on Bybit with zero global context.
            logger.critical(f"🎭 [Consensus] [{symbol}] CROSS-EXCHANGE DIVERGENCE! Global volume inactive. BLOCKING.")
            return False
            
        logger.info(f"✅ [Consensus] [{symbol}] Validated by Global Quorum.")
        return True
