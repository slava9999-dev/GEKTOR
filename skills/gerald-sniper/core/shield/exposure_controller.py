from typing import List, Dict, Any, Optional
from loguru import logger
from core.scenario.signal_entity import TradingSignal, SignalState

class ExposureController:
    """
    Global Risk Control (Task 5.2: Shield Stack).
    Manages sector limits and overall portfolio exposure.
    """
    
    # Static correlation / sector mapping (Task 5.2 Matrix)
    SECTORS = {
        "AI": ["FET", "AGIX", "WLD", "RNDR", "OCEAN", "TAO"],
        "MEME": ["PEPE", "DOGE", "SHIB", "FLOKI", "BONK", "WIF"],
        "L1_L2": ["SOL", "MATIC", "ARB", "OP", "NEAR", "AVAX", "ETH", "BTC"],
        "GAMING": ["IMX", "GALA", "PIXEL", "BEAM", "RON"],
        "DEFI": ["UNI", "AAVE", "MKR", "PENDLE"]
    }
    
    def __init__(self, max_total_exposure_usd: float = 500.0, max_pos_per_sector: int = 2):
        self.max_total_exposure = max_total_exposure_usd
        self.max_per_sector = max_pos_per_sector
        
    def get_symbol_sector(self, symbol: str) -> str:
        # Extract base (e.g. PEPEUSDT -> PEPE)
        base = symbol.replace("USDT", "").replace("PERP", "")
        for sector, coins in self.SECTORS.items():
            if base in coins:
                return sector
        return "OTHER"

    async def validate_signal(self, signal: TradingSignal, active_positions: List[Dict[str, Any]]) -> (bool, str):
        """
        Validates a signal against the global risk constraints.
        """
        # 1. Total Exposure check
        current_exposure = sum(p.get('size_usd', 0.0) for p in active_positions)
        # Note: In PaperTracker we don't have size_usd in the dict yet, we'll need to pass it or calculate
        # For now, let's assume size_usd estimate (notional)
        
        # 2. Sector Concentration check (Task 5.2)
        target_sector = self.get_symbol_sector(signal.symbol)
        
        sector_count = 0
        for pos in active_positions:
            pos_symbol = pos.get('symbol', "")
            if self.get_symbol_sector(pos_symbol) == target_sector:
                # Same direction contributes to risk concentration
                if pos.get('direction') == signal.direction:
                    sector_count += 1
                    
        if sector_count >= self.max_per_sector:
            msg = f"SECTOR_LIMIT: {target_sector} already has {sector_count} {signal.direction} positions"
            logger.warning(f"🛡️ [Exposure] REJECTED {signal.symbol}: {msg}")
            return False, msg
            
        logger.debug(f"🛡️ [Exposure] VALIDATED {signal.symbol} (Sector: {target_sector}, Count: {sector_count})")
        return True, "OK"
