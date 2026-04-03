# core/realtime/regime.py

from enum import Enum
from loguru import logger
from .models import SymbolLiveState

class MarketRegime(str, Enum):
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    RANGE = "RANGE"
    VOLATILITY_EXPANSION = "VOL_UP"
    UNKNOWN = "UNKNOWN"

class RegimeClassifier:
    """
    Classifies the market state using real-time price action and volume (Roadmap Step 3).
    """
    
    @staticmethod
    def classify(state: SymbolLiveState, btc_trend: str = "FLAT", 
                 atr_5m: float = None, atr_sma: float = None) -> MarketRegime:
        """
        Dynamic Microstructure Regime Classification (Task 4.2).
        Eliminates static 0.4% hardcode in favor of ATR-normalized bands.
        """
        try:
            price = state.current_price
            m5_ma = state.get_moving_average(300) # 5min MA
            
            # 1. VOLATILITY EXPANSION (Task 4.2: VOL_UP logic)
            # If current ATR spike is > 50% above its 14-period average
            if atr_5m and atr_sma and atr_5m > (atr_sma * 1.5):
                return MarketRegime.VOLATILITY_EXPANSION

            # Fallback to standard price deviation if ATR is missing
            if not atr_5m or m5_ma <= 0:
                dist_pct = ((price - m5_ma) / m5_ma) * 100 if m5_ma > 0 else 0
                if dist_pct > 0.4: return MarketRegime.TREND_UP
                if dist_pct < -0.4: return MarketRegime.TREND_DOWN
                return MarketRegime.RANGE

            # 2. DYNAMIC TREND BANDS (Rule: MA +/- 1.2 * ATR)
            upper_band = m5_ma + (atr_5m * 1.2)
            lower_band = m5_ma - (atr_5m * 1.2)

            if price > upper_band:
                return MarketRegime.TREND_UP
            elif price < lower_band:
                return MarketRegime.TREND_DOWN
            
            # 3. RANGE (Price within volatility-adjusted channel)
            return MarketRegime.RANGE

        except Exception as e:
            logger.error(f"❌ [Regime] Classification failure: {repr(e)}")
            return MarketRegime.UNKNOWN
