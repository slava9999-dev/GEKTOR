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
    def classify(state: SymbolLiveState, btc_trend: str = "FLAT") -> MarketRegime:
        try:
            # 1. Volatility check (60s window)
            vol = state.get_volatility(60)
            if vol > 1.2: # Increased threshold for expansion
                return MarketRegime.VOLATILITY_EXPANSION

            # 2. Trend check (300s window)
            m5_ma = state.get_moving_average(300)
            dist_pct = ((state.current_price - m5_ma) / m5_ma) * 100 if m5_ma > 0 else 0
            
            # Factor in BTC momentum (Roadmap Stage 3 Enhancement)
            btc_up = "UP" in btc_trend.upper()
            btc_down = "DOWN" in btc_trend.upper()

            if dist_pct > 0.4:
                # If coin is UP but BTC is crashing, it's likely a fakeout or high volatility
                if btc_down and dist_pct < 1.0:
                    return MarketRegime.VOLATILITY_EXPANSION
                return MarketRegime.TREND_UP
            elif dist_pct < -0.4:
                # If coin is DOWN but BTC is mooning, it might be a temporary dip
                if btc_up and dist_pct > -1.0:
                    return MarketRegime.RANGE
                return MarketRegime.TREND_DOWN
                
            return MarketRegime.RANGE
        except Exception as e:
            logger.error(f"Regime classification error: {e}")
            return MarketRegime.UNKNOWN
