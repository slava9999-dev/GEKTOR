import time
from loguru import logger
from typing import Optional
from core.realtime.market_state import market_state

class LeadLagEstimator:
    """
    HFT Lead-Lag V2 (Audit 16.7).
    Aligns exchange-level timestamps and filters noise using Volume Delta (CVD).
    """
    def __init__(self, max_ts_divergence_ms: int = 50, cvd_threshold: float = 0.5):
        self.max_ts_divergence_ms = max_ts_divergence_ms
        self.cvd_threshold = cvd_threshold # Minimal CVD movement to count as 'Confirming pressure'

    def is_bybit_lagging_toxically(self, symbol: str, signal_side: str, bybit_price: float, threshold_bps: float = 5.0) -> bool:
        """
        Returns True if Bybit is lagging behind Binance in a risky way.
        v4.3: Micro-CVD Momentum + Jitter Tolerance.
        """
        bin_state = market_state.get_state(symbol, exchange="binance")
        byb_state = market_state.get_state(symbol, exchange="bybit")

        if not bin_state or not byb_state:
            return False 
            
        # 1. TCP Jitter / Stream Desync Check (Audit 16.8)
        # If Best-Bid/Ask updated but we haven't seen a trade in the last 10ms,
        # we might be at the edge of a race condition.
        jitter_ms = abs(bin_state.exchange_ts - (bin_state.cvd_history[-1][0] if bin_state.cvd_history else 0))
        is_volume_confirmed = jitter_ms < 15 # Tolerance for stream desync

        # 2. Global Sync Gap (Bybit vs Binance)
        ts_diff = abs(bin_state.exchange_ts - byb_state.exchange_ts)
        if ts_diff > self.max_ts_divergence_ms:
            logger.warning(f"⚠️ [LEAD-LAG] Data Stale (Divergence: {ts_diff}ms)")
            return True

        # 3. Microprecedence + Micro-CVD Confirmation
        if signal_side.upper() == "BUY":
            if not bin_state.bba_bid or not byb_state.bba_ask: return False
            
            delta_bps = ((bin_state.bba_bid - byb_state.bba_ask) / byb_state.bba_ask) * 10000.0
            
            if delta_bps > threshold_bps:
                # If volume lags or direction is opposite, it's Spoofing/Noise
                if is_volume_confirmed and bin_state.current_cvd > self.cvd_threshold:
                    logger.info(f"🚫 [TOXIC FLOW] Binance UP confirmed by CVD ({bin_state.current_cvd:.2f}).")
                    return True
                else:
                    logger.debug(f"⚖️ [LEAD-LAG] Binance UP but unconfirmed (Jitter: {jitter_ms}ms, CVD: {bin_state.current_cvd:.2f})")
                    return False

        elif signal_side.upper() == "SELL":
            if not bin_state.bba_ask or not byb_state.bba_bid: return False
            delta_bps = ((byb_state.bba_bid - bin_state.bba_ask) / byb_state.bba_bid) * 10000.0
            
            if delta_bps > threshold_bps:
                if is_volume_confirmed and bin_state.current_cvd < -self.cvd_threshold:
                    logger.info(f"🚫 [TOXIC FLOW] Binance DOWN confirmed by CVD ({bin_state.current_cvd:.2f}).")
                    return True
                else:
                    logger.debug(f"⚖️ [LEAD-LAG] Binance DOWN but unconfirmed.")
                    return False

        return False
