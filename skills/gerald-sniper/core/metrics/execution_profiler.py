from decimal import Decimal
from loguru import logger
import time
from typing import Optional

class TradeExecutionProfiler:
    """
    HFT Telemetry & Alpha Decay Analysis (Audit 15.6).
    Analyzes execution quality, slippage in Basis Points (bps), and RTT.
    """
    def __init__(self):
        self.total_slippage_usd = Decimal('0')
        self.trades_analyzed = 0
        self.last_slippage_bps = 0.0

    def profile_execution(self, 
                          symbol: str, 
                          is_long: bool, 
                          signal_price: Decimal, 
                          exec_price: Decimal, 
                          qty: Decimal, 
                          rtt_ms: float):
        """
        Calculates slippage against signal (theoretical) price.
        Examines execution toxicity.
        """
        try:
            # 1. Basis Points Calculation (1 bps = 0.01%)
            if is_long:
                slippage_bps = ((exec_price - signal_price) / signal_price) * 10000
            else:
                slippage_bps = ((signal_price - exec_price) / signal_price) * 10000

            # 2. Financial Impact
            slippage_cost_usd = abs(exec_price - signal_price) * qty
            self.total_slippage_usd += slippage_cost_usd
            self.trades_analyzed += 1
            self.last_slippage_bps = float(slippage_bps)

            # 3. Toxicity Alerts
            msg = (f"📈 [EXEC] {symbol} | Side: {'BUY' if is_long else 'SELL'} | "
                   f"RTT: {rtt_ms:.2f}ms | Slip: {slippage_bps:.1f} bps | Cost: ${slippage_cost_usd:.2f}")

            if slippage_bps > 5.0: # Breach of 0.05% slippage threshold
                logger.warning(f"☣️ [TOXIC FILL] {msg}. High adverse selection detected.")
            elif slippage_bps < -1.0: # Negative slippage is better than expected (Rare in HFT)
                logger.info(f"🍀 [NEGATIVE SLIPPAGE] {msg}. Pure Alpha.")
            else:
                logger.info(f"⚡ [CLEAN FILL] {msg}")

            return float(slippage_bps)
        except Exception as e:
            logger.error(f"❌ [Profiler] Error profiling {symbol}: {e}")
            return 0.0
