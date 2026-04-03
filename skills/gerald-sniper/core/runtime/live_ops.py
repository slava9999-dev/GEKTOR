import asyncio
from loguru import logger
from decimal import Decimal
from typing import Optional
from core.metrics.execution_profiler import TradeExecutionProfiler
from core.shield.execution_guard import L1ExecutionGuard

class LiveOpsMonitor:
    """
    Asynchronous Heartbeat Monitor (Audit 15.8).
    Aggregates performance metrics, slippage decay, and risk limit buffers.
    """
    def __init__(self, profiler: TradeExecutionProfiler, guard: L1ExecutionGuard, report_interval: int = 3600):
        self.profiler = profiler
        self.guard = guard
        from core.shield.risk_engine import risk_guard
        self.risk_guard = risk_guard
        self.report_interval = report_interval
        self._is_running = True

    async def run_telemetry_loop(self):
        """Periodically outputs a high-level health report."""
        logger.info("👁️ [LIVE OPS] Telemetry Heartbeat ACTIVE.")
        
        while self._is_running:
            try:
                await asyncio.sleep(self.report_interval)
                
                # Snapshot current vitals with Guard Clauses (Audit 25.6)
                if not self.profiler or not hasattr(self.profiler, 'trades_analyzed'):
                    logger.debug("Telemetry profiler warming up...")
                    continue
                
                if not self.guard or not hasattr(self.guard, 'max_drawdown'):
                    logger.debug("Telemetry guard warming up...")
                    continue

                total_trades = getattr(self.profiler, 'trades_analyzed', 0)
                total_slip_usd = getattr(self.profiler, 'total_slippage_usd', Decimal('0'))
                daily_loss = getattr(self.guard, '_daily_loss', Decimal('0'))
                max_dd = getattr(self.guard, 'max_drawdown', Decimal('0'))
                
                # Math
                avg_slip_usd = (total_slip_usd / total_trades) if total_trades > 0 else Decimal('0')
                load_factor = (daily_loss / max_dd) * 100 if max_dd > 0 else Decimal('0')
                
                # Total load includes both active and reserved slots
                signals = self.risk_guard.signals_generated if self.risk_guard else 0
                active_slots = len(self.risk_guard.active_positions) + len(self.risk_guard.in_flight_reservations) if self.risk_guard else 0

                # The Vital Report
                report = (
                    f"\n{'='*48}\n"
                    f"📊 [TEKTON_ALPHA VITALS | HOURLY REPORT]\n"
                    f"  · Signals Identified: {signals}\n"
                    f"  · Armor Slots (Active+Res): {active_slots}\n"
                    f"  · Executed Fills: {total_trades}\n"
                    f"  · Aggregated Slippage: -${total_slip_usd:.2f}\n"
                    f"  · Avg Slip/Trade: -${avg_slip_usd:.4f}\n"
                    f"  · Drawdown Load: {load_factor:.1f}% (${daily_loss:.2f} / ${max_dd:.2f})\n"
                    f"  · RateLimit (Tokens): {self.guard._tokens:.1f}/{self.guard._capacity}\n"
                    f"{'='*48}"
                )
                
                if load_factor > 80:
                    logger.critical(f"⚠️ [RISK ALERT] Drawdown approaching Kill Switch! {report}")
                elif total_trades > 0 and avg_slip_usd > 1.0: # Arbitrary "high slip" threshold in USD
                    logger.warning(f"☣️ [ALPHA DECAY] Execution quality degrading. {report}")
                else:
                    logger.info(report)
                    
            except Exception as e:
                logger.error(f"❌ [LIVE OPS] Telemetry loop crashed: {e}")
                await asyncio.sleep(60)

    def stop(self):
        self._is_running = False
