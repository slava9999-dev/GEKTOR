
import asyncio
import time
from decimal import Decimal
from typing import Dict, Optional, Any
from loguru import logger

class HedgeSagaException(Exception):
    pass

class NetworkTearException(Exception):
    """Смертельный разрыв связи во время выполнения Саги."""
    pass

class SmartOrderRouter:
    """Оркестратор кросс-биржевых транзакций (Паттерн Saga)."""
    
    def __init__(self, primary_adapter: Any, db: Any, secondary_adapter: Any = None):
        self.fragile = primary_adapter   # Bybit
        self.db = db
        self.liquid = secondary_adapter  # Binance
        self.rollback_timeout = 0.2      # HFT Hardened: 200ms limit

    async def execute_delta_neutral_entry(self, symbol: str, qty: float, price: float, 
                                        signal_id: str) -> bool:
        """
        Consecutive execution with compensation (Saga).
        v5.3: HFT-Hardened Rollback (200ms budget).
        """
        d_qty = Decimal(str(qty))
        
        logger.info(f"⚔️ [SOR] Initiating Delta-Neutral Entry: {symbol} | Size: {qty}")
        
        # Step 0: Lead-Lag V3 Filter (Audit 16.7)
        if not await self._check_pre_execution_neutrality(symbol, "BUY", price):
            logger.warning(f"🛑 [SOR] Lead-Lag Rejection (Toxic Flow): {symbol}. Aborting.")
            return False

        # Step 1: Attack Fragile Leg (Bybit)
        order_link_id = f"sor_f_{signal_id}_{int(time.time())}"
        try:
            res_a = await self.fragile.execute_order(
                symbol=symbol, side="Buy", qty=qty, price=price, 
                order_link_id=order_link_id
            )
            
            if not res_a or res_a.get("status") != "FILLED":
                logger.warning(f"⚠️ [SOR] Fragile Leg Rejected: {res_a}. Aborting.")
                return False
                
            leg_a_fill_price = float(res_a.get("avg_price", price))
            logger.info(f"🛡️ [SOR] Leg A FILLED @ {leg_a_fill_price:.4f}. Balancing Hedge Leg B...")

        except Exception as e:
            logger.error(f"❌ [SOR] Fragile Leg ERROR: {e}. No risk created.")
            return False

        # Step 2: Attack Liquid Leg (Binance)
        hedge_link_id = f"sor_l_{signal_id}_{int(time.time())}"
        limit_price_b = leg_a_fill_price * 0.9980 # 20 bps band
        
        try:
            res_b = await self.liquid.execute_order(
                symbol=symbol, side="Sell", qty=qty, price=limit_price_b,
                type="LIMIT", tif="IOC",
                order_link_id=hedge_link_id
            )
            
            status_b = res_b.get("status")
            if status_b == "UNCERTAIN":
                # HFT Reconciliation: Aggressive 100ms window
                res_b = await self._reconcile_uncertain_leg_hft(symbol, hedge_link_id)
                status_b = res_b.get("status") if res_b else "REJECTED"

            filled_qty_b = Decimal(str(res_b.get("filled_qty", "0.0")))
            
            # Step 2.1: Economic Symmetry (Audit 16.8)
            hedged_ratio = float(filled_qty_b / d_qty) if d_qty > 0 else 0
            if status_b != "FILLED" and hedged_ratio < 0.5:
                logger.critical(f"🚨 [SOR] Fee-Coverage Failed ({hedged_ratio:.1%}). Initiating SYMMETRIC Rollback.")
                await self._execute_symmetric_rollback(symbol, d_qty, filled_qty_b, "Buy")
                return False

            if filled_qty_b >= (d_qty - Decimal("0.000001")):
                logger.info("🟢 [SOR] Saga SUCCESS. Full Delta Neutral established.")
                return True
            
            # Step 3: Hardened Partial Rollback
            unhedged_qty = d_qty - filled_qty_b
            logger.critical(f"⚠️ [SOR] Divergence! Remainder: {unhedged_qty}. Rolling back Leg A portion.")
            await self._execute_symmetric_rollback(symbol, unhedged_qty, Decimal("0.0"), "Buy")
            return filled_qty_b > 0

        except Exception as e:
            if isinstance(e, NetworkTearException): raise
            logger.critical(f"🚨 [SOR] Leg B FATAL Error: {e}. Global Symmetric Rollback.")
            await self._execute_symmetric_rollback(symbol, d_qty, Decimal("0.0"), "Buy")
            return False

    async def _reconcile_uncertain_leg_hft(self, symbol: str, order_link_id: str) -> Optional[Dict]:
        """
        HFT Aggressive Reconciliation. 
        Budget: max 150ms.
        """
        intervals = [0.05, 0.1]
        for attempt, wait_sec in enumerate(intervals, 1):
            await asyncio.sleep(wait_sec)
            try:
                status_data = await asyncio.wait_for(
                    self.liquid.get_order_status(symbol, order_link_id=order_link_id),
                    timeout=0.1
                )
                if status_data:
                    st = status_data.get("status")
                    if st in ("FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED", "REJECTED"):
                        return {
                            "status": "FILLED" if st in ("FILLED", "PARTIALLY_FILLED") else "REJECTED",
                            "filled_qty": status_data.get("filled_qty", 0.0),
                            "avg_price": status_data.get("avg_price", 0.0)
                        }
            except Exception: pass
        return None

    async def _check_pre_execution_neutrality(self, symbol: str, side: str, bybit_price: float) -> bool:
        from core.shield.lead_lag import LeadLagEstimator
        self.lead_lag = LeadLagEstimator(max_ts_divergence_ms=50, cvd_threshold=0.1)
        if self.lead_lag.is_bybit_lagging_toxically(symbol, side, bybit_price, threshold_bps=10.0):
            return False
        return True

    async def _execute_symmetric_rollback(self, symbol: str, leg_a_qty: Decimal, leg_b_qty: Decimal, leg_a_side: str):
        """
        Hardened Symmetric Rollback (Audit 16.9).
        Uses LIMIT IOC with aggressive price offset to control slippage while maximizing fill speed.
        """
        from core.realtime.market_state import market_state
        state = market_state.get_state(symbol, exchange="bybit")
        
        tasks = []
        if leg_a_qty > 0:
            # Calculate aggressive price for Bybit rollback (Fragile Leg)
            # Long Leg A means we must Sell to rollback.
            rb_side_a = "Sell" if leg_a_side.upper() == "BUY" else "Buy"
            rb_price_a = 0.0
            if state and state.bba_bid > 0:
                # Use 1% offset to effectively act as market but with IOC safety
                rb_price_a = state.bba_bid * 0.99 if rb_side_a == "Sell" else state.bba_ask * 1.01

            tasks.append(self._execute_with_timeout(self.fragile, {
                "symbol": symbol, "side": rb_side_a, "qty": float(leg_a_qty), 
                "price": rb_price_a, "type": "LIMIT", "tif": "IOC", "reduce_only": True
            }))
            
        if leg_b_qty > 0:
            # For Binance (Liquid Leg), we likely just need a Market or aggressive IOC
            rb_side_b = "Buy" if leg_a_side.upper() == "BUY" else "Sell"
            tasks.append(self._execute_with_timeout(self.liquid, {
                "symbol": symbol, "side": rb_side_b, "qty": float(leg_b_qty), 
                "price": 0.0, "type": "MARKET", "reduce_only": True
            }))
            
        if not tasks: return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, res in enumerate(results):
            if isinstance(res, (asyncio.TimeoutError, TimeoutError)):
                logger.critical(f"💀 [NETWORK TEAR] Rollback Step {i} TIMEOUT! Infrastructure failure.")
                raise NetworkTearException(f"Saga Rollback TCP Blackout on Leg {i}")
            elif isinstance(res, Exception):
                logger.critical(f"💀 [FATAL] Rollback failed: {res}")
            elif res and res.get("status") == "FILLED":
                logger.success(f"✅ [SAGA] Rollback step {i} success.")

    async def _execute_with_timeout(self, adapter, params: dict):
        return await asyncio.wait_for(
            adapter.execute_order(**params, order_link_id=f"rb_{int(time.time())}"),
            timeout=self.rollback_timeout
        )


