import asyncio
import time
from enum import Enum
from typing import Any, Optional, Dict
from dataclasses import dataclass
from collections import deque
from loguru import logger

class ExchangeCircuitBreaker:
    """
    [Audit 6.9] Атомарный предохранитель для изоляции падающих шлюзов.
    Паттерн 'Leaky Bucket' для плотности ошибок.
    """
    def __init__(self, exchange_name: str, max_failures: int = 3, time_window_sec: int = 60):
        self.exchange = exchange_name
        self.max_failures = max_failures
        self.time_window = time_window_sec
        self._failures_ts = deque()
        self.is_tripped = False

    def record_failure(self, reason: str):
        """Регистрирует фатальный сбой (Timeout / Order Loss)."""
        now = time.time()
        self._failures_ts.append(now)
        logger.warning(f"🚨 [{self.exchange}] Failure: {reason}. (Pool: {len(self._failures_ts)})")
        self._evaluate_state()

    def _evaluate_state(self):
        now = time.time()
        while self._failures_ts and (now - self._failures_ts[0] > self.time_window):
            self._failures_ts.popleft()

        if len(self._failures_ts) >= self.max_failures and not self.is_tripped:
            self.trip_breaker()

    def trip_breaker(self):
        self.is_tripped = True
        logger.critical(f"🛑 [CIRCUIT BREAKER TRIPPED] {self.exchange} isolated! "
                        f"{self.max_failures} failures in {self.time_window}s.")
        # [NERVE REPAIR] Здесь должен быть сигнал в EventBus (Panic Mode)
        
    def can_route(self) -> bool:
        return not self.is_tripped

    def manual_reset(self):
        self._failures_ts.clear()
        self.is_tripped = False
        logger.info(f"🟢 [CIRCUIT BREAKER RESET] {self.exchange} gate RESTORED.")

class OrderState(Enum):
    INIT = "INIT"
    SUBMITTED = "SUBMITTED"
    ACK_PENDING = "ACK_PENDING"
    RECONCILING = "RECONCILING"
    RESOLVED = "RESOLVED"
    FAILED_FATAL = "FAILED_FATAL"

@dataclass
class OrderContext:
    signal_id: str
    target_qty: float
    symbol: str
    side: str # BUY/SELL
    attempt: int = 1
    filled_qty: float = 0.0
    state: OrderState = OrderState.INIT

    @property
    def cl_ord_id(self) -> str:
        """Детерминированный идемпотентный ключ"""
        # HFT-Standard: f"hdg_{signal_id}_v{attempt}"
        return f"hdg_{self.signal_id}_v{self.attempt}"

    @property
    def remaining_qty(self) -> float:
        return max(0.0, self.target_qty - self.filled_qty)

class ReconciliationEngine:
    """
    [Audit 6.9] Idempotency Recovery FSM.
    Handles 'Ghost Drift' by reconciling local state with exchange ground-truth.
    Integrates with ExchangeCircuitBreaker to isolate dead legs.
    """
    def __init__(self, adapter: Any, breaker: ExchangeCircuitBreaker, execution_timeout_ms: int = 500):
        self.adapter = adapter
        self.breaker = breaker
        self.timeout = execution_timeout_ms / 1000.0

    async def execute_with_reconciliation(self, ctx: OrderContext) -> bool:
        """
        Атомарный цикл исполнения с детерминированной идемпотентностью и проверкой предохранителя.
        """
        if not self.breaker.can_route():
            logger.error(f"🛡️ [Hedge] Execution BLOCKED: {self.breaker.exchange} Circuit Breaker is active.")
            return False

        while ctx.remaining_qty > 0.000001: 
            # Fetch Dynamic Offset O_dynamic for EVERY attempt
            from core.realtime.market_state import market_state
            state = market_state.get_state(ctx.symbol)
            sigma = state.get_spread_std() if state else 0.0
            alpha = 2.0
            dynamic_offset_bps = max(10.0, alpha * sigma)
            max_slip_pct = dynamic_offset_bps / 10000.0
            
            # Fetch current ticker for pricing
            ticker = await self.adapter.get_ticker(ctx.symbol)
            best_bid = float(ticker.get("bidPrice", 0))
            best_ask = float(ticker.get("askPrice", 0))
            
            if ctx.side.upper() == "BUY":
                limit_price = best_ask * (1.0 + max_slip_pct)
            else:
                limit_price = best_bid * (1.0 - max_slip_pct)

            ctx.state = OrderState.SUBMITTED
            current_clordid = ctx.cl_ord_id
            
            logger.info(f"🚀 [Hedge] Executing {ctx.cl_ord_id} | {ctx.symbol} | Qty: {ctx.remaining_qty:.4f}")
            
            try:
                # 1. Fire Order (Wait for ACK with timeout)
                place_task = asyncio.create_task(self.adapter.execute_order(
                    symbol=ctx.symbol,
                    side=ctx.side,
                    qty=ctx.remaining_qty,
                    price=limit_price,
                    order_link_id=current_clordid,
                    params={"order_type": "Limit", "tif": "IOC", "reduceOnly": False}
                ))
                ctx.state = OrderState.ACK_PENDING
                
                resp = await asyncio.wait_for(place_task, timeout=self.timeout)
                
                # 2. Process Sync Response
                if resp.get("status") == "FILLED" or int(resp.get("retCode", -1)) == 0:
                    ctx.filled_qty += float(resp.get("filled_qty", 0))
                    ctx.state = OrderState.RESOLVED
                else:
                    logger.warning(f"⚠️ [Hedge] Order {current_clordid} rejected/expired: {resp.get('error')}")
                    # If it's a rejected/expired IOC, we just loop to next attempt
                    ctx.state = OrderState.RESOLVED 
                
            except asyncio.TimeoutError:
                logger.warning(f"🌐 [Hedge] Timeout on {current_clordid}. Entering RECONCILIATION.")
                await self._reconcile_state(ctx, current_clordid)
            except Exception as e:
                logger.error(f"💀 [Hedge] Fatal execution error: {e}")
                ctx.state = OrderState.FAILED_FATAL
                return False

            if ctx.remaining_qty <= 0.000001:
                break
                
            ctx.attempt += 1
            await asyncio.sleep(0.1) # Rate limit padding

        return True

    async def _reconcile_state(self, ctx: OrderContext, clordid_to_check: str):
        """
        Hard REST probe with RAW Lag (Read-After-Write) Protection.
        """
        ctx.state = OrderState.RECONCILING
        
        # [Audit 6.8] RAW LAG PROTECTION
        # matching server (C++) != order db (REST READ). 
        # We poll for 1.5s before assuming "Not Found" == "Never Reached".
        start_time = time.time()
        timeout = 1.5 
        
        while (time.time() - start_time) < timeout:
            try:
                status_data = await self.adapter.get_order_status(
                    symbol=ctx.symbol, 
                    order_link_id=clordid_to_check
                )
                
                if status_data:
                    # Order Found! Sync state and bailout
                    actual_filled = float(status_data.get('filled_qty', 0))
                    logger.success(f"✅ [Recon] FOUND {clordid_to_check}. Filled: {actual_filled}")
                    ctx.filled_qty += actual_filled
                    ctx.state = OrderState.RESOLVED
                    return
                
                # Not found yet, but still in window? Loop.
                await asyncio.sleep(0.3)
                
            except Exception as e:
                logger.error(f"🚨 [Recon] Error during probe for {clordid_to_check}: {e}")
                await asyncio.sleep(0.5)
        
        # After 1.5s, if it's still not found, we assume it's lost and move to next version
        logger.warning(f"❄️ [Recon] {clordid_to_check} confirmed MISSING (RAW Lag Safe). Moving to v{ctx.attempt+1}.")
        
        # If we have multiple consecutive misses on the same signal, it's a fatal sign
        if ctx.attempt >= 3:
            self.breaker.record_failure(f"Triple-miss on signal {ctx.signal_id}")
            ctx.state = OrderState.FAILED_FATAL
            return
            
        ctx.state = OrderState.RESOLVED

class EmergencyHedgeManager:
    """
    Инженерный стандарт: Изолированный контур спасения.
    Работает только когда основной API недоступен (v6.4).
    
    Architecture:
    - Independent Execution Path: Triggers only when Bybit RTT > 1000ms or API is dead.
    - Error Magnification Guard: Applies a conservative multiplier if WS state is stale.
    - Tier-2 Safety: Secondary to Bybit's server-side SL/ReduceOnly.
    """
    def __init__(self, binance_adapter: Any):
        self.binance = binance_adapter
        self.active_hedges = {}
        # [Audit 6.9] Gateway Protection
        self.breaker = ExchangeCircuitBreaker("Binance_Hedge")
        self.recon = ReconciliationEngine(binance_adapter, self.breaker)

    async def execute_cross_hedge(self, symbol: str, qty: float, side: str, is_blind: bool = False):
        """
        [P0] Emergency Hedge Allocation.
        side: The side of the UNREACHABLE position on Bybit (LONG/SHORT).
        qty: Estimated size from local state.
        is_blind: True if we've lost WebSocket sync (>1s gap).
        """
        # Invert side: To hedge LONG on Bybit, we go SHORT on Binance
        hedge_side = "Sell" if side.upper() == "LONG" else "Buy"
        
        # [Audit 6.8] Refactored to Idempotent Reconciliation FSM
        ctx = OrderContext(
            signal_id=f"crisis_{int(time.time())}",
            target_qty=float(qty) * (0.8 if is_blind else 1.0),
            symbol=symbol,
            side=hedge_side
        )

        success = await self.recon.execute_with_reconciliation(ctx)
        
        if success and ctx.filled_qty > 0:
            logger.success(f"✅ [Hedge] Secondary Shield DEPLOYED for {symbol} on Binance.")
            self.active_hedges[symbol] = {
                "qty": ctx.filled_qty,
                "side": hedge_side,
                "timestamp": time.time(),
                "signal_id": ctx.signal_id
            }
            return True
        
        return False

    async def reconcile_and_liquidate(self, symbol: str, actual_bybit_qty: float):
        """
        [P0] Closure of the Rescue Shield (Symmetry Recovery).
        Called when connectivity to Bybit is restored and actual state is reconciled.
        """
        if symbol not in self.active_hedges:
            return

        hedge = self.active_hedges[symbol]
        logger.info(f"🛡️ [Hedge] Reconciling {symbol}. Bybit Actual Qty: {actual_bybit_qty} | Hedge Qty: {hedge['qty']}")

        # [Audit 6.8] Refactored to Idempotent Reconciliation FSM
        if abs(actual_bybit_qty) < 0.000001: 
            logger.critical(f"☢️ [Hedge] Bybit position for {symbol} is CLOSED. Liquidating Binance Shield!")
            
            exit_side = "Buy" if hedge['side'] == "Sell" else "Sell"
            ctx = OrderContext(
                signal_id=hedge.get('signal_id') or f"close_{int(time.time())}",
                target_qty=hedge['qty'],
                symbol=symbol,
                side=exit_side
            )
            
            success = await self.recon.execute_with_reconciliation(ctx)
            if success:
                if ctx.remaining_qty > 0.000001:
                    logger.warning(f"⚠️ [Hedge] Partial exit for {symbol}. Remaining: {ctx.remaining_qty}")
                    hedge['qty'] = ctx.remaining_qty
                else:
                    logger.success(f"✅ [Hedge] Binance shield collapsed via Reconciled FSM for {symbol}.")
                    del self.active_hedges[symbol]
            else:
                logger.error(f"🔥 [Hedge] Fatal failure to collapse shield for {symbol}!")
        else:
            # Position still exists on Bybit, hedge is performing its duty.
            logger.info(f"🛡️ [Hedge] {symbol} Shield maintained (Bybit leg is alive).")

    async def cleanup(self):
        """[Audit 25.8] Orphaned State Cleanup Logic: Cancel all Binance orders for active hedges."""
        logger.warning("🛑 [Hedge] Initiating cleanup for orphaned stops on Binance...")
        symbols = list(self.active_hedges.keys())
        for symbol in symbols:
            try:
                await self.binance.cancel_all_orders(symbol)
                logger.info(f"🧹 [Hedge] Cleaned up {symbol}")
            except Exception as e:
                logger.error(f"💥 [Hedge] Cleanup failed for {symbol}: {e}")
        self.active_hedges.clear()
