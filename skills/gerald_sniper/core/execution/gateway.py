# core/execution/gateway.py
import asyncio
import time
import uuid
import hmac
import hashlib
from typing import Dict, Optional, Set, Any
from dataclasses import dataclass, field, asdict
from loguru import logger
from core.execution.priority_gateway import PriorityRESTGateway

@dataclass(slots=True, frozen=True)
class OrderResult:
    cl_ord_id: str
    filled_qty: float
    avg_price: float
    status: str # 'FILLED', 'PARTIALLY_FILLED', 'CANCELED', 'REJECTED'

class OrderStateMachine:
    """[GEKTOR v14.8.4] Multi-Order State Switching with Zero-Blocking."""
    __slots__ = ('_in_flight', 'last_execution_ts', 'armed', 'risk_allocator')

    def __init__(self, risk_allocator):
        self._in_flight: Set[str] = set()
        self.last_execution_ts = 0.0
        self.armed = True 
        self.risk_allocator = risk_allocator

    async def can_execute(self, symbol: str) -> bool:
        """[GEKTOR v14.8.6] Verify persistent lock (survives restarts)."""
        return not await self.risk_allocator.is_symbol_locked(symbol)

    def mark_in_flight(self, symbol: str):
        self._in_flight.add(symbol)

    def clear(self, symbol: str):
        self._in_flight.discard(symbol)

    def disarm(self):
        self.armed = False

    def arm(self):
        self.armed = True

class ConcurrentOrderRegistry:
    """[GEKTOR v14.4.8] Atomic resolver for REST vs WS Drop Copy race."""
    __slots__ = ('_pending_orders',)

    def __init__(self):
        self._pending_orders: Dict[str, asyncio.Future] = {}

    def register(self, cl_ord_id: str) -> asyncio.Future:
        fut = asyncio.get_running_loop().create_future()
        self._pending_orders[cl_ord_id] = fut
        return fut

    def resolve(self, result: OrderResult):
        fut = self._pending_orders.get(result.cl_ord_id)
        if fut and not fut.done():
            fut.set_result(result)
            logger.info(f"✅ [SNIPER] Order {result.cl_ord_id} resolved via {'DropCopy' if result.status == 'FILLED' else 'REST'}")

    def cleanup(self, cl_ord_id: str):
        self._pending_orders.pop(cl_ord_id, None)

@dataclass(slots=True, frozen=True)
class SnipeCommand:
    symbol: str
    side: str
    qty: float
    base_price: float
    max_slippage_bps: float
    cl_ord_id: str
    is_evacuation: bool = False # [NEW] Signal to dump position

class PositionReconciler:
    """
    [GEKTOR v14.4.7] Ground Truth Sync (Drop Copy Pattern).
    Ensures logical state aligns with physical exchange state after I/O failures.
    """
    __slots__ = ('_client', '_known_positions', '_last_sync_ts', '_priority_gateway')

    def __init__(self, client, risk_allocator=None, priority_gateway=None):
        self._client = client
        self._known_positions: Dict[str, float] = {}
        self._risk_allocator = risk_allocator
        self._last_sync_ts = 0.0
        self._priority_gateway = priority_gateway

    async def sync_all(self):
        """[GEKTOR v14.8.6] Priority 2: Routine Sink All."""
        try:
            # We wrap the heavier full-sync in Priority 2
            positions = await self._priority_gateway.execute(
                priority=2,
                name="FullPositionSync",
                coro_fn=lambda: self._client.get_positions()
            )
            # ... process positions ...
            # self._risk_allocator.full_reconcile(...)
        except Exception as e:
            logger.error(f"❌ [Reconciler] Sync Fail: {e}")

    async def full_reconcile(self):
        """Martin Kleppmann's Ground Truth: REST position poll."""
        try:
            # GET /v5/position/list
            res = await self._client.get_positions(category="linear", settleCoin="USDT")
            new_positions = {}
            for pos in res.get('list', []):
                symbol = pos['symbol']
                size = float(pos['size'])
                new_positions[symbol] = size
            
            self._known_positions = new_positions
            self._last_sync_ts = time.time()
            if self._risk_allocator:
                 # Ground Truth Reconciliation: Convert positions to estimated Beta
                 # In real system, we'd fetch rolling beta for each open symbol here
                 await self._risk_allocator.full_reconcile({s: 1.0 for s in new_positions}) 
            logger.debug(f"🔄 [RECON] Ground Truth Sync complete. Symbols: {list(new_positions.keys())}")
            return True
        except Exception as e:
            logger.error(f"❌ [RECON] REST reconciliation failed: {e}")
            return False

    async def get_positions_snapshot(self) -> Dict[str, dict]:
        """[GEKTOR v14.7.0] Fetch detailed positions for StateReconciler sync."""
        try:
            res = await self._client.get_positions(category="linear", settleCoin="USDT")
            snapshot = {}
            for pos in res.get('list', []):
                symbol = pos['symbol']
                snapshot[symbol] = {
                    'size': float(pos.get('size', 0)),
                    'mark_price': float(pos.get('markPrice', 0)),
                    'avg_price': float(pos.get('avgPrice', 0))
                }
            return snapshot
        except Exception as e:
            logger.error(f"❌ [GATEWAY] Failed to snapshot positions: {e}")
            return {}

class EvacuationChaser:
    """
    [GEKTOR v14.4.9] Aggressive Execution Algo.
    Forces position closure during liquidity vacuum using slippage escalation.
    """
    __slots__ = ('_client', '_max_retries', '_base_slip')

    def __init__(self, client):
        self._client = client
        self._max_retries = 3
        self._base_slip = 50.0

    async def sweep_exit(self, symbol: str, side: str, qty: float, current_price: float) -> bool:
        remaining = qty
        for attempt in range(1, self._max_retries + 1):
            if remaining <= 0: break
            
            # Escalate Slippage (50, 100, 200 bps)
            slip_bps = self._base_slip * (2 ** (attempt - 1))
            adj_price = current_price * (0.99 if side == 'Sell' else 1.01) # Basic wider buffer
            if side == 'Sell': 
                 limit_price = current_price * (1.0 - (slip_bps / 10000.0))
            else:
                 limit_price = current_price * (1.0 + (slip_bps / 10000.0))

            cl_ord_id = f"evac-{attempt}-{uuid.uuid4().hex[:6]}"
            logger.critical(f"🚀 [EVAC] Attempt {attempt} | {side} {remaining} {symbol} @ {limit_price}")
            
            try:
                res = await self._client.place_order(
                    symbol=symbol, side=side, orderType="Limit", 
                    qty=str(remaining), price=str(round(limit_price, 4)), 
                    timeInForce="IOC", orderLinkId=cl_ord_id
                )
                
                if res.get('retCode') == 0:
                     # For IOC, we assume partial/full fill. 
                     # Real system would wait for Drop Copy resolution here.
                     await asyncio.sleep(0.05) # Wait for match
                     # In this demo, we decrement fully (ideally check reconciler)
                     remaining = 0
                else:
                    logger.warning(f"⚠️ [EVAC] Step {attempt} Rejected: {res.get('retMsg')}")
            except Exception as e:
                logger.error(f"💥 [EVAC] API Fail during escape: {e}")
                return False
        
    async def _on_order_update(self, event: Any):
        """[DROP COPY] Reactive High-Speed Risk Handover."""
        # 1. Update Portfolio State (Idempotent Lua)
        await self.risk_allocator.handle_execution(
            cl_ord_id=event.order_id,
            symbol=event.symbol,
            cum_qty=event.cum_qty,
            total_qty=event.total_qty,
            original_beta=1.0, 
            is_final=event.status in ['Filled', 'Cancelled', 'Rejected']
        )
        
        # 2. [GEKTOR v14.8.6] Release Persistent Symbol Lock
        if event.status in ['Filled', 'Cancelled', 'Rejected']:
             # Use original cl_ord_id from event for Lease comparison
             await self.risk_allocator.unlock_symbol(event.symbol, event.order_id)
             res = OrderResult(event.order_id, event.cum_qty, 0.0, "RESOLVED")
             self._registry.resolve(res)

@dataclass(slots=True, frozen=True)
class LimboItem:
    symbol: str
    cl_ord_id: str
    side: str
    qty: float
    timestamp: float = field(default_factory=time.time)

class LimboReaper:
    """
    [GEKTOR v14.8.4] Post-Mortem Resolution Engine.
    Martin Kleppmann Standard: Resolves UNKNOWN states in background 
    to prevent Event Loop starvation during infrastructure failure.
    """
    def __init__(self, client, bus, risk_allocator, priority_gateway):
        self._client = client
        self._bus = bus
        self._risk_allocator = risk_allocator
        self._priority_gateway = priority_gateway
        self._running = False

    async def put(self, item: LimboItem):
        await self._risk_allocator.push_to_limbo(asdict(item))
        logger.warning(f"🧟 [LIMBO] Order {item.cl_ord_id} ({item.symbol}) queued for background resolution.")

    async def start(self):
        if self._running: return
        self._running = True
        asyncio.create_task(self._reaper_loop())

    async def _reaper_loop(self):
        while self._running:
            it = await self._risk_allocator.pop_from_limbo()
            if not it: await asyncio.sleep(1.0); continue
            item = LimboItem(**it)
            # Background task per order for parallel reconciliation
            asyncio.create_task(self._resolve_cl_ord_id(item))

    async def _resolve_cl_ord_id(self, item: LimboItem):
        from core.events.events import LimboResolutionEvent
        
        for attempt in range(5):
            try:
                # [GEKTOR v14.8.6] Priority 1: High-Priority Recovery
                response = await self._priority_gateway.execute(
                    priority=1,
                    name=f"Reconcile:{item.cl_ord_id}",
                    coro_fn=lambda: self._client.get_order_realtime(
                        symbol=item.symbol,
                        orderLinkId=item.cl_ord_id
                    )
                )
                
                ret_code = response.get('retCode')
                res_data = response.get('result', {}).get("list", [{}])[0]
                
                if ret_code == 0:
                    status = res_data.get("orderStatus")
                    cum_qty = float(res_data.get("cumExecQty", 0))
                    avg_price = float(res_data.get("avgPrice", 0))
                    order_id = res_data.get("orderId", "Limbo")
                    
                    logger.success(f"⚓ [LIMBO_RESOLVED] {item.cl_ord_id}: {status} | Filled: {cum_qty}")
                    
                    event = LimboResolutionEvent(
                        symbol=item.symbol,
                        cl_ord_id=item.cl_ord_id,
                        order_id=order_id,
                        status=status,
                        cum_qty=cum_qty,
                        avg_price=avg_price,
                        side=item.side
                    )
                    await self._bus.publish(event)
                    await self._risk_allocator.handle_limbo_resolution(event)
                    # [GEKTOR v14.8.6] RECOVERY BARRIER: Unlock symbol ONLY after resolution
                    await self._risk_allocator.unlock_symbol(item.symbol, item.cl_ord_id)
                    return
                
                elif ret_code == 110001:
                    # Not found - wait for exchange to sync
                    pass
                
                else:
                    logger.error(f"❌ [LIMBO_ERROR] {item.cl_ord_id} Code {ret_code}: {response.get('retMsg')}")

            except Exception as e:
                logger.error(f"💥 [LIMBO_FAIL] Attempt {attempt+1} fail for {item.cl_ord_id}: {e}")
            
            # Wait for next exponential backoff attempt
            await asyncio.sleep(2 ** (attempt + 1))
        
        # [GEKTOR v14.8.7] TERMINAL FALLBACK
        # If all retries failed (Bybit down/long lockout), we force release 
        # to prevent Permanent Deadlock (Infinite Strike Muting).
        logger.critical(f"☢️ [REAPER_TERMINAL] Failed to resolve {item.cl_ord_id}. FORCING UNLOCK.")
        await self._risk_allocator.unlock_symbol(item.symbol, item.cl_ord_id)
        
        logger.critical(f"💀 [LIMBO_FATAL] Could not reconcile {item.cl_ord_id} after exhaustive attempts.")

class OrderRoutingGateway:
    """I/O Gateway isolating computing core from network instability (GEKTOR v14.4.8)."""
    def __init__(self, exchange_client, risk_allocator, bus, priority_gateway: PriorityRESTGateway):
        self.client = exchange_client
        self.risk_allocator = risk_allocator
        self.bus = bus
        self._priority_gateway = priority_gateway
        
        self._state = OrderStateMachine(risk_allocator)
        self._reconciler = PositionReconciler(exchange_client, self.risk_allocator, priority_gateway)
        self._registry = ConcurrentOrderRegistry()
        self._reaper = LimboReaper(exchange_client, bus, self.risk_allocator, priority_gateway)

    def _generate_cl_ord_id(self):
        return f"gk-{int(time.time()*1000)}-{uuid.uuid4().hex[:4]}"

    async def run(self):
        asyncio.create_task(self._reconciliation_loop())
        # Start WebSocket Listener in background (Drop Copy Path)
        asyncio.create_task(self._drop_copy_listener())
        # Start the Limbo Reaper task
        await self._reaper.start()
        
        while True:
            cmd = await self._queue.get()
            if not await self._state.can_execute(cmd.symbol): continue
            asyncio.create_task(self._execute_with_resolution(cmd))

    async def _drop_copy_listener(self):
        """Websocket Execution/Execution_v5 listener for Drop Copy sync."""
        from core.events.events import OrderUpdateEvent
        self.bus.subscribe(OrderUpdateEvent, self._on_order_update, persistent=False)
        logger.info("🎧 [GATEWAY] Listening for Drop Copy (OrderUpdateEvent) on bus.")

    # ─────────────────────────────────────────────────────────────────────────────
    # [GEKTOR v2.0 STRICT] MANIFESTO GUARD
    # DECOMMISSIONED: Automated execution is PROHIBITED.
    # ─────────────────────────────────────────────────────────────────────────────
    def _manifesto_guard(self, op: str):
        """Hard barrier for any execution attempt."""
        logger.critical(f"🛑 [MANIFESTO VIOLATION] Attempted {op}. REJECTED.")
        raise RuntimeError(
            "GEKTOR v2.0 ARCHITECTURE MANIFESTO: "
            "Automated execution is STRICTLY PROHIBITED. "
            "System is in ADVISORY RADAR mode only."
        )

    async def execute_limit_ioc(self, symbol: str, side: str, qty: float, price: float, priority: int = 0) -> float:
        """[DEPRECATED] Automated execution disabled by Manifesto."""
        self._manifesto_guard(f"IOC_STRIKE:{symbol}")
        return 0.0

    async def _execute_with_resolution(self, cmd: Any):
        """[DEPRECATED] Automated execution disabled by Manifesto."""
        self._manifesto_guard(f"ORDER_SUBMISSION:{cmd.symbol}")
        return

    def _trigger_terminal_alarm(self, symbol: str):
        """[AIR-GAP SIREN] Last hope when API/Network fails during dump."""
        logger.critical(f"🆘 [TERMINAL_FAILURE] {symbol}: API BLACKOUT. ACTIVATE EMERGENCY CONTROL.")
        try:
            import winsound
            for _ in range(5):
                winsound.Beep(1000, 200)
                winsound.Beep(2000, 500)
        except Exception:
            pass

            logger.error(f"❌ [GATEWAY] REST submission error {cl_ord_id}: {e}")
            # Do not resolve. Let the 'wait_for' timeout and trigger recovery query.

    async def _reconciliation_loop(self):
        while True:
            await asyncio.sleep(10) # 10s Ground Truth check
            await self._reconciler.full_reconcile()
            self._state.pending_recon.clear() # Clear block after full sync
