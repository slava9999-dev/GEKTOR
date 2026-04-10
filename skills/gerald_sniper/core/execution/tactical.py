# core/execution/tactical.py
import asyncio
import logging
import time
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum, auto

logger = logging.getLogger("GEKTOR.APEX.Execution")

class ExecutionStatus(Enum):
    PENDING = auto()
    PARTIAL = auto()
    COMPLETED = auto()
    EXPIRED = auto()

@dataclass(slots=True)
class ExecutionIntent:
    cl_ord_id: str
    symbol: str
    side: str # 'Buy' or 'Sell'
    target_qty: float
    max_slippage_offset: float = 0.0005  # 0.05%
    ttl_seconds: float = 10.0
    
    _filled_qty: float = field(default=0.0, init=False)
    
    @property
    def remaining_qty(self) -> float:
        return max(0.0, self.target_qty - self._filled_qty)

class TacticalExecutionEngine:
    """
    [GEKTOR v14.9.0] Hardened Execution Orchestrator.
    - Zero REST-Spam via Micro-Structural Sync.
    - Phantom Liquidity Shield (Toxic Level Marking).
    - Adaptive Replenishment Waiting.
    """
    def __init__(self, rest_gateway, l2_cache_manager, event_bus):
        """
        rest_gateway: OrderRoutingGateway
        l2_cache_manager: LiveMarketState or something providing SymbolStates
        event_bus: NerveCenter
        """
        self.gateway = rest_gateway
        self.l2_cache_manager = l2_cache_manager
        self.event_bus = event_bus

    async def execute_combat_strike(self, intent: ExecutionIntent) -> ExecutionStatus:
        """[DEPRECATED] Tactical execution disabled by Manifesto."""
        logger.critical(f"🛑 [MANIFESTO VIOLATION] Tactical strike attempted for {intent.symbol}. REJECTED.")
        raise RuntimeError("v2.0 MANIFESTO: HFT Scalping and Automated Execution are PROHIBITED.")
        start_time = asyncio.get_running_loop().time()
        deadline = start_time + intent.ttl_seconds
        
        # 获取 L2 引擎
        state = self.l2_cache_manager.get_state(intent.symbol)
        if not state:
            logger.error(f"❌ [TACTICAL] No state for {intent.symbol}. Aborting strike.")
            return ExecutionStatus.EXPIRED
        
        # Assume SymbolLiveState has direct orderbook/l2_engine logic or we use l2_engine
        # In GEKTOR, L2LiquidityEngine is often inside the state or a dedicated shard.
        # For this implementation, we assume `state.orderbook` or equivalent is our L2Radar.
        # As per my previous edit, L2LiquidityEngine is what I modified.
        # Let's assume some bridge provides access to it. 
        
        # [GEKTOR v14.9.1] Microstructural Logic: Toxic Flow Guard (OFI)
        # Standard: Abort if the current flow is critically opposed to our direction
        if intent.side.lower() == 'buy' and state.current_ofi < -0.8:
            logger.warning(f"🛑 [TOXIC ABORT] {intent.symbol} Long aborted. OFI: {state.current_ofi:.2f}")
            return ExecutionStatus.EXPIRED
        elif intent.side.lower() == 'sell' and state.current_ofi > 0.8:
            logger.warning(f"🛑 [TOXIC ABORT] {intent.symbol} Short aborted. OFI: {state.current_ofi:.2f}")
            return ExecutionStatus.EXPIRED
        
        # [GEKTOR v15.0] Combat Readiness Gate (Post-Mortem Sync)
        # We wait until the RiskAllocator has reconciled the ground truth with the exchange.
        try:
            # We assume gateway has a reference to risk_allocator (RiskAllocator)
            # In GEKTOR v14.8, it's injected during OrderRoutingGateway initialization.
            await self.gateway.risk_allocator.wait_until_ready(timeout=5.0)
        except Exception as e:
            logger.error(f"[{intent.cl_ord_id}] Cannot strike: Portfolio not ready: {e}")
            return ExecutionStatus.EXPIRED

        while intent.remaining_qty > 0:
            now = asyncio.get_running_loop().time()
            time_left = deadline - now
            if time_left <= 0:
                logger.warning(f"[{intent.cl_ord_id}] Tactical TTL expired. Filled: {intent._filled_qty}/{intent.target_qty}")
                return ExecutionStatus.EXPIRED if intent._filled_qty == 0 else ExecutionStatus.PARTIAL

            # 1. Проверка ликвидности (Не бьем в пустоту / Срезаем миражи)
            # get_liquidity_at_offset returns (total_qty, strike_price)
            # It already filters out 'toxic_zones'.
            available_qty, strike_price = state.l2_radar.get_liquidity_at_offset(
                side=intent.side, 
                max_slippage_offset=intent.max_slippage_offset
            )

            # Heuristic: If liquidity is less than 5% of our target OR 0, wait for replenishment
            if available_qty < (intent.target_qty * 0.05) or strike_price == 0:
                logger.debug(f"[{intent.symbol}] Liquidity drought / Toxic cluster. Awaiting replenishment...")
                try:
                    # Ждем триггера от вебсокета через NerveCenter barrier
                    await asyncio.wait_for(
                        self.event_bus.wait_for_l2_update(intent.symbol),
                        timeout=min(2.0, time_left) # Don't wait forever, re-check often
                    )
                    continue  # Ре-эвалюация стакана
                except asyncio.TimeoutError:
                    if time_left <= 2.0:
                         return ExecutionStatus.EXPIRED
                    continue

            # 2. Формирование микро-страйка (Срезаем только доступное, не более остатка)
            slice_qty = min(intent.remaining_qty, available_qty)
            
            # 3. Боевой бросок IOC через OrderRoutingGateway (which handles locks)
            try:
                fill_qty = await self.gateway.execute_limit_ioc(
                    symbol=intent.symbol,
                    side=intent.side,
                    qty=slice_qty,
                    price=strike_price,
                    priority=0
                )
                
                intent._filled_qty += fill_qty
                
                # [PHANTOM TRAP DETECTION]
                if fill_qty == 0 and slice_qty > 0:
                    logger.critical(f"💀 [MIRAGE] {intent.symbol} @ {strike_price} returned ZERO. Level marked toxic.")
                    state.l2_radar.mark_level_toxic(strike_price) # Маркировка без таймеров
                    
                    # [HFT Quant]: Никаких слипов. Просто ждем следующего обновления L2 для ре-эвалюации.
                    await self.event_bus.wait_for_l2_update(intent.symbol, timeout_ms=50.0) 
                    continue

                if 0 < fill_qty < slice_qty:
                    # UNDERFILL: Ждем 5мс (а не 50) для обновления очереди
                    await asyncio.sleep(0.005) 

            except Exception as e:
                logger.error(f"[{intent.cl_ord_id}] Execution gateway failed: {str(e)}")
                # Fail fast on connection/protocol errors
                return ExecutionStatus.EXPIRED

        return ExecutionStatus.COMPLETED
