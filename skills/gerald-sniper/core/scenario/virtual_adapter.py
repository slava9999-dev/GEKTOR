import time
import asyncio
from loguru import logger
from typing import Dict, Any, Optional, List
from .oms_adapter import IExchangeAdapter, quantize_value

class VirtualExchangeAdapter(IExchangeAdapter):
    """
    Institutional Paper-Trading Adapter (Task 3.5).
    - Real-time BBA (Best Bid/Ask) matching from CandleCache.
    - Standard Taker Fee simulation (0.055%).
    - Conservative Entry Slippage penalty (5bps).
    - Persistent state in paper_positions table.
    """
    TAKER_FEE = 0.00055
    ENTRY_SLIPPAGE_BPS = 5
    GHOST_SLIPPAGE_BPS = 15 # Extra penalty for public WS "hologram" data (Audit 16.2)
    
    def __init__(self, db_manager, candle_cache, paper_tracker, instruments_provider, is_ghost: bool = False, exchange: str = "bybit"):
        self.db = db_manager
        self.candle_cache = candle_cache
        self.tracker = paper_tracker
        self.instruments = instruments_provider
        self.is_ghost = is_ghost
        self.exchange = exchange
        
        if is_ghost:
            logger.warning(f"👻 [Virtual] Configured in GHOST mode. Entry penalty increased to {self.GHOST_SLIPPAGE_BPS} bps.")
            self.slippage_bps = self.GHOST_SLIPPAGE_BPS
        else:
            self.slippage_bps = self.ENTRY_SLIPPAGE_BPS
            
        # Task 15.8: Telemetry Compatibility
        self.profiler = None


    async def execute_order(self, symbol: str, side: str, qty: float, price: float, 
                            order_link_id: str, **kwargs) -> Optional[Dict]:
        """
        Simulates market impact by hitting the opposite side of the BBA.
        """
        # 1. Get real BBA
        bba = self.candle_cache.get_bba(symbol)
        side_upper = side.upper()
        
        # 2. Pessimistic Entry (Rule: Long buys at Ask+, Short sells at Bid-)
        # Audit 16.2: GHOST_MODE should not just use static slippage.
        # It must simulate order book penetration to avoid 'Equity Illusion'.
        from core.realtime.market_state import market_state
        from core.shield.slippage import DynamicSlippageEstimator
        
        state = market_state.get_state(symbol, exchange=self.exchange)
        is_long = side.upper() in ("BUY", "LONG", "OPEN_LONG")
        
        fill_price = 0.0
        
        if state and state.orderbook and state.orderbook.is_valid:
            # 1. Institutional VWAP Fill from L2 Orderbook (Task 3.5.1)
            side_ob = "a" if is_long else "b"
            res = state.orderbook.calculate_vwap(side_ob, qty)
            
            if res["status"] == "OK":
                fill_price = res["vwap"]
            else:
                # OB Depth exhausted (Audit 16.2): Dynamic Penalty logic
                last_p = res.get("worst_price") or (state.bba_ask if is_long else state.bba_bid)
                
                # Formula: max(15 bps, spread * 2)
                spread_bps = 0.0
                if state.bba_ask > 0 and state.bba_bid > 0:
                    spread_bps = abs(state.bba_ask - state.bba_bid) / state.bba_bid * 10000
                
                penalty_bps = max(15.0, spread_bps * 2.5) # 2.5x spread for aggressive penalty
                penalty = (1 + penalty_bps/10000) if is_long else (1 - penalty_bps/10000)
                
                fill_price = last_p * penalty
                logger.warning(f"⚠️ [VIRTUAL] {symbol} Depth Breach! Spread: {spread_bps:.1f} bps. Using penalty: {penalty_bps:.1f} bps.")
                
            # Additional GHOST penalty (v4.1): Adding 3 bps extra delay tax
            if self.is_ghost:
                fill_price *= (1.0003 if is_long else 0.9997)
                
            slippage_bps = abs((fill_price - price) / price) * 10000
            logger.debug(f"🎲 [VIRTUAL] {symbol} {side} L2 VWAP Fill: {fill_price:.6f} (Slippage: {slippage_bps:.1f} bps)")
        else:
            # Fallback if no L2 data is available (unlikely for active symbols)
            # Use BBA with static slippage
            if is_long:
                base_price = bba['ask'] if bba['ask'] > 0 else price
                fill_price = base_price * (1 + self.slippage_bps / 10000)
            else:
                base_price = bba['bid'] if bba['bid'] > 0 else price
                fill_price = base_price * (1 - self.slippage_bps / 10000)
            logger.warning(f"⚠️ [GHOST] {symbol} No L2 Depth — using static {self.slippage_bps} bps.")

        order_id = f"virt_{int(time.time() * 1000)}"


        # 3. Apply Fees & Quantization (Rule 3.3)
        # All virtual orders MUST be quantized to maintain institutional parity.
        # If quantization fails (symbol not in cache), the order is REJECTED.
        is_long = side_upper in ("BUY", "LONG", "OPEN_LONG")
        is_maker = kwargs.get('params', {}).get('order_type') == 'Maker'
        
        try:
            q_price, q_qty = self.instruments.quantize_order(symbol, fill_price, qty, is_long, is_maker)
            fill_price, qty = q_price, q_qty
        except Exception as e:
            logger.error(f"❌ [Virtual] STRICT REJECTION: Quantization failed for {symbol}: {e}")
            return {"status": "REJECTED", "error": f"QUANTIZATION_FAILURE:{str(e)}"}

        notional = fill_price * qty
        fee_usd = notional * self.TAKER_FEE
        
        # 4. Save to DB and Internal Tracker
        params = kwargs.get('params', {})
        alert_id = params.get('alert_id')
        
        # We save the ALERT first if it hasn't been saved? 
        # Usually orchestrator saves alert and then calls execute_order.
        
        pos_id = await self.db.insert_paper_position(
            alert_id=alert_id,
            symbol=symbol,
            side=side_upper,
            entry_price=fill_price,
            entry_qty=qty,
            sl_price=params.get('stop_loss', 0.0),
            tp_price=params.get('take_profit', 0.0),
            fee=fee_usd
        )
        
        if pos_id:
            # Sync with the memory tracker for real-time monitoring
            self.tracker.add_position(
                pos_id=pos_id,
                symbol=symbol,
                direction=side_upper,
                entry_price=fill_price,
                qty=qty,
                stop_price=params.get('stop_loss', 0.0),
                target_price=params.get('take_profit', 0.0),
                alert_id=alert_id
            )
            
            logger.info(
                f"🎲 [VIRTUAL] {symbol} {side_upper} Opened (ID: {pos_id}). "
                f"Fill: {fill_price:.5f} (Slippage applied) | Fee: ${fee_usd:.4f}"
            )
            
            return {
                "status": "FILLED",
                "order_id": str(pos_id),
                "filled_qty": qty,
                "avg_price": fill_price
            }
            
        return {"status": "REJECTED", "error": "DB_PERSISTENCE_ERROR"}

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        return True # Virtual orders fill instantly

    async def amend_order(self, symbol: str, order_id: str, **kwargs) -> bool:
        """[Virtual] Always succeeds as virtual orders are pre-filled."""
        logger.debug(f"🎲 [Virtual] Order {order_id} amended successfully.")
        return True

    async def cancel_all_orders(self, symbol: str) -> bool:
        """[Virtual] Instantly clears all (none) pending orders."""
        logger.debug(f"🎲 [Virtual] All orders for {symbol} canceled (Virtual-mode).")
        return True

    async def get_position(self, symbol: str) -> Optional[Dict]:
        return self.tracker.positions.get(symbol)

    async def get_all_positions(self) -> List[Dict]:
        return list(self.tracker.positions.values())

    async def set_dms_window(self, window: int):
        """Stub for Dead Man's Switch in Paper Mode."""
        pass

    async def hot_swap_credentials(self, api_key: str, api_secret: str):
        """Stub for credentials update in Paper Mode."""
        logger.info(f"🎲 [VirtualAdapter] 'Hot Swap' simulated for credentials update.")
        return True
