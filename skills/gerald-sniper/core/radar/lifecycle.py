import json
import time
import asyncio
from uuid import uuid4
from typing import Optional, Dict, Any, List, Tuple
from loguru import logger
from pydantic import BaseModel, Field
from core.events.nerve_center import bus
from core.events.events import SignalEvent
from core.realtime.market_state import market_state
from utils.math_utils import generate_sortable_id

class RadarSignalState(BaseModel):
    """
    [GEKTOR v10.2] Stateful Signal Object.
    Stored in Redis to keep Telegram Buttons lean.
    """
    id: str = Field(default_factory=generate_sortable_id)
    symbol: str
    direction: str
    price: float
    dynamic_ttl_sec: int
    chat_id: Optional[int] = None
    message_id: Optional[int] = None
    created_at: float = Field(default_factory=time.time)
    
    @property
    def expires_at(self) -> float:
        return self.created_at + self.dynamic_ttl_sec

class SignalLifecycleManager:
    """
    [GEKTOR v10.2] Institutional Signal Controller.
    Manages Redis-backed state, Proactive UI Invalidation (Reaper), and Atomic execution.
    """
    def __init__(self, zset_key: str = "radar:signals:expiring"):
        self.zset_key = zset_key
        self._running = False
        self._reaper_task: Optional[asyncio.Task] = None
        self._reaper_semaphore = asyncio.Semaphore(1) # [GEKTOR v10.3] TG 429 Guard

    async def register_signal(self, event: SignalEvent, dynamic_ttl: int) -> RadarSignalState:
        """
        [GEKTOR v10.3] Calculates TTL with Engineering Bounds, stores state, and queues for UI Reaper.
        """
        # [GEKTOR v10.3] Engineering Bound: clamp(15s, calculated_ttl, 120s)
        safe_ttl = max(15, min(dynamic_ttl, 120))
        
        state = RadarSignalState(
            symbol=event.symbol,
            direction=event.direction,
            price=event.price,
            dynamic_ttl_sec=safe_ttl,
            created_at=event.created_at
        )
        
        # Atomically store state and add to expiration queue
        async with bus.redis.pipeline(transaction=True) as pipe:
            # Key format: signal:state:{id}
            pipe.set(f"signal:state:{state.id}", state.model_dump_json(), ex=safe_ttl + 3600)
            # ZSET Key: signals:expiring → score = expires_at
            pipe.zadd(self.zset_key, {state.id: state.expires_at})
            await pipe.execute()
            
        return state

    async def link_message(self, signal_id: str, chat_id: int, message_id: int):
        """Links a Telegram message to a signal for proactive Reaper invalidation."""
        # Key format: signal:ui:{id}
        mapping = {"chat_id": chat_id, "msg_id": message_id}
        await bus.redis.set(f"signal:ui:{signal_id}", json.dumps(mapping), ex=86400)

    async def get_audited_entry_price(self, state: RadarSignalState, click_time: float, 
                                     exec_size_usd: float = 10000.0) -> float:
        """
        [MICROSTRUCTURAL AUDITOR v10.3]
        Simulates Market Impact by 'proeating' the L2 Order Book (Sweep VWAP).
        Detects 'Reality Stratification' (L2/L3 desync) during high volatility.
        """
        sym_state = market_state.get_state(state.symbol)
        if not sym_state:
            logger.warning(f"⚠️ [Auditor] No real-time state for {state.symbol}. Using candle historical data (cheating).")
            return await self._get_legacy_candle_audit(state, click_time)

        # 1. REALITY SYNC GUARD (Kite-Dump Defense)
        # Verify L2 Snapshot isn't stale compared to recent Trades/Reality
        if abs(sym_state.last_l2_ts - sym_state.last_trade_ts) > 0.3:
            logger.critical(f"🛑 [Reality Check] {state.symbol}: Order Book Desync detected (> 300ms)! Exchange 'lying'.")
            # Fail-closed: Apply maximum penalty or raise (here we apply heavy slippage penalty)
            # For HFT Audit Reliability: we force legacy candle audit if L2 is untrustworthy
            return await self._get_legacy_candle_audit(state, click_time, penalty_multiplier=1.5)

        # 2. SWEEP VWAP CALCULATION
        try:
            audited_price = self._calculate_sweep_vwap(
                is_long=(state.direction == "LONG"),
                ob=sym_state.orderbook,
                exec_size_usd=exec_size_usd
            )
            
            # 3. Log Performance (Audit Only)
            slippage = abs(audited_price - state.price) / state.price * 100
            if slippage > 0.05:
                logger.info(f"⚖️ [SweepAuditor] Market Impact accounted: {slippage:.2f}% | VWAP: {audited_price}")
            
            return audited_price
        except ValueError as e:
            logger.error(f"❌ [Auditor] {e}. Falling back to Pessimistic Candle Audit.")
            return await self._get_legacy_candle_audit(state, click_time)

    def _calculate_sweep_vwap(self, is_long: bool, ob: Any, exec_size_usd: float) -> float:
        """Simulates proeating the order book until target size is filled."""
        side = "a" if is_long else "b" # Long buys Asks, Short sells Bids
        heap = sorted(ob._heaps[side])
        pmap = ob._price_map[side]
        
        remaining_usd = exec_size_usd
        total_cost = 0.0
        executed_qty = 0.0

        for p_key in heap:
            # Correct Bids (negative in heap)
            price = abs(p_key)
            vol = pmap[price]
            
            level_usd = price * vol
            take_usd = min(remaining_usd, level_usd)
            
            total_cost += take_usd
            executed_qty += (take_usd / price)
            remaining_usd -= take_usd
            
            if remaining_usd <= 0:
                break
        
        if remaining_usd > 0:
            raise ValueError(f"INSUFFICIENT LIQUIDITY: Can only fill ${exec_size_usd - remaining_usd:.0f}")
            
        return total_cost / executed_qty

    async def _get_legacy_candle_audit(self, state: RadarSignalState, click_time: float, 
                                     penalty_multiplier: float = 1.0) -> float:
        """Fallback to 1-second price buckets if L2 is unavailable/stale."""
        points = market_state.get_price_history(state.symbol, start_ts_ms=int(state.created_at * 1000))
        if not points: return state.price
        
        prices = [p['price'] for p in points if p['ts'] <= click_time * 1000]
        if not prices: return state.price
        
        base_audited = max(prices) if state.direction == "LONG" else min(prices)
        
        if penalty_multiplier > 1.0:
            # Apply extra penalty for data unreliability
            offset = base_audited * (penalty_multiplier - 1.0) / 100
            return base_audited + (offset if state.direction == "LONG" else -offset)
            
        return base_audited

    async def get_signal_state(self, signal_id: str) -> Optional[RadarSignalState]:
        """Atomic retrieval for Button Callbacks."""
        data = await bus.redis.get(f"signal:state:{signal_id}")
        if not data:
            return None
        return RadarSignalState.model_validate_json(data)

    async def start_reaper(self, bot_instance):
        """
        [THE REAPER]
        Background process that sweeps Redis ZSET for expired signals.
        Edits Telegram messages in real-time to remove 'Phantom Buttons'.
        """
        if self._running: return
        self._running = True
        self._reaper_task = asyncio.create_task(self._reaper_loop(bot_instance))
        logger.info("💀 [Lifecycle] UI Reaper ACTIVE (GEKTOR v10.2)")

    async def _reaper_loop(self, bot):
        while self._running:
            try:
                now = time.time()
                # Get all signal_ids that should have expired by now
                expired_ids = await bus.redis.zrangebyscore(self.zset_key, 0, now)
                
                if not expired_ids:
                    await asyncio.sleep(1.0)
                    continue

                for sid_bytes in expired_ids:
                    sid = sid_bytes.decode() if isinstance(sid_bytes, bytes) else sid_bytes
                    
                    # [GEKTOR v10.3] Rate Limiting — Use Semaphore + Burst Protection
                    async with self._reaper_semaphore:
                        # 1. Fetch UI linkage
                        ui_data = await bus.redis.get(f"signal:ui:{sid}")
                        if ui_data:
                            ui = json.loads(ui_data)
                            chat_id = ui['chat_id']
                            msg_id = ui['msg_id']
                            
                            # 2. Proactive UI Invalidation
                            try:
                                await bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=msg_id,
                                    text=f"❌ [EXPIRED] СУЩНОСТЬ УНИЧТОЖЕНА\n(Alpha Decay: время жизни сигнала истекло)",
                                    reply_markup=None,
                                    parse_mode="HTML"
                                )
                                # Institutional pacing: 1.0s delay between edits (Rule 3.1)
                                await asyncio.sleep(1.0)
                            except Exception:
                                pass
                    
                    # 3. Cleanup Redis
                    async with bus.redis.pipeline() as pipe:
                        pipe.zrem(self.zset_key, sid)
                        pipe.delete(f"signal:ui:{sid}")
                        await pipe.execute()
                
            except Exception as e:
                logger.error(f"❌ [Reaper] Sweep failure: {e}")
                await asyncio.sleep(5.0)
            
            await asyncio.sleep(0.5)

# Global Instance
lifecycle_manager = SignalLifecycleManager()
