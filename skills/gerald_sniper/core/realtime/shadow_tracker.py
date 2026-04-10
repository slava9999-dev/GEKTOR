# skills/gerald-sniper/core/realtime/shadow_tracker.py
import asyncio
import time
import json
from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from loguru import logger
from sqlalchemy import text
import heapq
from ...data.database import DatabaseManager

class BarrierHit(Enum):
    NONE = "NONE"
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    TIME_EXPIRATION = "TIME_EXPIRATION"

@dataclass
class TripleBarrierState:
    __slots__ = ['pos_id', 'symbol', 'entry_price', 'upper_barrier', 'lower_barrier', 'expiration_ts', 'is_active', 'side']
    
    pos_id: int
    symbol: str
    entry_price: float
    upper_barrier: float
    lower_barrier: float
    expiration_ts: float # Unix timestamp
    side: str # 'LONG' | 'SHORT'
    is_active: bool

class ShadowPositionTracker:
    """
    [GEKTOR v21.17] Triple Barrier Shadow Monitoring Engine.
    - O(1) In-Memory Lookup: Fast evaluation on every tick.
    - Crash Resilience: Full state rehydration from PostgreSQL.
    - Slippage Awareness: Records the actual price that triggered the barrier (Accounting for Gaps).
    """

    def __init__(self, db: DatabaseManager):
        self.db = db
        # Internal Cache: symbol -> {pos_id: state}
        # Optimized for tick routing by symbol
        self._active_barriers: Dict[str, Dict[int, TripleBarrierState]] = {}
        self._lock = asyncio.Lock()
        self._running = False
        
        # [GEKTOR v21.23] Causal-Consistent Temporal Reaper
        self._expiration_heap: List[Tuple[float, str, int]] = [] 
        self._global_watermark: float = 0
        self._last_tick_arrival: float = time.monotonic()
        self._safe_drift_ms = 150

    async def initialize(self):
        """Initial recovery of open positions (State Rehydration)."""
        logger.info("📡 [ShadowTracker] Rehydrating Triple Barrier state from DB...")
        async with self.db.SessionLocal() as session:
            try:
                # Assuming table 'paper_positions' exists with these columns
                stmt = text("""
                    SELECT id, symbol, side, entry_price, upper_barrier, lower_barrier, expiration_ts 
                    FROM paper_positions 
                    WHERE status = 'OPEN'
                """)
                result = await session.execute(stmt)
                rows = result.mappings().all()
                
                async with self._lock:
                    self._active_barriers.clear()
                    self._expiration_heap = []
                    
                    for r in rows:
                        symbol = r['symbol']
                        pos_id = r['id']
                        exp_ts = float(r['expiration_ts'])
                        
                        if symbol not in self._active_barriers:
                            self._active_barriers[symbol] = {}
                            
                        self._active_barriers[symbol][pos_id] = TripleBarrierState(
                            pos_id=pos_id,
                            symbol=symbol,
                            entry_price=float(r['entry_price']),
                            upper_barrier=float(r['upper_barrier']),
                            lower_barrier=float(r['lower_barrier']),
                            expiration_ts=exp_ts,
                            side=r['side'],
                            is_active=True
                        )
                        # Rehydrate Temporal Reaper
                        heapq.heappush(self._expiration_heap, (exp_ts, symbol, pos_id))
                
                # Ensure the heap is valid if we didn't use heappush (optional but safe)
                # heapq.heapify(self._expiration_heap)
                
                logger.success(f"✅ [ShadowTracker] {len(rows)} active missions loaded into memory.")
                self._running = True
            except Exception as e:
                logger.error(f"❌ [ShadowTracker] Rehydration FAILED: {e}")
                raise # Critical: Fail-Fast for Orchestrator

    async def process_tick(self, symbol: str, price: float, timestamp: Optional[float] = None):
        """
        [GEKTOR v21.17] Hot Path: $O(1)$ Barrier Validation.
        Triggered on every L2 update. No DB calls unless a barrier is breached.
        """
        if not self._running:
            return

        now = timestamp or time.time()
        
        # Update System Watermark for Causal Consistency
        if timestamp and timestamp > self._global_watermark:
            self._global_watermark = timestamp
            self._last_tick_arrival = time.monotonic()

        # Effective Time: max(exchange_watermark, local_wall_clock - jitter)
        effective_now = max(
            self._global_watermark,
            now - (self._safe_drift_ms / 1000.0)
        )

        resolved = []

        async with self._lock:
            # 1. TEMPORAL REAPER (O(1) check via heap)
            while self._expiration_heap and self._expiration_heap[0][0] <= effective_now:
                exp_ts, sym, sid = heapq.heappop(self._expiration_heap)
                if sym in self._active_barriers and sid in self._active_barriers[sym]:
                    state = self._active_barriers[sym].pop(sid)
                    resolved.append((state, BarrierHit.TIME_EXPIRATION, price or state.entry_price, now))

            # 2. PRICE VALIDATION (If real tick present)
            if symbol and price and symbol in self._active_barriers:
                for pid, state in list(self._active_barriers[symbol].items()):
                    hit = BarrierHit.NONE
                    # SL > TP Priority (Safety First)
                    if price <= state.lower_barrier:
                        hit = BarrierHit.STOP_LOSS
                    elif price >= state.upper_barrier:
                        hit = BarrierHit.TAKE_PROFIT
                        
                    if hit != BarrierHit.NONE:
                        resolved.append((state, hit, price, now))
                        del self._active_barriers[symbol][pid]

        if resolved:
            asyncio.create_task(self._finalize_positions(resolved))

    async def liveness_pulse(self):
        """Synthetic Heartbeat to trigger Reaper during Trading Halts."""
        logger.info("💓 [Heartbeat] Temporal Pulse started.")
        while True:
            try:
                await self.process_tick(symbol="", price=0.0)
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"💔 [Heartbeat] Pulse Error: {e}")
                await asyncio.sleep(2.0)

    async def _finalize_positions(self, resolutions: List[Tuple[TripleBarrierState, BarrierHit, float, float]]):
        """
        [Audit 21.17] Zero-Friction Finalization.
        Calculates ACTUAL PnL based on the first tick that breached the barrier.
        """
        async with self.db.SessionLocal() as session:
            try:
                for state, hit, exit_price, exit_ts in resolutions:
                    # Calculate PnL (accounting for side and slippage)
                    if state.side == 'LONG':
                        pnl_pct = (exit_price - state.entry_price) / state.entry_price
                    else: # SHORT
                        pnl_pct = (state.entry_price - exit_price) / state.entry_price
                        
                    # Calculate Slip from theoretical barrier
                    theoretical = state.upper_barrier if hit == BarrierHit.TAKE_PROFIT else state.lower_barrier
                    if hit == BarrierHit.TIME_EXPIRATION: theoretical = exit_price
                    
                    slippage = abs(exit_price - theoretical) / theoretical if theoretical != 0 else 0
                    
                    logger.warning(
                        f"🎯 [SHADOW EXIT] {state.symbol} | ID: {state.pos_id} | "
                        f"Reason: {hit.name} | PnL: {pnl_pct:.4%} | Slip: {slippage:.4%}"
                    )
                    
                    # Persist resolution with actual price (No hallucinations of perfect execution)
                    stmt = text("""
                        UPDATE paper_positions 
                        SET status = 'CLOSED', 
                            exit_price = :ep, 
                            exit_ts = :ets, 
                            pnl_pct = :pnl, 
                            exit_reason = :res,
                            updated_at = NOW()
                        WHERE id = :id
                    """)
                    await session.execute(stmt, {
                        "ep": exit_price,
                        "ets": exit_ts,
                        "pnl": pnl_pct,
                        "res": hit.value,
                        "id": state.pos_id
                    })
                
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [ShadowTracker] Finalization Error: {e}")
                # Emergency: Trigger full re-sync to ensure no state is lost
                asyncio.create_task(self.initialize())


