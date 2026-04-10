# skills/gerald_sniper/core/realtime/shadow.py
import asyncio
import time
import heapq
from typing import Dict, List, Optional, Any
from loguru import logger
from pydantic import BaseModel
import asyncpg
from sqlalchemy import text

class ShadowMission(BaseModel):
    id: str
    symbol: str
    side: str
    entry_price: float
    pessimistic_entry: float
    tp_price: float
    sl_price: float
    expiration_ms: int
    volume_usd: float
    status: str = "ACTIVE"

class ShadowTrackerApex:
    """
    [GEKTOR v21.49] High-Performance Shadow Execution Engine.
    - O(1) Check Complexity via Min-Max Heaps.
    - Full Causal Recovery from PostgreSQL.
    - Pessimistic Execution Model (Sigma-based Slippage).
    """
    def __init__(self, db_pool, taker_fee: float = 0.0006, slippage_k: float = 0.5):
        self.db = db_pool # Это sqlalchemy engine или db_manager. Мы будем использовать его Session
        self.taker_fee_rate = taker_fee
        self.slippage_k = slippage_k
        
        self.active_missions: Dict[str, ShadowMission] = {}
        
        # Spatial Indexing (Heaps) for O(1) price cross checks
        # Format: (price_level, signal_id)
        self._long_tp_heap = [] # Min-heap (inverted price for max-heap behavior)
        self._long_sl_heap = [] # Min-heap 
        self._short_tp_heap = [] # Min-heap
        self._short_sl_heap = [] # Min-heap (inverted price)

    async def hydrate_state(self):
        """Восстановление активных миссий из БД при старте (State Recovery)."""
        try:
            async with self.db.SessionLocal() as session:
                result = await session.execute(text("SELECT * FROM shadow_missions WHERE status = 'ACTIVE'"))
                records = result.fetchall()
                for r in records:
                    mission = ShadowMission(
                        id=r.signal_id, symbol=r.symbol, side=r.side,
                        entry_price=r.entry_price, pessimistic_entry=r.pessimistic_entry,
                        tp_price=r.tp_price, sl_price=r.sl_price,
                        expiration_ms=r.expiration_ms, volume_usd=r.volume_usd, status=r.status
                    )
                    self._register_in_memory(mission)
            logger.info(f"🕵️ [Shadow] Rehydrated {len(self.active_missions)} missions from PostgreSQL.")
        except Exception as e:
            logger.error(f"❌ [Shadow] Hydration failed: {e}")

    def _register_in_memory(self, m: ShadowMission):
        self.active_missions[m.id] = m
        if m.side == "BUY":
            heapq.heappush(self._long_tp_heap, (-m.tp_price, m.id)) # Invert for >= check
            heapq.heappush(self._long_sl_heap, (m.sl_price, m.id))  # Normal for <= check
        else:
            heapq.heappush(self._short_tp_heap, (m.tp_price, m.id)) # Normal for <= check
            heapq.heappush(self._short_sl_heap, (-m.sl_price, m.id))# Invert for >= check

    async def register_signal(self, dto):
        """Создает миссию и фиксирует ее в БД атомарно."""
        side_factor = 1 if dto.side == "BUY" else -1
        slippage_pct = dto.sigma * self.slippage_k
        pessimistic_entry = dto.price * (1 + (side_factor * slippage_pct / 100))
        
        # Triple Barrier Calculation
        tp_dist = dto.price * (dto.sigma * 2.5 / 100)
        sl_dist = dto.price * (dto.sigma * 1.0 / 100)
        tp_price = pessimistic_entry + tp_dist if dto.side == "BUY" else pessimistic_entry - tp_dist
        sl_price = pessimistic_entry - sl_dist if dto.side == "BUY" else pessimistic_entry + sl_dist
        expiration = int(time.time() * 1000) + (4 * 3600 * 1000)

        mission = ShadowMission(
            id=dto.signal_id, symbol=dto.symbol, side=dto.side,
            entry_price=dto.price, pessimistic_entry=pessimistic_entry,
            tp_price=tp_price, sl_price=sl_price, expiration_ms=expiration,
            volume_usd=dto.volume_usd
        )

        # 1. Persistence
        try:
            async with self.db.SessionLocal() as session:
                async with session.begin():
                    await session.execute(text("""
                        INSERT INTO shadow_missions 
                        (signal_id, symbol, side, entry_price, pessimistic_entry, tp_price, sl_price, expiration_ms, volume_usd)
                        VALUES (:id, :symbol, :side, :ep, :pe, :tp, :sl, :exp, :vol)
                    """), {
                        "id": mission.id, "symbol": mission.symbol, "side": mission.side,
                        "ep": mission.entry_price, "pe": mission.pessimistic_entry,
                        "tp": mission.tp_price, "sl": mission.sl_price,
                        "exp": mission.expiration_ms, "vol": mission.volume_usd
                    })
            # 2. Memory
            self._register_in_memory(mission)
            logger.info(f"🕵️ [Shadow] Mission {mission.id} Armed & Persisted.")
        except Exception as e:
            logger.error(f"❌ [Shadow] Failed to persist mission: {e}")

    async def apply_market_tick(self, symbol: str, price: float, ts_ms: int):
        """
        Проверка тика за O(1). 
        Использует кучи для мгновенного определения пробития барьеров.
        """
        resolved = []

        # LONG Barriers
        while self._long_sl_heap and self._long_sl_heap[0][0] >= price:
            _, sig_id = heapq.heappop(self._long_sl_heap)
            if sig_id in self.active_missions:
                resolved.append((sig_id, "LOSS", price))

        while self._long_tp_heap and (-self._long_tp_heap[0][0]) <= price:
            _, sig_id = heapq.heappop(self._long_tp_heap)
            if sig_id in self.active_missions:
                resolved.append((sig_id, "PROFIT", price))
        
        # SHORT Barriers
        while self._short_tp_heap and self._short_tp_heap[0][0] >= price:
            _, sig_id = heapq.heappop(self._short_tp_heap)
            if sig_id in self.active_missions:
                resolved.append((sig_id, "PROFIT", price))

        while self._short_sl_heap and (-self._short_sl_heap[0][0]) <= price:
            _, sig_id = heapq.heappop(self._short_sl_heap)
            if sig_id in self.active_missions:
                resolved.append((sig_id, "LOSS", price))

        if resolved:
            await self._persist_resolutions(resolved)
        
        # Periodic expiration check (Time Barrier)
        # In a real system, this would be a separate ticker/loop to avoid cluttering apply_tick
        # But we can check a few missions per tick or every X ticks.

    async def _persist_resolutions(self, resolutions: list):
        """Атомарное обновление статуса миссий в БД и памяти."""
        try:
            async with self.db.SessionLocal() as session:
                async with session.begin():
                    for sig_id, outcome, price in resolutions:
                        mission = self.active_missions.pop(sig_id, None)
                        if mission:
                            await session.execute(text(
                                "UPDATE shadow_missions SET status = :status, exit_price = :price WHERE signal_id = :id"
                            ), {"status": outcome, "price": price, "id": sig_id})
                            logger.success(f"🏁 [Shadow] Mission {sig_id} resolved: {outcome} @ {price:.2f}")
        except Exception as e:
            logger.error(f"❌ [Shadow] Resolution persistence failed: {e}")
