# skills/gerald-sniper/core/realtime/relay.py
import asyncio
import time
import heapq
import json
from enum import Enum
from typing import List, Dict, Any, Optional
from loguru import logger
from sqlalchemy import text
from .dto import SignalDTO, TelegramFormatter
from ...data.database import DatabaseManager

class RelayStatus(Enum):
    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    COMPACTED = "COMPACTED"
    AUTONOMOUS_SHADOW = "AUTONOMOUS_SHADOW"
    FAILED = "FAILED"

class CircuitState(Enum):
    CLOSED = "CLOSED"  # All good
    OPEN = "OPEN"      # Egress dead, use autonomous fallback
    HALF_OPEN = "HALF_OPEN" # Testing if connection restored

class SignalPayload:
    __slots__ = ['db_id', 'symbol', 'prob', 'vol', 'ev', 'payload']
    
    def __init__(self, db_id: int, symbol: str, prob: float, vol: float, payload: dict):
        self.db_id = db_id
        self.symbol = symbol
        self.prob = prob
        self.vol = vol
        self.payload = payload
        # EV = Brier_Probability * sigma (Volatility)
        self.ev = self.prob * self.vol

    def __lt__(self, other):
        # Max-Heap behavior for heapq (we want highest EV first)
        return self.ev > other.ev

class CompactingOutboxRelay:
    """
    [GEKTOR v21.16] Institutional Egress Compactor.
    Phase 6 Hardening: Handles HTTP 429 Flood Control via EV-based Top-K.
    Implements Autonomous Survival Protocol (Circuit Breaker).
    """
    
    def __init__(self, db: DatabaseManager, tg_client, window_ms: int = 1500, top_k: int = 3):
        self.db = db
        self.tg_client = tg_client
        self.window_ms = window_ms / 1000.0
        self.top_k = top_k
        self._signal_heap: List[SignalPayload] = []
        self._flush_event = asyncio.Event()
        
        # Circuit Breaker state
        self.circuit_state = CircuitState.CLOSED
        self.failure_count = 0
        self.failure_threshold = 3
        self.recovery_timeout = 60.0 # Time to stay OPEN before HALF_OPEN
        self.last_failure_ts = 0.0
        
        # Autonomous Alpha Threshold (Rule 2.1)
        self.critical_ev_threshold = 0.85 

    async def notify_new_signals(self, signals: List[Dict[str, Any]]):
        """LISTEN/NOTIFY bridge for new outbox events."""
        for sig in signals:
            item = SignalPayload(
                db_id=sig['id'],
                symbol=sig['symbol'],
                prob=sig.get('prob', 0.5),
                vol=sig.get('vol', 1.0),
                payload=sig.get('payload', {})
            )
            heapq.heappush(self._signal_heap, item)
        
        self._flush_event.set()

    async def relay_loop(self):
        logger.info(f"🚀 [Relay] CompactingOutboxRelay active (Window: {self.window_ms*1000}ms, Top-K: {self.top_k})")
        while True:
            try:
                await self._flush_event.wait()
                self._flush_event.clear()

                # Accumulation window (wait for the storm to manifest)
                await asyncio.sleep(self.window_ms)

                if not self._signal_heap:
                    continue

                await self._process_batch()
            except Exception as e:
                logger.error(f"❌ [Relay] Critical Loop Error: {e}")
                await asyncio.sleep(1)

    async def _process_batch(self):
        batch = []
        while self._signal_heap:
            batch.append(heapq.heappop(self._signal_heap))

        if not batch: return

        winners = batch[:self.top_k]
        losers = batch[self.top_k:]

        # Check Circuit Breaker health
        if self.circuit_state == CircuitState.OPEN:
            if time.time() - self.last_failure_ts > self.recovery_timeout:
                logger.info("🔄 [Relay] Circuit HALF_OPEN: Attempting recovery...")
                self.circuit_state = CircuitState.HALF_OPEN
        
        if self.circuit_state == CircuitState.OPEN:
            await self._handle_autonomous_survival(winners + losers)
            return

        # Normal Egress (CLOSED or HALF_OPEN)
        delivered_count = 0
        try:
            for winner in winners:
                success = await self.tg_client.send_message(
                    self._format_msg(winner),
                    parse_mode="HTML"
                )
                
                if success:
                    await self._update_status(winner.db_id, RelayStatus.DELIVERED)
                    delivered_count += 1
                    if self.circuit_state == CircuitState.HALF_OPEN:
                        logger.success("✅ [Relay] Recovery successful. Circuit CLOSED.")
                        self.circuit_state = CircuitState.CLOSED
                        self.failure_count = 0
                else:
                    raise ConnectionError("Telegram delivery failed (HTTP 429/5xx)")

            # Atomic Compaction for noise
            if losers:
                loser_ids = [l.db_id for l in losers]
                await self._update_status_bulk(loser_ids, RelayStatus.COMPACTED)
                logger.info(f"📉 [Relay] Compacted {len(losers)} low-EV signals.")

        except Exception as e:
            await self._handle_egress_failure(winners, e)

    async def _handle_egress_failure(self, winners: List[SignalPayload], error: Exception):
        self.failure_count += 1
        self.last_failure_ts = time.time()
        logger.error(f"🛑 [Relay] Egress Failure ({self.failure_count}/{self.failure_threshold}): {error}")
        
        if self.failure_count >= self.failure_threshold:
            logger.critical("🚨 [Relay] CIRCUIT BREAKER OPEN: Telegram is unreachable. Pivioting to Autonomous Survival.")
            self.circuit_state = CircuitState.OPEN
            
        # For the current failed batch, we must decide: Re-queue or Shadow?
        # High EV winners from this failed batch move to Shadow immediately
        await self._handle_autonomous_survival(winners)

    async def _handle_autonomous_survival(self, items: List[SignalPayload]):
        """
        [GEKTOR v21.16] Autonomous Survival Protocol (ASP).
        If connection to Operator is lost, high-conviction signals are 
        escalated to Shadow Execution to ensure no opportunity is lost.
        """
        autonomous_count = 0
        for item in items:
            if item.ev >= self.critical_ev_threshold:
                # 1. Promote to Shadow Mission
                # In a real system, we'd trigger the Phase 5 Shadow Tracker here
                logger.warning(f"👻 [Relay] [ASP] {item.symbol} Escalated to SHADOW (EV: {item.ev:.4f})")
                await self._update_status(item.db_id, RelayStatus.AUTONOMOUS_SHADOW)
                autonomous_count += 1
            else:
                # Too weak for autonomous action, keep as PENDING for retry or just fail
                await self._update_status(item.db_id, RelayStatus.FAILED)
        
        if autonomous_count > 0:
            logger.info(f"🌑 [Relay] ASP: {autonomous_count} signals processed in MISSION-SILENT mode.")

    def _format_msg(self, sig: SignalPayload) -> str:
        """Трансляция сигнала в HTML-шаблон Telegram."""
        try:
            # Превращаем сырой словарь обратно в валидированный DTO
            # Это гарантирует, что мы не отправим "битый" или "пустой" сигнал
            dto = SignalDTO(**sig.payload)
            return TelegramFormatter.build_alert(dto)
        except Exception as e:
            logger.error(f"⚠️ [Relay] Formatter Error: {e}. Falling back to minimalist alert.")
            return f"🚨 <b>SIGNAL ERROR</b>\nSymbol: {sig.symbol}\nError: Data Contract Breach"

    async def _update_status(self, db_id: int, status: RelayStatus):
        async with self.db.SessionLocal() as session:
            try:
                await session.execute(
                    text("UPDATE outbox_events SET status = :s, updated_at = NOW() WHERE id = :id"),
                    {"s": status.value, "id": db_id}
                )
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"DB Update Error: {e}")

    async def _update_status_bulk(self, ids: List[int], status: RelayStatus):
        if not ids: return
        async with self.db.SessionLocal() as session:
            try:
                await session.execute(
                    text("UPDATE outbox_events SET status = :s, updated_at = NOW() WHERE id IN :ids"),
                    {"s": status.value, "ids": tuple(ids)}
                )
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"DB Bulk Update Error: {e}")
