# src/application/exit_protocol_service.py
import asyncio
import json
import time
from typing import List, Dict, Optional
from loguru import logger
from sqlalchemy import text

from src.domain.exit_protocol import ActiveSignal, MarketTick, InvalidationRule, SignalState, VPINDecayRule
from src.infrastructure.config import settings
from src.application.outbox_relay import OutboxRepository

class SignalTracker:
    """[GEKTOR APEX] Reactive Signal Monitoring & Invalidation Engine."""
    def __init__(self, db, tg):
        self.db = db # Expects DatabaseManager
        self.tg = tg # Expects TelegramRadarNotifier
        self.outbox_repo = OutboxRepository(db)
        self.active_signals: Dict[str, ActiveSignal] = {}
        self.rules: List[InvalidationRule] = []
        self._lock = asyncio.Lock()
        
        # Default Rules
        self.rules.append(VPINDecayRule(settings.EXIT_VPIN_DECAY_FACTOR))

    def add_rule(self, rule: InvalidationRule):
        self.rules.append(rule)

    async def hydrate_signals(self):
        """Восстановление стейта из Redis (или SQL) при старте."""
        try:
            # GEKTOR Protocol v2.0: Active signals are source of truth for protection
            key = "gektor:active_signals"
            data = await self.db.buffer.redis.get(key)
            if data:
                signals_data = json.loads(data)
                for sid, sdata in signals_data.items():
                    self.active_signals[sid] = ActiveSignal(
                        signal_id=sid,
                        symbol=sdata["symbol"],
                        entry_ts=sdata["entry_ts"],
                        entry_price=sdata["entry_price"],
                        direction=sdata["direction"],
                        state=SignalState[sdata["state"]],
                        bars_observed=sdata.get("bars_observed", 0),
                        max_vpin=sdata.get("max_vpin", 0.0)
                    )
                logger.success(f"📟 [ExitProtocol] Hydrated {len(self.active_signals)} active premises.")
        except Exception as e:
            logger.error(f"⚠️ [ExitProtocol] Hydration failure: {e}")

    async def register_signal(self, symbol: str, price: float, vpin: float, direction: int, timestamp: int, 
                              entry_bid: float = 0.0, entry_ask: float = 0.0):
        """Идемпотентная регистрация новой торговой гипотезы с учетом спреда."""
        signal_id = f"{symbol}_{timestamp}"
        async with self._lock:
            if signal_id not in self.active_signals:
                signal = ActiveSignal(
                    signal_id=signal_id,
                    symbol=symbol,
                    entry_ts=timestamp,
                    entry_price=price,
                    entry_bid=entry_bid,
                    entry_ask=entry_ask,
                    direction=direction,
                    max_vpin=vpin
                )
                self.active_signals[signal_id] = signal
                
                # [LATENCY GUARD] Запускаем таймер для фиксации "Биологического входа"
                asyncio.create_task(self._capture_human_latency_baseline(signal))
                
                await self._persist_all_signals()
                logger.info(f"🔍 [ExitProtocol] Registered tracking for {symbol} ({'LONG' if direction > 0 else 'SHORT'}) | Spread: {abs(entry_ask - entry_bid):.2f}")

    async def _capture_human_latency_baseline(self, signal: ActiveSignal):
        """Фиксация цен через 1200мс для проверки 'Альфы для людей'."""
        await asyncio.sleep(1.2) # Biological + Network Relay Latency
        
        # We need current prices. In this architecture, we can get them from the orchestrator's state 
        # or we assume process_tick keeps us updated. 
        # For simplicity, we'll store the latest prices in the SignalTracker itself via update_math_state.
        pass

    async def update_math_state(self, symbol: str, current_vpin: float, current_price: float, timestamp: int,
                                bid: float = 0.0, ask: float = 0.0):
        """Обновление макро-параметров и L1 Snapshot."""
        async with self._lock:
             for sig in self.active_signals.values():
                 if sig.symbol == symbol:
                     sig.bars_observed += 1
                     sig.max_vpin = max(sig.max_vpin, current_vpin)
                     
                     # Fill latency baseline if it's the first update after 1200ms
                     if sig.human_entry_bid == 0 and bid > 0:
                          # This is a bit non-deterministic but works for shadow ledger analysis
                          # We check if 1.2s has passed since entry
                          if (timestamp - sig.entry_ts) > 1200:
                               sig.human_entry_bid = bid
                               sig.human_entry_ask = ask
                               logger.debug(f"⏱️ [LATENCY] Captured human baseline for {symbol}")

             await self._persist_all_signals()

    async def process_tick(self, tick: MarketTick, current_vpin: float):
        """Реактивная проверка приходящих тиков."""
        to_remove = []
        async with self._lock:
            for sid, sig in self.active_signals.items():
                if sig.symbol != tick.symbol: continue
                
                for rule in self.rules:
                    invalidation_state = rule.check(sig, tick, current_vpin)
                    if invalidation_state:
                         sig.state = invalidation_state
                         await self._trigger_abort(sig, current_vpin, tick.price)
                         to_remove.append(sid)
                         break
            
            if to_remove:
                for sid in to_remove: self.active_signals.pop(sid)
                await self._persist_all_signals()

    async def _trigger_abort(self, signal: ActiveSignal, vpin: float, price: float):
        """[NON-BLOCKING] Atomic emission of state and alert via Redis WAL."""
        signal_query = """
            INSERT INTO signals (
                signal_id, symbol, state, entry_bid, entry_ask, 
                exit_bid, exit_ask, human_entry_bid, human_entry_ask, exit_vpin
            ) VALUES (:sid, :symbol, :state, :eb, :ea, :xb, :xa, :heb, :hea, :ev)
        """
        # Note: We assume 'price' here is the current exit price (Mid or Last).
        # To be precise, we should use Bid/Ask from the 'tick' or latest update.
        # For now, we'll use price as a proxy for both if specific ones aren't in tick.
        signal_params = {
            "sid": signal.signal_id,
            "symbol": signal.symbol,
            "state": signal.state.name,
            "eb": signal.entry_bid,
            "ea": signal.entry_ask,
            "xb": price, # Exit Bid (approximation if not provided)
            "xa": price, # Exit Ask (approximation if not provided)
            "heb": signal.human_entry_bid,
            "hea": signal.human_entry_ask,
            "ev": vpin
        }
        
        # 1. Push Signal Record to WAL (Redis)
        await self.db.push_query_to_wal(signal_query, signal_params)

        # 2. Push Alert to Outbox via WAL (Redis)
        event_dict = {
            "symbol": signal.symbol,
            "price": price,
            "vpin": vpin,
            "timestamp": int(time.time() * 1000), 
            "abort_mission": True,
            "abort_reason": signal.state.name
        }
        tg_payload = self.tg._format_message(event_dict)
        
        outbox_query = "INSERT INTO outbox_events (payload) VALUES (:payload)"
        await self.db.push_query_to_wal(outbox_query, {"payload": tg_payload})

        logger.warning(f"🚨 [ExitProtocol] PREMISE INVALIDATED (Pushed to WAL): {signal.symbol} -> {signal.state.name}")

    async def _persist_all_signals(self):
        """Dump active signals to Redis for survival."""
        try:
            key = "gektor:active_signals"
            data = {sid: {
                "symbol": s.symbol,
                "entry_ts": s.entry_ts,
                "entry_price": s.entry_price,
                "entry_bid": s.entry_bid,
                "entry_ask": s.entry_ask,
                "human_entry_bid": s.human_entry_bid,
                "human_entry_ask": s.human_entry_ask,
                "direction": s.direction,
                "state": s.state.name,
                "bars_observed": s.bars_observed,
                "max_vpin": s.max_vpin
            } for sid, s in self.active_signals.items()}
            await self.db.buffer.redis.set(key, json.dumps(data))
        except Exception as e:
            logger.error(f"❌ [ExitProtocol] State persistence failure: {e}")
