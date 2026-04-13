# src/application/exit_protocol_service.py
import asyncio
import json
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

    async def register_signal(self, symbol: str, price: float, vpin: float, direction: int, timestamp: int):
        """Идемпотентная регистрация новой торговой гипотезы."""
        signal_id = f"{symbol}_{timestamp}"
        async with self._lock:
            if signal_id not in self.active_signals:
                signal = ActiveSignal(
                    signal_id=signal_id,
                    symbol=symbol,
                    entry_ts=timestamp,
                    entry_price=price,
                    direction=direction,
                    max_vpin=vpin
                )
                self.active_signals[signal_id] = signal
                await self._persist_all_signals()
                logger.info(f"🔍 [ExitProtocol] Registered tracking for {symbol} ({'LONG' if direction > 0 else 'SHORT'})")

    async def update_math_state(self, symbol: str, current_vpin: float, current_price: float, timestamp: int):
        """Обновление макро-параметров из MathCore (VPIN, Bars)."""
        async with self._lock:
             for sig in self.active_signals.values():
                 if sig.symbol == symbol:
                     sig.bars_observed += 1
                     sig.max_vpin = max(sig.max_vpin, current_vpin)
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
        """Атомарная фиксация стейта и алерта (Transactional Outbox)."""
        query = "INSERT INTO signals (signal_id, symbol, state, exit_price, exit_vpin) VALUES (:sid, :symbol, :state, :ep, :ev)"
        params = {
            "sid": signal.signal_id,
            "symbol": signal.symbol,
            "state": signal.state.name,
            "ep": price,
            "ev": vpin
        }
        
        # Format payload for Telegram (same as how TelegramRadarNotifier formats it, roughly based on event_data)
        event_dict = {
            "symbol": signal.symbol,
            "price": price,
            "vpin": vpin,
            "timestamp": int(asyncio.get_event_loop().time() * 1000), 
            "abort_mission": True,
            "abort_reason": signal.state.name
        }
        tg_payload = self.tg._format_message(event_dict)
        
        async with self.db.SessionLocal() as session:
            try:
                await session.execute(text(query), params)
                await self.outbox_repo.save_alert_atomically(tg_payload, session)
                await session.commit()
                logger.warning(f"🚨 [ExitProtocol] PREMISE INVALIDATED: {signal.symbol} -> {signal.state.name}")
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ [ExitProtocol] Transaction Failed for {signal.symbol}: {e}")

    async def _persist_all_signals(self):
        """Dump active signals to Redis for survival."""
        try:
            key = "gektor:active_signals"
            data = {sid: {
                "symbol": s.symbol,
                "entry_ts": s.entry_ts,
                "entry_price": s.entry_price,
                "direction": s.direction,
                "state": s.state.name,
                "bars_observed": s.bars_observed,
                "max_vpin": s.max_vpin
            } for sid, s in self.active_signals.items()}
            await self.db.buffer.redis.set(key, json.dumps(data))
        except Exception as e:
            logger.error(f"❌ [ExitProtocol] State persistence failure: {e}")
