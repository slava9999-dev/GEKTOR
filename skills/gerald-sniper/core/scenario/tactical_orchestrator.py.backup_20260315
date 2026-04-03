# core/scenario/tactical_orchestrator.py

import asyncio
from typing import Dict, List, Optional
import time
from loguru import logger
import json
from datetime import datetime

from .signal_accumulator import SignalAccumulator, CONFLUENCE_RULES
from .signal_entity import TradingSignal, SignalState
from core.events.event_bus import bus
from core.events.events import DetectorEvent, SignalEvent
from core.realtime.regime import RegimeClassifier, MarketRegime
from core.realtime.market_state import market_state

class TacticalOrchestrator:
    """Tactical Orchestrator (Decision Engine) for filtering and executing trades."""
    
    def __init__(self):
        self.db = None
        self.active_signals: Dict[str, TradingSignal] = {}
        self.signal_queue = asyncio.Queue()
        self._started = False
        self.paper_tracker = None
        self._btc_context = "FLAT"
        self.decision_engine = None
        self.execution_engine = None
        self._lock = asyncio.Lock()
        self._rejected_cooldowns: Dict[tuple, float] = {}
        self._position_cooldowns: Dict[str, float] = {} # key -> expire_ts
        self._emergency_scan_cache: Dict[str, dict] = {} # symbol -> {ts, levels}
        self._tracked_symbols: set[str] = set()

    def update_btc_context(self, btc_ctx: dict):
        if isinstance(btc_ctx, dict):
            self._btc_context = btc_ctx.get("trend", "FLAT")

    async def start(self, db_manager, paper_tracker, level_service=None, loop=None):
        if self._started: return
        self.db = db_manager
        self.paper_tracker = paper_tracker
        self.level_service = level_service
        
        # Initialize Engines (Rule: Use config from main)
        from utils.config import config
        from .decision_engine import DecisionEngine
        from .execution_engine import PaperExecutionEngine
        
        config_dict = config.dict() if hasattr(config, 'dict') else config
        self.decision_engine = DecisionEngine(config_dict)
        self.execution_engine = PaperExecutionEngine(db_manager, paper_tracker, config_dict)
        self.execution_engine.start()
        
        if loop is None:
            loop = asyncio.get_event_loop()
        loop.create_task(self.run_loop())
        loop.create_task(self.cleanup_loop())
        
        # Subscribe to Bus
        bus.subscribe(DetectorEvent, self.handle_detector_event)
        
        self._started = True
        logger.info("🛡️ Tactical Orchestrator [v4.2] ACTIVE")

    async def run_loop(self):
        logger.info("🛡️ Tactical Orchestrator [v4.1] Decision Engine started")
        while True:
            try:
                signal: TradingSignal = await self.signal_queue.get()
                await self.process_signal(signal)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Tactical Orchestrator error: {e}")

    async def put_signal(self, signal: TradingSignal):
        await self.signal_queue.put(signal)

    def sync_watchlist(self, symbols: list[str]):
        """Updates the set of symbols tracked by Radar."""
        self._tracked_symbols = set(symbols)
        # Ensure priority symbols are also tracked
        from core.radar.priority_watchlist import priority_watchlist
        self._tracked_symbols.update(priority_watchlist.symbols)
        logger.debug(f"🛡️ [Orchestrator] Watchlist synced: {len(self._tracked_symbols)} symbols")

    def on_symbol_removed_from_watchlist(self, symbol: str):
        """Called by radar when a symbol is no longer tracked."""
        logger.info(f"🗑️ [Orchestrator] Symbol {symbol} removed from tracking. Invalidating pending signals.")
        if symbol in self._tracked_symbols:
            self._tracked_symbols.remove(symbol)
        self.invalidate_signals_for_symbol(symbol)

    def invalidate_signals_for_symbol(self, symbol: str):
        """Marks all pending signals for a symbol as REJECTED."""
        for sig in list(self.active_signals.values()):
            if sig.symbol == symbol and sig.state in [SignalState.DETECTED, SignalState.EVALUATING]:
                sig.transition_to(SignalState.REJECTED, "Symbol removed from watchlist")

    async def handle_detector_event(self, event: DetectorEvent):
        """Bus handler for detector triggers."""
        await self.put_event(event)

    async def put_event(self, event: DetectorEvent):
        """Processes a raw detector event and aggregates it into a TradingSignal."""
        symbol = event.symbol
        direction = event.direction
        price = event.price
        now_ts = time.time()
        
        # 0. Lifecycle & Cooldown Blocks
        if symbol not in self._tracked_symbols:
            # First time seeing this symbol? (e.g. priority escalation)
            from core.radar.priority_watchlist import priority_watchlist
            if symbol not in priority_watchlist.symbols:
                return # Ignore events for untracked symbols (Rule 1.4)
            self._tracked_symbols.add(symbol)
            
        key = f"{symbol}:{direction}"
        if self._position_cooldowns.get(key, 0) > now_ts:
            return # Silent drop (Rule 1.4)
            
        if event.detector == "LEVEL_PROXIMITY" and self.level_service:
            warmup = self.level_service.get_warmup_remaining(symbol)
            if warmup > 0:
                logger.debug(f"📐 [Orchestrator] {symbol} LEVEL_PROXIMITY suppressed (warmup: {warmup:.1f}s)")
                return # Block 1.5
        
        # T-04: Proximity Filter (Dist > 3%)
        dist, lvl_price = await self.get_distance_to_nearest_level(symbol, price, direction)
        
        # Adaptive threshold (Rule 1.6)
        max_dist = 3.0
        if dist is not None and dist > max_dist:
             logger.debug(f"📐 [Orchestrator] {symbol} {direction} skipped | Dist: {dist}% > {max_dist}%")
             return

        async with self._lock:
            # 1. Aggregation Lookup (Strict key: symbol + direction)
            key = f"{symbol}:{direction}"
            existing_sig = self.active_signals.get(key)
            
            if existing_sig:
                if existing_sig.state in [SignalState.APPROVED, SignalState.POSITION_OPEN]:
                    return
                
                if existing_sig.state in [SignalState.DETECTED, SignalState.EVALUATING]:
                    if event.detector not in existing_sig.detectors:
                        existing_sig.detectors.append(event.detector)
                        logger.info(f"➕ [Signal {symbol}] Added factor: {event.detector} | Total: {len(existing_sig.detectors)}")
                    
                    # Ensure tier is propagated if missing
                    if not existing_sig.liquidity_tier and event.liquidity_tier:
                        existing_sig.liquidity_tier = event.liquidity_tier
                    return

            # 2. Block opposite direction
            for sig in list(self.active_signals.values()):
                if sig.symbol == symbol and sig.direction != direction:
                    if sig.state in [SignalState.POSITION_OPEN, SignalState.APPROVED]:
                        logger.debug(f"🚫 [Orchestrator] Blocked opposite {direction} for {symbol}")
                        return

            # 3. Create new signal
            new_sig = TradingSignal(
                symbol=symbol,
                detectors=[event.detector],
                entry_price=price,
                direction=direction,
                volume_spike_ratio=float(event.payload.get("ratio", 1.0)),
                radar_score=int(event.payload.get("radar_score", 0)),
                liquidity_tier=event.liquidity_tier
            )
            new_sig.level_distance_pct = dist
            new_sig.level_price = lvl_price

            # Register in memory using stable key
            self.active_signals[key] = new_sig
            logger.info(f"✨ [Orchestrator] New Signal: {symbol} {direction} via {event.detector} | Dist: {dist}%")
        
        await self.put_signal(new_sig)

    async def get_distance_to_nearest_level(self, symbol: str, price: float, direction: str) -> tuple[Optional[float], Optional[float]]:
        if not price or price <= 0: return None, None
        
        logger.debug(f"📐 Calculating distance for {symbol} at {price}...")
        best_dist = 999.0
        best_lvl_price = None

        try:
            from core.alerts.state_manager import alert_manager
            # Gather all levels from state manager (active and historical)
            targets = []
            for lvl in alert_manager.levels.values():
                if lvl.ref.symbol == symbol:
                    targets.append(lvl)

            if not targets:
                # If no active levels, the coin isn't in watchlist or levels not calculated
                # This explains the None% issue.
                return None, None

            for lvl in targets:
                l_price = float(lvl.ref.level_price)
                if not l_price or l_price <= 0: continue
                
                dist = abs(price - l_price) / l_price * 100
                
                # Side filtering
                # Relaxed relevance: Breakouts often target/break resistance on LONG
                # We check all levels but prefer the "correct" type
                is_support = "SUPPORT" in str(lvl.ref.side).upper()
                is_priority = (direction == "LONG" and is_support) or (direction == "SHORT" and not is_support)
                
                # Apply penalty for non-priority level types to prefer correct bias
                effective_dist = dist if is_priority else dist * 1.2
                if lvl.state == "ARMED": effective_dist *= 0.8 # Prefer armed levels
                
                if effective_dist < best_dist:
                    best_dist = dist
                    best_lvl_price = l_price

            if best_lvl_price is not None and best_dist < 20.0:
                return round(best_dist, 4), best_lvl_price
            
            return None, None
        except Exception as e:
            logger.error(f"Distance calculation error: {e}")
            return None, None

    async def process_signal(self, signal: TradingSignal):
        """Главный цикл принятия решений."""
        if signal.state in [SignalState.POSITION_OPEN, SignalState.REJECTED, SignalState.APPROVED]:
            return
            
        # 0. Cooldown & BTC Trend Check (Early Rejection T-02)
        now = time.time()
        cooldown_key = (signal.symbol, signal.direction)
        if cooldown_key in self._rejected_cooldowns:
            if now - self._rejected_cooldowns[cooldown_key] < 300:
                logger.debug(f"⏳ [Orchestrator] Cooldown active for {signal.symbol} {signal.direction}")
                return
            else:
                del self._rejected_cooldowns[cooldown_key]

        if getattr(signal, "btc_trend_1h", "FLAT") == "FLAT":
            signal.btc_trend_1h = self._btc_context
            
        # BTC Alignment Early Check
        if signal.direction == "SHORT" and "UP" in str(signal.btc_trend_1h).upper():
            logger.info(f"🚫 [Orchestrator] BTC Early Rejection: {signal.symbol} SHORT vs BTC {signal.btc_trend_1h}")
            signal.transition_to(SignalState.REJECTED, f"BTC Trend mismatch: {signal.btc_trend_1h} vs SHORT")
            return
            
        if signal.direction == "LONG" and "DOWN" in str(signal.btc_trend_1h).upper():
            logger.info(f"🚫 [Orchestrator] BTC Early Rejection: {signal.symbol} LONG vs BTC {signal.btc_trend_1h}")
            signal.transition_to(SignalState.REJECTED, f"BTC Trend mismatch: {signal.btc_trend_1h} vs LONG")
            return

        self.active_signals[signal.id] = signal
        
        # 1. Transition to EVALUATING
        signal.transition_to(SignalState.EVALUATING, "Waiting for confluence...")
        
        # 2. Accumulation Window (Rule 1.1)
        # Wait for confluence factors to accumulate
        window = CONFLUENCE_RULES["accumulation_window_sec"]
            
        start_wait = time.time()
        passed_confluence = False
        conf_score = 0.0
        
        while time.time() - start_wait < window:
            # T-03 Guard: check if signal was already rejected or modified externally
            if signal.state != SignalState.EVALUATING:
                return

            passed_confluence, conf_score, reason = SignalAccumulator.evaluate_confluence(signal)
            if passed_confluence:
                logger.info(f"🔥 [Orchestrator] Confluence achieved for {signal.symbol}: {signal.detectors} (Conf: {conf_score:.2f})")
                break
            await asyncio.sleep(2)
        
        if not passed_confluence:
            # T-03 Guard: don't rejected if already rejected
            if signal.state == SignalState.EVALUATING:
                signal.transition_to(SignalState.REJECTED, f"Confluence Timeout: {reason}")
                key = f"{signal.symbol}:{signal.direction}"
                async with self._lock:
                    if self.active_signals.get(key) is signal:
                        del self.active_signals[key]
            await self._save_signal_stats(signal)
            return

        logger.info(f"🧠 [Orchestrator] Final Evaluation for {signal.symbol} | Detectors: {signal.detectors} | Dist: {signal.level_distance_pct}% | BTC: {signal.btc_trend_1h}")
        
        # 2a. Regime Filter (Roadmap Step 3)
        state = market_state.get_state(signal.symbol)
        if state:
            regime = RegimeClassifier.classify(state, btc_trend=signal.btc_trend_1h)
            logger.info(f"🌐 [Orchestrator] {signal.symbol} Regime: {regime}")
            
            # Rule 3.1: Alignment check
            if regime == MarketRegime.TREND_UP and signal.direction == "SHORT":
                signal.transition_to(SignalState.REJECTED, "Regime Conflict: SHORT vs TREND_UP")
                return
            if regime == MarketRegime.TREND_DOWN and signal.direction == "LONG":
                signal.transition_to(SignalState.REJECTED, "Regime Conflict: LONG vs TREND_DOWN")
                return
            
            # Rule 3.2: Range penalty
            if regime == MarketRegime.RANGE and conf_score < 0.75:
                 signal.transition_to(SignalState.REJECTED, f"Range Market: Confidence {conf_score:.2f} < 0.75")
                 return
        
        # 2b. Decision Engine (FEAT-002)
        if not self.decision_engine:
            logger.error("❌ DecisionEngine not initialized")
            return

        passed, reason = self.decision_engine.evaluate(signal)
        
        if not passed:
            signal.transition_to(SignalState.REJECTED, reason)
            self._rejected_cooldowns[(signal.symbol, signal.direction)] = time.time()
            await self._save_signal_stats(signal)
            return

        # 3. APPROVED
        signal.transition_to(SignalState.APPROVED, "Passed all filters")
        
        # 4. Publish SignalEvent (Roadmap Step 1)
        # This decouples decision from execution/notification
        asyncio.create_task(bus.publish(SignalEvent(
            symbol=signal.symbol,
            direction=signal.direction,
            confidence=signal.confidence_score or 0.0,
            factors=signal.detectors,
            price=signal.entry_price or 0.0,
            metadata={
                "level_price": signal.level_price,
                "dist_pct": signal.level_distance_pct,
                "btc_trend": signal.btc_trend_1h
            }
        )))
        
        # 5. EXECUTION is now handled via EventBus subscription in ExecutionEngine
        pass

    def _on_position_opened(self, symbol: str, direction: str):
        """Sets a cooldown after a position is successfully opened."""
        key = f"{symbol}:{direction}"
        self._position_cooldowns[key] = time.time() + 120
        logger.info(f"🔒 [Orchestrator] Cooldown active for {key} (120s)")

    async def _save_signal_stats(self, signal: TradingSignal):
        if not self.db: return
        await self.db.insert_signal_stat(
            signal_id=signal.id,
            symbol=signal.symbol,
            state=signal.state.value,
            entry_price=signal.entry_price,
            detectors=",".join(signal.detectors),
            rejection_reason=signal.rejection_reason,
        )

    async def cleanup_loop(self):
        """Periodically removes old signals from memory."""
        while True:
            try:
                now = time.time()
                to_remove = []
                async with self._lock:
                    for sid, sig in self.active_signals.items():
                        age = now - sig.detected_at.timestamp()
                        # Remove if older than 1 hour or handled
                        if sig.state in [SignalState.REJECTED, SignalState.POSITION_OPEN]:
                            if age > 3600: to_remove.append(sid)
                        elif age > 14400: # 4 hours for pending
                            to_remove.append(sid)
                    
                    for sid in to_remove:
                        del self.active_signals[sid]
                
                if to_remove:
                    logger.debug(f"🧹 Orchestrator cleanup: removed {len(to_remove)} signals")
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
            await asyncio.sleep(600)

# Global instance
tactical_orchestrator = TacticalOrchestrator()
