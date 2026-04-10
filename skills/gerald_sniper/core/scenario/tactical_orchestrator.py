# core/scenario/tactical_orchestrator.py

import asyncio
import uuid
from typing import Dict, List, Optional, Set
import time
from loguru import logger
import json
from datetime import datetime

from .signal_accumulator import SignalAccumulator, CONFLUENCE_RULES
from .signal_entity import TradingSignal, SignalState
from core.events.nerve_center import bus
from core.events.events import DetectorEvent, SignalEvent
from core.realtime.regime import RegimeClassifier, MarketRegime
from core.realtime.market_state import market_state
from core.realtime.cognitive_context import cognitive_pipeline
from core.runtime.health import health_monitor

class TacticalOrchestrator:
    """Tactical Orchestrator (Decision Engine) for filtering and executing trades."""
    
    def __init__(self):
        self.db = None
        self.active_signals: Dict[str, TradingSignal] = {}
        self.signal_queue = asyncio.Queue(maxsize=1000)
        self._started = False
        self.paper_tracker = None
        self._btc_context = "FLAT"
        self.decision_engine = None
        self.exposure_controller = None # Task 5.2 Shield
        self.execution_engine = None
        self._lock = asyncio.Lock()
        self._rejected_cooldowns: Dict[tuple, float] = {}
        self._position_cooldowns: Dict[str, float] = {} # key -> expire_ts
        self._emergency_scan_cache: Dict[str, dict] = {} # symbol -> {ts, levels}
        self._tracked_symbols: set[str] = set()
        self.execution_mode = "AUTO" # [Audit 25.13] AUTO | ONLY_CLOSE

    def update_btc_context(self, btc_ctx: dict):
        if isinstance(btc_ctx, dict):
            self._btc_context = btc_ctx.get("trend", "FLAT")

    async def start(self, db_manager, paper_tracker, candle_cache, adapter=None, level_service=None, loop=None, time_sync=None, kill_switch=None, advisory_mode: bool = True):
        if self._started: return
        self.db = db_manager
        self.paper_tracker = paper_tracker
        self.level_service = level_service
        self.candle_cache = candle_cache
        
        # Initialize Engines (Rule: Use config from main)
        from utils.config import config
        from .decision_engine import DecisionEngine
        
        config_dict = config.dict() if hasattr(config, 'dict') else config
        self.decision_engine = DecisionEngine(config_dict)
        
        if advisory_mode:
            self.execution_engine = None
            logger.info("🛠️ [ORCHESTRATOR] ExecutionEngine bypassed (Advisory Mode active).")
        else:
            try:
                from .execution_engine import ExecutionEngine
                # Task 3.3: Inject Adapter via DI
                self.execution_engine = ExecutionEngine(db_manager, adapter, bus.redis, candle_cache, config_dict, time_sync=time_sync, kill_switch=kill_switch)
                await self.execution_engine.start()
            except (ModuleNotFoundError, ImportError):
                logger.error("❌ LIVE MODE FAILURE: ExecutionEngine module missing!")
                raise

        from core.shield.risk_engine import risk_guard
        self.risk_guard = risk_guard
        
        # [Audit 6.15] Signal Ranker (Tumbling Window)
        from .signal_ranker import signal_ranker
        await signal_ranker.start()
        
        if loop is None:
            loop = asyncio.get_event_loop()
        loop.create_task(self.run_loop())
        loop.create_task(self.cleanup_loop())
        self._started = True
        
        # Subscribe to Bus
        bus.subscribe(DetectorEvent, self.handle_detector_event, persistent=True)
        
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
        """Marks pending signals as REJECTED. Optimization: Allow EVALUATING to continue."""
        for sig in list(self.active_signals.values()):
            if sig.symbol == symbol:
                if sig.state == SignalState.EVALUATING:
                    logger.debug(f"🛡️ [Orchestrator] {symbol} removed from watchlist but EVALUATING signal allowed to finish.")
                    continue
                if sig.state == SignalState.DETECTED:
                    sig.transition_to(SignalState.REJECTED, "Symbol removed from watchlist")

    def get_active_signal_symbols(self) -> set[str]:
        """Returns set of symbols currently being evaluated or held."""
        return {sig.symbol for sig in self.active_signals.values() 
                if sig.state in [SignalState.EVALUATING, SignalState.APPROVED, SignalState.POSITION_OPEN]}

    async def handle_detector_event(self, event: DetectorEvent):
        """Bus handler for detector triggers. [REPLAY GUARD v12.0]"""
        if market_state.is_replaying:
            return # Squelch signals during state reconstruction
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
                logger.info(f"📐 [Orchestrator] {symbol} LEVEL_PROXIMITY suppressed (warmup: {warmup:.1f}s)")
                return # Block 1.5
        
        # T-04: Proximity Filter (Dist > 3%)
        dist, lvl_price, _ = await self.get_distance_to_nearest_level(symbol, price, direction)
        
        # Adaptive threshold (Rule 1.6)
        max_dist = 3.0
        if dist is not None and dist > max_dist:
             logger.info(f"📐 [Orchestrator] {symbol} {direction} skipped | Dist: {dist}% > {max_dist}%")
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

            dist, lvl_price, lvl_sources = await self.get_distance_to_nearest_level(symbol, price, direction)
            
            # [Audit v6.0] Register Ingestion
            health_monitor.record_gate_event('received', 'received')
            if health_monitor.is_debug_active(symbol):
                logger.info(f"🔎 [DEBUG:{symbol}] Signal Received | Price: {price} | Dir: {direction} | Det: {event.detector}")

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
            if hasattr(new_sig, 'level_sources'):
                new_sig.level_sources = lvl_sources
            else:
                new_sig.level_sources = lvl_sources

            # Register in memory using stable key
            self.active_signals[key] = new_sig
            logger.info(f"✨ [Orchestrator] New Signal: {symbol} {direction} via {event.detector} | Dist: {dist}%")
        
        await self.put_signal(new_sig)

    async def get_distance_to_nearest_level(self, symbol: str, price: float, direction: str) -> tuple[Optional[float], Optional[float], List[str]]:
        if not price or price <= 0: return None, None, []
        
        logger.debug(f"📐 Calculating distance for {symbol} at {price}...")
        best_dist = 999.0
        best_lvl_price = None
        best_sources = []

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
                return None, None, []

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
                    raw_src = getattr(lvl.ref, 'source', '')
                    best_sources = raw_src.split('+') if raw_src else []

            if best_lvl_price is not None and best_dist < 20.0:
                return round(best_dist, 4), best_lvl_price, best_sources
            
            return None, None, []
        except Exception as e:
            logger.error(f"Distance calculation error: {e}")
            return None, None, []

    async def process_signal(self, signal: TradingSignal):
        """Главный цикл принятия решений."""
        if signal.state in [SignalState.POSITION_OPEN, SignalState.REJECTED, SignalState.APPROVED]:
            return
            
        # Task 3.1: Fail-Closed Price Guard
        if not signal.entry_price or signal.entry_price <= 0:
            logger.error(f"❌ [Orchestrator] {signal.symbol} KILLED: Missing price data (Fail-Closed)")
            signal.transition_to(SignalState.REJECTED, "MISSING_PRICE_DATA")
            # Immediate cleanup key
            self.active_signals.pop(f"{signal.symbol}:{signal.direction}", None)
            return

        # 0. Cooldown & BTC Trend Check (Early Rejection T-02)
        now = time.time()
        cooldown_key = (signal.symbol, signal.direction)
        
        # EPIC 4: Market Regime Check
        from core.realtime.market_state import market_state
        from core.realtime.regime import RegimeClassifier, MarketRegime
        sym_st = market_state.symbols.get(signal.symbol)
        
        regime = MarketRegime.UNKNOWN
        if sym_st:
            # Task 4.2: Dynamic Regime Classification
            atr_5m = self.candle_mgr.get_atr_abs(signal.symbol, "5") if self.candle_mgr else None
            atr_sma = self.candle_mgr.get_atr_sma(signal.symbol, 14) if self.candle_mgr else None
            
            regime = RegimeClassifier.classify(
                state=sym_st, 
                btc_trend=signal.btc_trend_1h,
                atr_5m=atr_5m,
                atr_sma=atr_sma
            )
            signal.market_regime = regime
            logger.info(f"🌐 [Orchestrator] Regime detected for {signal.symbol}: {regime} (ATR: {atr_5m or 'N/A'})")

        cooldown_window = 900 if regime == MarketRegime.RANGE else 300
        if cooldown_key in self._rejected_cooldowns:
            if now - self._rejected_cooldowns[cooldown_key] < cooldown_window:
                logger.info(f"🔇 [Orchestrator] Skipped {signal.symbol}: In cooldown ({cooldown_window}s window)")
                return
            else:
                del self._rejected_cooldowns[cooldown_key]

        # 0.5 [Armor 2.0] Pre-Flight Risk Check (Non-atomic)
        if self.risk_guard:
            is_ok, reason = self.risk_guard.can_proceed(signal.symbol)
            if not is_ok:
                logger.info(f"🛡️ [RiskGuard] Pre-Flight REJECT {signal.symbol}: {reason}")
                signal.transition_to(SignalState.REJECTED, f"SHIELD_BLOCK: {reason}")
                self._rejected_cooldowns[cooldown_key] = time.time()
                await self._save_signal_stats(signal)
                return

        if getattr(signal, "btc_trend_1h", "FLAT") == "FLAT":
            signal.btc_trend_1h = self._btc_context
            
        # v5.1: MarketSentiment replaces fragmented BTC checks
        from core.realtime.sentiment import market_sentiment
        sentiment_modifier = market_sentiment.confidence_modifier(signal.direction)
        signal._sentiment_modifier = sentiment_modifier
        signal.btc_trend_1h = market_sentiment.get_trend_string()  # Keep backward compat
        
        # Log only when sentiment has meaningful impact
        if abs(sentiment_modifier) >= 0.05:
            logger.info(
                f"🌡️ [Orchestrator] Sentiment: {market_sentiment.score:+d} ({market_sentiment.label}) | "
                f"{signal.symbol} {signal.direction} modifier: {sentiment_modifier:+.3f}"
            )

        self.active_signals[signal.id] = signal
        
        # 1. Transition to EVALUATING
        signal.transition_to(SignalState.EVALUATING, "Waiting for confluence...")
        
        # 2. Accumulation Window (v5.0: Progressive Timeout)
        base_window = CONFLUENCE_RULES["accumulation_window_sec"]  # 180s
        radar = signal.radar_score or 0
        if radar > 80:
            window = 240  # High-quality radar: give more time
        elif radar > 60:
            window = base_window  # Standard: 180s
        else:
            window = 120  # Low radar: don't waste time
            
        start_wait = time.time()
        passed_confluence = False
        conf_score = 0.0
        
        while time.time() - start_wait < window:
            # T-03 Guard: check if signal was already rejected or modified externally
            if signal.state != SignalState.EVALUATING:
                return

            passed_confluence, conf_score, reason = SignalAccumulator.evaluate_confluence(signal)
            if passed_confluence:
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

        # 2.1 Apply MarketSentiment modifier to confidence (v5.1)
        sentiment_mod = getattr(signal, '_sentiment_modifier', 0.0)
        if sentiment_mod != 0.0:
            original = conf_score
            conf_score = round(conf_score + sentiment_mod, 2)
            direction_label = "boost" if sentiment_mod > 0 else "penalty"
            logger.info(f"🌡️ [Orchestrator] Sentiment {direction_label} for {signal.symbol} {signal.direction}: {original:.2f} → {conf_score:.2f} ({sentiment_mod:+.3f})")

        # EPIC 4: RANGE Regime Penalty
        sig_regime = getattr(signal, 'market_regime', None)
        if sig_regime == MarketRegime.RANGE:
            original = conf_score
            penalty = 0.15 # 15% confidence penalty for trading in chop
            conf_score = round(conf_score - penalty, 2)
            logger.warning(f"🦀 [Orchestrator] {signal.symbol} is in RANGE! Applying penalty: {original:.2f} → {conf_score:.2f} (-{penalty})")

        if conf_score < CONFLUENCE_RULES["min_confidence"]:
            health_monitor.record_gate_event('rejected_conf', 'rejected_conf')
            if health_monitor.is_debug_active(signal.symbol):
                logger.info(f"🔎 [DEBUG:{signal.symbol}] Rejected: Confidence {conf_score:.2f} < {CONFLUENCE_RULES['min_confidence']}")
            
            signal.transition_to(SignalState.REJECTED, f"Confidence dropped below {CONFLUENCE_RULES['min_confidence']}: {conf_score:.2f}")
            await self._save_signal_stats(signal)
            return
        
        signal.confidence_score = conf_score  # FIX T-FATAL: Populate attribute
        logger.info(f"🔥 [Orchestrator] Confluence achieved for {signal.symbol}: {signal.detectors} (Conf: {conf_score:.2f})")

        logger.info(f"🧠 [Orchestrator] Final Evaluation for {signal.symbol} | Detectors: {signal.detectors} | Dist: {signal.level_distance_pct}% | BTC: {signal.btc_trend_1h}")
        
        # [HIVE MIND] Агенты (Trend, Volatility, Sentiment) голосуют за сделку
        from core.agents import debate_chamber
        passed, consensus_score, reason = await debate_chamber.hold_debate(signal)
        
        if not passed:
            signal.transition_to(SignalState.REJECTED, f"Agent VETO: {reason}")
            self._rejected_cooldowns[(signal.symbol, signal.direction)] = time.time()
            await self._save_signal_stats(signal)
            return
            
        signal.confidence_score = conf_score * 0.5 + consensus_score * 0.5

        # 3. [NERVE REPAIR v4.0] Routing: APPROVED vs SHADOW
        await self.route_signal(signal)

    async def route_signal(self, signal: TradingSignal):
        """
        [NERVE REPAIR v4.0] Signal Router.
        Branches into Combat Execution or Shadow Logging based on confidence.
        """
        confidence = getattr(signal, "confidence_score", 0.0)
        
        if confidence >= 0.85: # Restored strict HFT limit
            # Combat Path -> RiskGuard -> Execution
            await self._process_combat_signal(signal)
        elif 0.45 <= confidence < 0.85:
            # Shadow Path -> Logging/Observation Only
            await self._process_shadow_signal(signal)
        else:
            logger.debug(f"🔇 [Orchestrator] Signal {signal.symbol} confidence {confidence:.2f} too low for even Shadow Mode")



    async def _process_combat_signal(self, signal: TradingSignal):
        """Combat execution path with non-blocking Ranker (v6.15)."""
        # [Audit 25.13] Memory Backpressure: Lame Duck Mode
        if self.execution_mode == "ONLY_CLOSE":
            logger.warning(f"🦆 [LAME_DUCK] Refusing to open fresh {signal.symbol} {signal.direction}. Resource depletion in progress!")
            signal.transition_to(SignalState.REJECTED, "LAME_DUCK_THRESHOLD_REACHED")
            return

        logger.info(f"📥 [Orchestrator] Sending {signal.symbol} to Ranking Buffer...")
        
        signal_id = f"sig_{str(uuid.uuid4())[:14]}"
        
        # Inject cognitive bias if available
        from core.realtime.cognitive_context import cognitive_pipeline
        cognitive_mod = cognitive_pipeline.get_current_bias(signal.direction)
        final_conf = round(signal.confidence_score + cognitive_mod, 2)
        
        event = SignalEvent(
            signal_id=signal_id,
            symbol=signal.symbol,
            direction=signal.direction,
            confidence=final_conf,
            price=signal.entry_price,
            factors=list(signal.detectors),
            market_regime=getattr(signal, 'market_regime', MarketRegime.UNKNOWN).value,
            liquidity_tier=getattr(signal, 'liquidity_tier', 'C'),
            metadata={
                "entry": signal.entry_price,
                "shadow_mode": False,
                "radar_score": signal.radar_score, # For ranker
                "priority": 2 # NORMAL baseline, ranker will escalate winner to 3
            }
        )
        
        from .signal_ranker import signal_ranker
        await signal_ranker.add_signal(event)

    async def _process_shadow_signal(self, signal: TradingSignal):
        """Shadow mode: No execution, only logging to Redis/DB."""
        confidence = getattr(signal, 'confidence_score', 0.0)
        logger.info(f"👁️ [Shadow Mode] {signal.symbol} {signal.direction} logged (Conf: {confidence:.2f})")
        
        # [Task 15.6] Send Shadow Alert for awareness if confidence > 0.5
        if confidence >= 0.5:
             try:
                 from core.alerts.outbox import telegram_outbox, MessagePriority, TelegramOutbox
                 from core.alerts.formatters import format_execution_alert
                 msg = format_execution_alert(signal, f"shadow_{signal.id}", ghost=True)
                 msg = msg.replace("🚀 EXECUTION", "👁️ SHADOW SIGNAL")
                 
                 alert_hash = TelegramOutbox.compute_alert_hash(signal.symbol, "SHADOW", signal.id)
                 telegram_outbox.enqueue(
                     msg, 
                     priority=MessagePriority.LOW,
                     disable_notification=True,
                     alert_hash=alert_hash
                 )
             except Exception as e:
                 logger.error(f"Failed to send shadow alert: {e}")

        await self._save_signal_stats(signal, is_shadow=True)

    def _on_position_opened(self, symbol: str, direction: str):
        """Sets a cooldown after a position is successfully opened."""
        key = f"{symbol}:{direction}"
        self._position_cooldowns[key] = time.time() + 120
        logger.info(f"🔒 [Orchestrator] Cooldown active for {key} (120s)")

    async def _save_signal_stats(self, signal: TradingSignal, is_shadow: bool = False):
        if not self.db: return
        
        # v4.0: Metadata for stats
        metadata = {"is_shadow": is_shadow}
        
        await self.db.insert_signal_stat(
            signal_id=signal.id,
            symbol=signal.symbol,
            state=signal.state.value,
            entry_price=signal.entry_price or 0.0,
            detectors=",".join(signal.detectors),
            rejection_reason=f"{'[SHADOW] ' if is_shadow else ''}{signal.rejection_reason or ''}",
        )
        # v5.0: Publish rejection event for metrics tracking
        if signal.state == SignalState.REJECTED and signal.rejection_reason:
            try:
                from core.events.safe_publish import safe_publish
                asyncio.create_task(safe_publish(RejectionEvent(
                    symbol=signal.symbol,
                    direction=signal.direction,
                    reason=signal.rejection_reason
                )))
            except Exception:
                pass

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
                    
                # [Task 4] Manual Trigger: Between ticks
                from core.runtime.memory_manager import mem_manager
                mem_manager.trigger_hft_gc(1)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
            await asyncio.sleep(60)

# Global instance
tactical_orchestrator = TacticalOrchestrator()
