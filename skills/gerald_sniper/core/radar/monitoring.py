# core/radar/monitoring.py
import asyncio
import time
import json
import logging
from loguru import logger
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from core.realtime.market_state import market_state
from core.radar.risk_tracker import risk_tracker
from core.radar.preemptive_ladder import preemptive_ladder, PreEmptiveLadderEngine
from core.radar.basis_guard import basis_guard, SpotPremiumGuard
from core.radar.triple_barrier import TripleBarrierEngine, triple_barrier, BarrierState
from core.radar.absorption_detector import IcebergAbsorptionDetector, iceberg_detector

@dataclass(slots=True)
class MonitoringContext:
    signal_id: str
    symbol: str
    entry_price: float
    size_pct: float = 0.0
    is_long: bool = True
    sigma_db: float = 0.02 # [GEKTOR v21.15] Dollar Bar Volatility
    returns: List[float] = field(default_factory=list) # To compute rolling sigma
    vpin_history: List[float] = field(default_factory=list)
    predicted_probability: float = 0.5  # [GEKTOR v21.15.2] For Brier Score calibration
    
    # [GEKTOR v21.15] Triple Barrier State (persisted alongside context)
    barrier_state: Optional[BarrierState] = None
    
    # [GEKTOR v21.4] Adaptive Liquidity Pulse
    last_bar_timestamp: float = field(default_factory=time.time)
    ema_bar_interval: float = 3600.0  # Default 1h (SMA initialization)
    
    def update_pulse(self, new_timestamp: float) -> None:
        """EMA recurrence for expected Bar Interval."""
        if self.last_bar_timestamp > 0:
            delta_t = new_timestamp - self.last_bar_timestamp
            # Alpha = 0.1 (Institutional smoothing standard)
            self.ema_bar_interval = 0.1 * delta_t + 0.9 * self.ema_bar_interval
        self.last_bar_timestamp = new_timestamp

    @property
    def drought_threshold(self) -> float:
        """Dynamic exit trigger: 5x the expected interval."""
        return self.ema_bar_interval * 5.0

    def update_volatility(self, current_price: float, prev_price: float) -> None:
        """Rolling sigma of Dollar Bar log-returns. No Time Bars allowed."""
        import math
        if prev_price > 0:
            log_ret = math.log(current_price / prev_price)
            self.returns.append(log_ret)
            if len(self.returns) > 50: self.returns.pop(0) # 50-bar rolling window
            
            if len(self.returns) >= 10:
                mean = sum(self.returns) / len(self.returns)
                var = sum((x - mean) ** 2 for x in self.returns) / len(self.returns)
                self.sigma_db = math.sqrt(var)
        
    @property
    def current_trend(self) -> float:
        """Assesses institutional pressure (VPIN shift)."""
        if not self.vpin_history:
            return 0.0
        # SMA(3) for noise filtering
        window = self.vpin_history[-3:]
        return sum(window) / len(window)

class MonitoringOrchestrator:
    """
    [GEKTOR v21.4] Phase 5: Exit Radar.
    Adaptive Pulse Monitoring (López de Prado standard).
    """
    def __init__(
        self, 
        db_manager, 
        inversion_threshold: float = -0.45
    ):
        self.db = db_manager
        self.inversion_threshold = inversion_threshold
        self.active_contexts: Dict[str, MonitoringContext] = {}
        from core.radar.liquidity import LiquidityEngine
        self.liquidity_engine = LiquidityEngine(stale_threshold_ms=500)

    async def hydrate_from_db(self) -> None:
        """Restores state on Composition Root initialization."""
        records = await self.db.fetch_active_contexts()
        for r in records:
            # Parse context JSON from DB
            raw_ctx = json.loads(r['context']) if isinstance(r['context'], str) else r['context']
            
            context = MonitoringContext(
                signal_id=r['signal_id'],
                symbol=r['symbol'],
                entry_price=raw_ctx.get('entry_price', 0.0),
                size_pct=raw_ctx.get('size_pct', 0.0),
                last_bar_timestamp=time.time(), # Reset clock at startup
                ema_bar_interval=raw_ctx.get('ema_bar_interval', 3600.0),
                barrier_state=BarrierState.from_dict(raw_ctx['barrier_state']) if 'barrier_state' in raw_ctx else None,
                predicted_probability=raw_ctx.get('predicted_probability', 0.5)
            )
            self.active_contexts[r['symbol']] = context
            
            # [STRESS TEST] Cold-Start Integrity Check (Stale Rehydration Trap)
            from core.realtime.market_state import market_state
            current_price = market_state.get_mark_price(r['symbol'])
            if current_price > 0 and context.barrier_state:
                # Immediate evaluation: are we already behind the wall while server was off?
                barrier_event = triple_barrier.evaluate(r['symbol'], current_price, context.barrier_state)
                if barrier_event and barrier_event.event_type != "TIME_BARRIER_EXPIRATION":
                    logger.critical(f"⚠️ [Phase 5] [{r['symbol']}] POST_RECOVERY_EXPOSURE! Price is beyond barriers on startup.")
                    await self._execute_evacuation(context, barrier_event)
                    self.active_contexts.pop(r['symbol'], None)

        logger.info(f"♻️ [Phase 5] Rehydrated {len(self.active_contexts)} active contexts.")

    async def process_tick_fast_track(self, symbol: str, current_price: float, volume_usd: float = 0.0) -> None:
        """
        [GEKTOR v21.15] Intra-bar Fast Track (Tick-by-Tick).
        Eliminates 'Intra-bar Blindness'. Evaluation happens with L1 micro-latency.
        """
        context = self.active_contexts.get(symbol)
        if not context or not context.barrier_state: return

        # 1. Evaluate Price Barriers (UPPER/LOWER) with Volume Confirmation
        barrier_event = triple_barrier.evaluate(symbol, current_price, context.barrier_state, volume_usd)
        
        if barrier_event and barrier_event.event_type != "TIME_BARRIER_EXPIRATION":
            alert_msg = TripleBarrierEngine.format_barrier_alert(barrier_event)
            # Idempotency: atomic_emergency_abort handles DB state and prevents double alerts
            try:
                await self.db.atomic_emergency_abort(symbol, {
                    "type": barrier_event.event_type,
                    "reason": "FAST_TRACK_TRIGGER",
                    "msg": alert_msg,
                    "pnl_pct": barrier_event.pnl_pct,
                    "priority": barrier_event.priority
                })
                
                # [GEKTOR v21.15.2] Record outcome for Model Decay Monitor
                from core.radar.model_decay import decay_monitor, TradeOutcome
                is_success = 1 if barrier_event.is_profit else 0
                decay_monitor.record_outcome(TradeOutcome(
                    correlation_id=context.signal_id,
                    predicted_prob=context.predicted_probability,
                    is_success=is_success,
                    timestamp=time.time()
                ))
                # Check for calibration drift
                verdict = decay_monitor.evaluate()
                if verdict.is_blind:
                    await self.db.atomic_emergency_abort(symbol, {
                        "type": "MODEL_DECAY_ALARM",
                        "reason": "BRIER_SCORE_PENETRATION",
                        "msg": decay_monitor.format_alert(verdict),
                        "priority": 2
                    })

                self.active_contexts.pop(symbol, None) # Clean up in-memory context
            except Exception as e:
                logger.error(f"📊 [Fast-Track] Alert failed for {symbol}: {e}")

    async def process_macro_bar(self, symbol: str, vpin_score: float) -> None:
        """
        Event-Driven Handler for new Macro-Bars.
        Called by the Radar when a Dollar Bar forms.
        """
        context = self.active_contexts.get(symbol)
        if not context: return

        # 0. Update Volatility (Internal State Shift)
        # Fetching price from the market_state singleton
        from core.realtime.market_state import market_state
        current_price = market_state.get_mark_price(symbol)
        context.update_volatility(current_price, context.entry_price) # Simplification for now

        # [GEKTOR v21.15] Check Sigma-Ladder Drift
        new_ladder = preemptive_ladder.check_ladder_drift(
            symbol, current_price, context.sigma_db
        )
        if new_ladder:
            # Ladder is stale — push rebalance alert via outbox
            old_ladder_data = preemptive_ladder._active_ladders.get(symbol)
            drift_msg = PreEmptiveLadderEngine.format_drift_alert(
                symbol, 
                old_ladder_data if old_ladder_data else new_ladder,
                new_ladder
            )
            try:
                await self.db.atomic_emergency_abort(symbol, {
                    "type": "LADDER_DRIFT_REBALANCE",
                    "reason": "VOLATILITY_DRIFT",
                    "msg": drift_msg
                })
            except Exception as e:
                logger.error(f"🔄 [Phase 5] Ladder drift alert failed for {symbol}: {e}")

        # [GEKTOR v21.15] Basis Guard: Detect Derivative Dislocation
        # If futures price is crashing toward ladder but spot is stable → phantom dislocation
        dislocation = basis_guard.evaluate(symbol, current_price)
        if dislocation.is_dislocated:
            alert_msg = SpotPremiumGuard.format_dislocation_alert(symbol, dislocation)
            logger.critical(
                f"🌊 [Phase 5] [{symbol}] DERIVATIVE DISLOCATION! "
                f"Basis={dislocation.basis_pct:+.3%} Z={dislocation.basis_z_score:.1f}σ. "
                f"CANCEL LADDER!"
            )
            try:
                await self.db.atomic_emergency_abort(symbol, {
                    "type": "DERIVATIVE_DISLOCATION",
                    "reason": dislocation.reason,
                    "msg": alert_msg,
                    "basis_pct": dislocation.basis_pct,
                    "index_price": dislocation.index_price
                })
            except Exception as e:
                logger.error(f"🌊 [Phase 5] Dislocation alert failed for {symbol}: {e}")

        now = time.time()
        context.update_pulse(now)
        context.vpin_history.append(vpin_score)
        
        # Persist pulse update to DB (Institutional State Persistence)
        await self.db.update_monitoring_context(symbol, {
            "entry_price": context.entry_price,
            "ema_bar_interval": context.ema_bar_interval,
            "last_bar_timestamp": context.last_bar_timestamp,
            "barrier_state": context.barrier_state.to_dict() if context.barrier_state else None,
            "predicted_probability": context.predicted_probability
        })
        
        if len(context.vpin_history) > 10:
            context.vpin_history.pop(0)

        # 1. Check for Institutional Distribution (VPIN Inversion — Global Abort)
        if context.current_trend <= self.inversion_threshold:
            logger.critical(f"🛑 [Phase 5] [{symbol}] VPIN INVERSION: {context.current_trend:.4f}. ABORTING.")
            await self._execute_abort(context, reason="VPIN_INVERSION")
            return  # No further checks after global abort

        # 2. [GEKTOR v21.15] Triple Barrier Engine — Macro Track (Time Decay)
        if context.barrier_state is not None:
            bs = context.barrier_state
            
            # 2a. Recalculate barriers (they "breathe" with σ_db)
            triple_barrier.recalculate_barriers(bs, context.sigma_db, current_price)
            
            # 2b. Evaluate Vertical Barrier (Time Decay)
            # Price barriers are already checked by Fast-Track.
            if bs.bars_elapsed >= bs.max_bars:
                alert_msg = TripleBarrierEngine.format_barrier_alert(
                    BarrierEvent(
                        event_type="TIME_BARRIER_EXPIRATION",
                        symbol=symbol,
                        current_price=current_price,
                        barrier_price=0.0,
                        entry_price=context.entry_price,
                        bars_elapsed=bs.bars_elapsed,
                        pnl_pct=0.0 # Placeholder
                    )
                )
                try:
                    await self.db.atomic_emergency_abort(symbol, {
                        "type": "TIME_BARRIER_EXPIRATION",
                        "reason": "VERTICAL_BARRIER_TOUCH",
                        "msg": alert_msg
                    })
                    
                    # [GEKTOR v21.15.2] Record outcome for Model Decay Monitor (Time Decay)
                    from core.radar.model_decay import decay_monitor, TradeOutcome
                    decay_monitor.record_outcome(TradeOutcome(
                        correlation_id=context.signal_id,
                        predicted_prob=context.predicted_probability,
                        is_success=0, # Time expiry is treated as failure of alpha
                        timestamp=time.time()
                    ))
                    # Check for calibration drift
                    verdict = decay_monitor.evaluate()
                    if verdict.is_blind:
                        await self.db.atomic_emergency_abort(symbol, {
                            "type": "MODEL_DECAY_ALARM",
                            "reason": "BRIER_SCORE_PENETRATION",
                            "msg": decay_monitor.format_alert(verdict),
                            "priority": 2
                        })

                    self.active_contexts.pop(symbol, None)
                except Exception as e:
                    logger.error(f"⌛ [Phase 5] Time barrier alert failed for {symbol}: {e}")
                return

            # 2c. Iceberg Detection near Upper Barrier (TP zone)
            tp_target = bs.upper_target if bs.is_long else bs.lower_stop
            iceberg = iceberg_detector.evaluate(
                symbol, current_price, tp_target, context.sigma_db, bs.is_long
            )
            if iceberg and iceberg.is_iceberg:
                alert_msg = IcebergAbsorptionDetector.format_iceberg_alert(symbol, iceberg)
                try:
                    await self.db.atomic_emergency_abort(symbol, {
                        "type": "ICEBERG_ABSORPTION",
                        "reason": iceberg.reason,
                        "msg": alert_msg,
                        "priority": 4
                    })
                except Exception as e:
                    logger.error(f"🧳 [Phase 5] Iceberg alert failed for {symbol}: {e}")

    async def _execute_evacuation(self, context, event):
        """Emergency procedure for Post-Recovery exposure."""
        payload = {
            "type": "RECOVERY_EVACUATION",
            "symbol": context.symbol,
            "reason": f"STALE_POSITION_FOUND_DURING_HYDRATION ({event.event_type})",
            "price": event.current_price,
            "pnl_pct": event.pnl_pct,
            "msg": (
                f"🚨 *STALE RECOVERY: #{context.symbol}*\n"
                f"Position found already beyond barriers after system restart.\n"
                f"Current Price: `${event.current_price:,.4f}`\n"
                f"P&L: `{event.pnl_pct:+.2%}`\n"
                f"⚠️ *EVACUATE BY MARKET NOW.*"
            ),
            "priority": 1 # TOP PRIORITY
        }
        await self.db.atomic_emergency_abort(context.symbol, payload)

    async def run_liquidity_watchdog(self, shutdown_event: asyncio.Event) -> None:
        """
        [GEKTOR v21.4] Adaptive Watchdog.
        Detects 'Zombie Assets' based on statistical expectation E[T].
        """
        logger.info(f"👮 [Phase 5] Adaptive Liquidity Watchdog active.")
        
        while not shutdown_event.is_set():
            try:
                now = time.time()
                zombie_symbols = []
                
                for symbol, ctx in self.active_contexts.items():
                    # [GEKTOR v21.9] Beta Sentry Feeding (BTC-USDT Perpetual Guide)
                    if symbol == 'BTCUSDT':
                        from core.radar.beta_sentry import beta_sentry
                        await beta_sentry.update(market_state.get_state(symbol).last_price)
                    
                    idle_duration = now - ctx.last_bar_timestamp
                    if idle_duration > ctx.drought_threshold:
                        zombie_symbols.append(symbol)
                
                for sym in zombie_symbols:
                    ctx = self.active_contexts[sym]
                    idle_s = now - ctx.last_bar_timestamp
                    # [GEKTOR v21.14] Liquidity-Aware SOS Generation
                    from core.realtime.market_state import market_state
                    snapshot = market_state.get_l2_snapshot(sym) 
                    
                    if snapshot:
                         liq_info = self.liquidity_engine.calculate_capitulation_floor(
                             snapshot, 
                             ctx.size_pct * 10000, # Assume 10k fixed depth max
                             is_long=ctx.is_long
                         )
                    else:
                         # [GEKTOR v21.15] SIGMA LADDER (Blind Mode)
                         # Step: 1.0σ | Strategy: Anchor across the waterfall
                         mark_price = market_state.get_mark_price(sym)
                         sigma = max(ctx.sigma_db, 0.02) # Floor at 2% for safety
                         
                         mults = [2.5, 3.5, 5.0] # 3-Tier Sigma-Ladder
                         weights = [0.4, 0.4, 0.2] # Proportional volume exit
                         
                         ladder = []
                         for m, w in zip(mults, weights):
                             p = mark_price * (1 - (m * sigma)) if ctx.is_long else mark_price * (1 + (m * sigma))
                             ladder.append({"price": round(p, 6), "vol_pct": int(w * 100)})
                             
                         liq_info = {
                             "type": "SIGMA_LADDER",
                             "ladder": ladder,
                             "sigma_db": round(sigma, 4),
                             "confidence": "BLIND_SIGMA_SURVIVAL"
                         }
                    
                    alert_payload = {
                        "reason": "LIQUIDITY_DROUGHT",
                        "type": "ABORT",
                        "liq": liq_info
                    }
                    await self.db.atomic_emergency_abort(sym, alert_payload)
                    self.active_contexts.pop(sym, None)

                # Poll every 60s
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=60)
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"👮 [Phase 5] Scrubber Error: {e}")
                await asyncio.sleep(60)

    async def _execute_abort(self, context: MonitoringContext, reason: str) -> None:
        """
        Atomic escape execution with Microstructural Fair Value.
        """
        now = time.time()
        # Fetch Micro-Price for Fair Value exit instruction
        fair_value = 0.0
        state = market_state.get_state(context.symbol)
        if state and state.l2_radar:
            fair_value = state.l2_radar.get_micro_price() or 0.0

        alert_payload = {
            "type": "ABORT_MISSION",
            "symbol": context.symbol,
            "signal_id": context.signal_id,
            "reason": reason,
            "fair_value_price": fair_value,
            "instruction": "USE LIMIT/TWAP EXECUTION. DO NOT USE MARKET ORDER. SPREADS WIDENED." if reason == "LIQUIDITY_DROUGHT" else "EXIT IMMEDIATELY.",
            "metric": context.current_trend if reason == "VPIN_INVERSION" else f"IDLE_{now - context.last_bar_timestamp:.0f}s"
        }
        
        try:
            # 1. Atomic DB update
            await self.db.atomic_abort_context(context.symbol, alert_payload)
            
            # 2. Memory release (Phase 4 Risk Sync)
            await risk_tracker.release_exposure(context.size_pct)
            
            # 3. Purge from orchestrator
            self.active_contexts.pop(context.symbol, None)
            logger.warning(f"🛑 [Phase 5] [{context.symbol}] Context closed. Risk exposure released.")
        except Exception as e:
            logger.error(f"❌ [Phase 5] Abort failed for {context.symbol}: {e}")

    async def _monitor_global_health(self):
        """
        [GEKTOR v21.9] Beta-Mass-Abort Protocol.
        If BTC/Guide crashes, we kill all Altcoin experiments instantly.
        """
        while True:
            try:
                from core.radar.beta_sentry import beta_sentry
                if beta_sentry.is_alarm_active:
                    logger.critical("🔥 [Phase 5] GLOBAL_BETA_SHOCK! Executing MASS_ABORT protocol.")
                    # Atomic Mass-Abort for all symbols
                    all_symbols = list(self.active_contexts.keys())
                    for sym in all_symbols:
                        if sym == 'BTCUSDT': continue # Guide stays for observation
                        # Final Step (Phase 6): Atomic Causal Abort
                        await self.db.atomic_emergency_abort(sym, {"reason": "SYSTEMIC_BETA_SHOCK", "type": "ABORT"})
                        self.active_contexts.pop(sym, None)
            except Exception as e:
                logger.error(f"❌ [Phase 5] Global Health Sentry Failed: {e}")
            
            await asyncio.sleep(0.5) # High-precision health check
