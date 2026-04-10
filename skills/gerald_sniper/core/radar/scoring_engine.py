# core/radar/scoring_engine.py
import asyncio
import logging
import json
from dataclasses import dataclass, field
from loguru import logger
from typing import Dict, Optional, List
from core.neural.labeler import AsyncMetaLabeler
from core.radar.risk_tracker import risk_tracker
from core.radar.consensus import CrossExchangeValidator
from core.radar.integrity import PriceIntegrityGuard, PerformanceCircuitBreaker
from core.realtime.market_state import market_state
from core.db.outbox import OutboxRepository
from core.radar.triple_barrier import triple_barrier, TripleBarrierEngine
from core.radar.liquidation_discriminator import LiquidationDiscriminator, liquidation_discriminator
from core.radar.preemptive_ladder import PreEmptiveLadderEngine, preemptive_ladder

class SignalScoringEngine:
    """
    [GEKTOR v21.2] Institutional Signal Scoring & Validation.
    Orchestrates Phase 4: Validates macro-anomalies through Meta-Labeling 
    and ensures atomic delivery via the Transactional Outbox.
    """
    def __init__(
        self, 
        labeler: AsyncMetaLabeler, 
        outbox_repo: OutboxRepository,
        db_manager,
        consensus_validator: CrossExchangeValidator,
        squeeze_discriminator: Optional[LiquidationDiscriminator] = None,
        ladder_engine: Optional[PreEmptiveLadderEngine] = None,
        threshold: float = 0.65,
        total_equity: float = 10000.0,
        win_loss_ratio: float = 2.0,
        max_staleness_ms: int = 1500 # [GEKTOR v21.6] Maximum age of bar data
    ):
        self.labeler = labeler
        self.outbox_repo = outbox_repo
        self.db = db_manager
        self.consensus = consensus_validator
        
        # [GEKTOR v21.15] Squeeze Discrimination & PreEmptive Defense
        self.discriminator = squeeze_discriminator or liquidation_discriminator
        self.ladder_engine = ladder_engine or preemptive_ladder
        
        # [GEKTOR v21.8] Integrity Guards
        self.price_guard = PriceIntegrityGuard(max_allowed_deviation_pct=0.002) # 0.2% hard limit
        self.circuit_breaker = PerformanceCircuitBreaker(critical_lag_ms=200.0) 
        
        self.threshold = threshold
        self.total_equity = total_equity
        self.win_loss_ratio = win_loss_ratio
        self.max_staleness_ms = max_staleness_ms

    async def process_macro_anomaly(self, event) -> None:
        """
        Main handler for VPIN-driven macro anomalies.
        1. Extract Feature Vector.
        2. Async Meta-Labeling Inference (Isolated GIL).
        3. Atomic DB Persistence (Signal + Outbox).
        """
        # [GEKTOR v21.15.2] Model Decay Lock (Phase 4 Safety)
        from core.radar.model_decay import decay_monitor
        if decay_monitor.is_blocked:
            logger.warning(f"🛑 [Scoring] Signal REJECTED: MODEL_BLIND (Concept Drift Active)")
            return

        symbol = event.symbol
        logger.info(f"⚡ [Radar] Captured macro anomaly on {symbol}. Starting Phase 4 validation...")

        # 1. Feature Vector extraction (Order Flow, VPIN, Volatility, Velocity)
        # Assuming event has a method to provide features mapped to self.labeler.feature_names
        features = self._extract_features(event)
        
        # 2. Async Meta-Labeling (Isolates CPU-bound RF.predict inside ProcessPool)
        probability = await self.labeler.get_probability(features)

        if probability is None:
            logger.error(f"⚡ [Radar] Validation failed for {symbol}: ML inference error.")
            return

        logger.info(f"⚡ [Radar] [{symbol}] ML Probability: {probability:.4f} (Threshold: {self.threshold})")

        if probability >= self.threshold:
            # 1. Internal Performance Check (Circuit Breaker)
            perf_now = time.perf_counter()
            processing_lag_ms = (perf_now - getattr(event, 'core_entry_perf', perf_now)) * 1000
            
            if not self.circuit_breaker.check_internal_lag(processing_lag_ms):
                # If internal lag is too high, signal logic might be "blinded"
                return

            # 2. Dynamic Temporal Entropy Guard (IBM Multiplier)
            
            # vwas_ms is already corrected for clock drift by clock_sync.get_corrected_staleness
            vwas_network_ms = getattr(event, 'vwas_ms', 0)
            total_staleness_ms = vwas_network_ms + processing_lag_ms

            # Fetch consensus data
            shadows = self.consensus.shadow_data.get(symbol, {})
            max_z_vol = max([s.z_score_vol for s in shadows.values()] + [0.0])
            
            ibm_multiplier = 1.0 + math.log1p(max_z_vol)
            dynamic_threshold_ms = self.max_staleness_ms * ibm_multiplier

            if total_staleness_ms > dynamic_threshold_ms:
                logger.critical(f"🥀 [Scoring] [{symbol}] Signal REJECTED: DATA_PUTREFACTION. Staleness: {total_staleness_ms:.1f}ms (IBM: {ibm_multiplier:.2f}x)")
                return

            # 3. Price Integrity Guard (Slip protection)
            current_price = market_state.get_last_price(symbol, exchange="bybit")
            if not self.price_guard.is_price_still_valid(event.close, current_price):
                 # Price moved too far during the network/processing lag
                 return

            # 4. Consensus Check (Global Volume Quorum)
            is_real = await self.consensus.validate(symbol, event.vpin)
            if not is_real:
                logger.warning(f"🎭 [Scoring] [{symbol}] Signal REJECTED: LOCAL_HALLUCINATION (Divergence)")
                return

            # 4.5 [GEKTOR v21.15] Liquidation Squeeze Discrimination
            # Prevents false positives from Short Squeeze cascades that mimic VPIN anomalies
            price_direction = 1 if getattr(event, 'direction', 1) > 0 else -1
            vpin_volume_usd = getattr(event, 'bar_volume_usd', 0.0) or (getattr(event, 'close', 0) * getattr(event, 'volume', 0))
            
            squeeze_verdict = self.discriminator.evaluate(
                symbol=symbol,
                vpin_volume_usd=vpin_volume_usd,
                price_direction=price_direction
            )
            
            if squeeze_verdict.is_squeeze:
                logger.critical(
                    f"🦈 [Scoring] [{symbol}] Signal REJECTED: {squeeze_verdict.reason}. "
                    f"P(Sq)={squeeze_verdict.squeeze_probability:.2%}"
                )
                return

            # 5. Core Signal Scoring (Phase 4 Logic)
            kelly_f = max(0, probability - (1 - probability) / self.win_loss_ratio)
            
            # 3.2 Atomic Risk Overlay Validation (In-Memory singleton)
            recommendation = await risk_tracker.validate_signal(kelly_f, probability)
            
            if recommendation.is_suppressed:
                 logger.warning(f"💤 [Radar] [{symbol}] Signal approved but REJECTED by RiskOverlay: {recommendation.reason}")
                 return

            size_pct = recommendation.approved_size_pct
            size_usd = size_pct * self.total_equity
            
            logger.critical(f"🚀 [Radar] [{symbol}] SIGNAL APPROVED (Prob: {probability:.4f}). Size: {size_pct:.1%}. Triggering Phase 5.")
            
            # 5. Atomic Transaction: Signal + Outbox + Monitoring Context (Kleppmann Pattern)
            signal_id = f"sig_{symbol}_{int(time.time())}"
            signal_data = {
                'id': signal_id,
                'symbol': symbol,
                'price': event.last_price,
                'detectors': {
                    'vpin': event.vpin,
                    'ml_prob': probability,
                    'is_macro': True,
                    'z_score_global_vol': max_z_vol,
                    'staleness_ms': total_staleness_ms,
                    'ibm_multiplier': ibm_multiplier,
                    # [GEKTOR v21.15] Squeeze Discrimination Audit Trail
                    'squeeze_probability': squeeze_verdict.squeeze_probability,
                    'squeeze_oi_delta': squeeze_verdict.delta_oi_pct,
                    'squeeze_fr_velocity': squeeze_verdict.funding_velocity,
                    'squeeze_liq_ratio': squeeze_verdict.liq_vol_ratio
                },
                'created_at_precise': time.time() # [GEKTOR v21.8] High-precision for Outbox E2E telemetry
            }
            
            # Using the outbox repository to push with Priority 10 (Standard Entry)
            await self.outbox_repo.push_signal_alert(signal_data, priority=10)
            
            # Context for cold-start rehydration (VPIN inversion, price thresholds)
            quantity = size_usd / event.last_price
            monitoring_context = {
                'vpin_threshold': event.vpin,  # Snapshot at entry
                'entry_price': event.last_price,
                'quantity': quantity,
                'size_pct': size_pct,
                'timestamp': signal_data['id'].split('_')[-1]
            }

            # [GEKTOR v21.15] Generate PreEmptive Sigma-Ladder
            current_sigma = getattr(event, 'sigma_db', 0.02)
            ladder_snapshot = self.ladder_engine.generate_initial_ladder(
                entry_price=event.last_price,
                current_sigma=current_sigma,
                is_long=(price_direction > 0)
            )
            self.ladder_engine.register_active_ladder(symbol, ladder_snapshot)
            ladder_text = PreEmptiveLadderEngine.format_telegram_ladder(ladder_snapshot, size_usd)

            # [GEKTOR v21.15] Initialize Triple Barrier State
            # Barriers breathe with current sigma — precision exit for the jaguar
            is_long = (price_direction > 0)
            tbm_state = triple_barrier.init_barriers(
                entry_price=event.last_price,
                current_sigma=current_sigma,
                is_long=is_long
            )
            tbm_text = TripleBarrierEngine.format_barriers_for_entry(tbm_state)

            # Context for rehydration (persisted to DB)
            monitoring_context['barrier_state'] = tbm_state.to_dict()

            # [GEKTOR v21.15.3] Signal Packaging for Routing
            from core.radar.router import ScoredSignal, signal_router
            
            scored_signal = ScoredSignal(
                correlation_id=signal_id,
                symbol=symbol,
                probability=probability,
                is_long=(price_direction > 0),
                barrier_data={
                    'entry_price': event.last_price,
                    'sigma': current_sigma,
                    'tbm_state': tbm_state.to_dict()
                },
                size_usd=size_usd
            )

            # This payload will be picked up by OutboxRelay and sent to Telegram
            is_shadow = signal_router.is_blind
            outbox_event = {
                'type': 'NEW_SIGNAL',
                'payload': {
                    'signal_id': signal_id,
                    'symbol': symbol,
                    'msg': (
                        f"{'👻 *SHADOW*: ' if is_shadow else '🎯 *COMBAT*: '}*HIGH CONVICTION SIGNAL*\n"
                        f"Asset: #{symbol}\n"
                        f"ML Confidence: {probability*100:.1f}%\n"
                        f"Brier Score Status: {signal_router.mode}\n"
                        f"Recommended Size: ${size_usd:.2f} (Portfolio Adjusted)\n"
                        f"Squeeze Risk: {squeeze_verdict.squeeze_probability:.0%}\n"
                        f"{'⚠️ _Virtual Tracking only - Model Blind._' if is_shadow else '⚔️ _Validated & Persisted._'}\n"
                        f"{ladder_text}\n"
                        f"{tbm_text}"
                    )
                }
            }

            try:
                # ATOMIC WRITE: Signal + Outbox + Monitoring Context
                await self.outbox_repo.save_signal_with_outbox_and_context(
                    signal_data, 
                    outbox_event if not is_shadow else None, # Skip outbox for shadow if desired
                    monitoring_context
                )
                # DELEGATED ROUTING: To Live or Shadow track
                await signal_router.route_signal(scored_signal)
                
            except Exception as e:
                logger.error(f"⚡ [Radar] Atomic persistence failed for {symbol}: {e}")
        else:
            logger.info(f"💤 [Radar] [{symbol}] Signal filtered: Probability {probability:.4f} below threshold.")

    def _extract_features(self, event) -> Dict[str, float]:
        """Maps event data to the ML model's feature space."""
        # This should strictly match the model training names
        return {
            'vpin': getattr(event, 'vpin', 0.0),
            'volatility': getattr(event, 'volatility', 0.0),
            'velocity': getattr(event, 'velocity', 0.0),
            'volume_spike': getattr(event, 'volume_spike', 0.0),
            'orderflow_imbalance': getattr(event, 'ofi', 0.0),
            'rsi': getattr(event, 'rsi', 50.0)
        }
