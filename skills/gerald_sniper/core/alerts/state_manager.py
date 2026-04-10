import asyncio
import hashlib
import json
import os
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from loguru import logger
from typing import Dict, List, Optional, Tuple, Any

from core.alerts.models import (
    AlertType, LevelState, LevelRef, LevelRuntime, AlertEvent
)
from core.runtime.health import health_monitor
from core.alerts.cooldown import cooldown_store
from core.alerts.formatters import format_alert_to_telegram
from core.scenario.engine import scenario_engine
from core.activity.trade_tracker import trade_tracker
from core.structure.detector import detect_all
from core.structure.models import StructureType
from core.levels.models import LevelCluster, LevelSide

STATE_FILE = "./data_run/alert_state.json"

class AlertStateManager:
    """
    Manages the lifecycle and state of trading levels and system alerts.
    Implements proximity logic, cooldowns, and state transitions.
    """
    def __init__(self):
        self.levels: Dict[str, LevelRuntime] = {}  # key: level_id
        self.run_id = hashlib.md5(str(datetime.utcnow()).encode()).hexdigest()[:8]
        self.load_state()
        
    def save_state(self):
        """Persists levels and cooldowns to JSON, handling non-serializable types."""
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            
            # v4.2: Runtime state pruning (prevent unbounded growth)
            now = datetime.utcnow()
            cutoff = now - timedelta(hours=self.STATE_TTL_HOURS)
            
            # 1. Evict INVALID and expired levels
            to_remove = [
                lid for lid, rt in self.levels.items()
                if rt.state == LevelState.INVALID or rt.last_seen_ts < cutoff
            ]
            for lid in to_remove:
                del self.levels[lid]
            
            # 2. Enforce MAX_STATE_LEVELS cap (keep most recent)
            if len(self.levels) > self.MAX_STATE_LEVELS:
                sorted_items = sorted(
                    self.levels.items(),
                    key=lambda x: x[1].last_seen_ts,
                    reverse=True
                )
                self.levels = dict(sorted_items[:self.MAX_STATE_LEVELS])
            
            # 3. Clean expired cooldowns
            cooldown_store.cleanup()
            
            data = {
                'levels': {},
                'cooldowns': []
            }
            
            # Serialize levels
            for lid, runtime in self.levels.items():
                ref = runtime.ref
                data['levels'][lid] = {
                    'ref': {
                        'symbol': ref.symbol, 'timeframe': ref.timeframe, 'side': str(ref.side),
                        'level_price': str(ref.level_price), 'tolerance_pct': ref.tolerance_pct,
                        'source': ref.source, 'touches': ref.touches, 'score': ref.score, 'level_id': ref.level_id
                    },
                    'state': str(runtime.state),
                    'last_seen_ts': runtime.last_seen_ts.isoformat()
                }
            
            # Serialize cooldowns (already cleaned)
            cds = cooldown_store.get_all_active()
            for key_str, ts in cds.items():
                data['cooldowns'].append({
                    'key': key_str.split('|'),
                    'until': ts.isoformat()
                })
                    
            with open(STATE_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug(f"💾 Alert state persisted ({len(self.levels)} levels, {len(data['cooldowns'])} cooldowns)")
        except Exception as e:
            logger.error(f"Error saving alert state: {e}")

    MAX_STATE_LEVELS = 300  # v4.2: Increased from 50 to accommodate 20+ symbols * 10 levels
    STATE_TTL_HOURS = 24

    def load_state(self):
        """Restores levels and cooldowns from JSON with cleanup."""
        if not os.path.exists(STATE_FILE):
            logger.info("📂 No state file found. Fresh start.")
            return

        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)

            now = datetime.utcnow()
            cutoff = now - timedelta(hours=self.STATE_TTL_HOURS)
            
            raw_levels = data.get('levels', {})
            total_raw = len(raw_levels)
            
            # 1. Filter by TTL and valid data
            cleaned_runtimes = {}
            for lid, ldata in raw_levels.items():
                try:
                    last_seen = datetime.fromisoformat(ldata['last_seen_ts'])
                    if last_seen < cutoff:
                        continue
                    
                    r = ldata['ref']
                    ref = LevelRef(
                        symbol=r['symbol'], timeframe=r['timeframe'], side=r['side'],
                        level_price=Decimal(r['level_price']), tolerance_pct=r['tolerance_pct'],
                        source=r['source'], touches=r['touches'], score=r['score'], level_id=r['level_id']
                    )
                    cleaned_runtimes[lid] = LevelRuntime(
                        ref=ref, 
                        state=LevelState(ldata['state']),
                        last_seen_ts=last_seen
                    )
                except (KeyError, ValueError, TypeError) as e:
                    logger.debug(f"Skipping malformed level state {lid}: {e}")
                    continue

            # 2. Cap by count (most recent first)
            if len(cleaned_runtimes) > self.MAX_STATE_LEVELS:
                sorted_items = sorted(
                    cleaned_runtimes.items(),
                    key=lambda x: x[1].last_seen_ts,
                    reverse=True
                )
                self.levels = dict(sorted_items[:self.MAX_STATE_LEVELS])
            else:
                self.levels = cleaned_runtimes

            # 3. Load cooldowns
            for c in data.get('cooldowns', []):
                try:
                    key = c['key']  # [type, symbol, lid]
                    until = datetime.fromisoformat(c['until'])
                    if until > now:
                        cooldown_store._store["|".join(key)] = until
                except (KeyError, ValueError):
                    continue

            logger.info(f"📂 Alert state restored ({len(self.levels)} levels, cleaned from {total_raw})")
        except Exception as e:
            logger.error(f"📂 State load failed. Fresh start. Error: {e}")
            self.levels = {}
    def generate_level_id(self, symbol: str, timeframe: str, side: str, price: float) -> str:
        """Generates a stable, quantized level ID (v4.3: Removed source dependency)."""
        # Quantize price to avoid float jitter
        p_dec = Decimal(str(price))
        if p_dec > 1000:
            q = Decimal('1.0')      # 70000.5 -> 70001
        elif p_dec > 50:
            q = Decimal('0.05')    # 87.039 -> 87.05
        elif p_dec > 1:
            q = Decimal('0.001')   # 1.1234 -> 1.123
        else:
            q = Decimal('0.0001')  # 0.12345 -> 0.1235
        
        quantized_price = p_dec.quantize(q, rounding=ROUND_HALF_UP)
        raw_id = f"{symbol}|{timeframe}|{side}|{quantized_price}"
        return hashlib.blake2s(raw_id.encode(), digest_size=6).hexdigest()

    def get_proximity_thresholds(self, atr_pct: float) -> Tuple[float, float]:
        """
        Radar v2 Proximity Logic:
        proximity = ATR_pct * 0.6
        Clamp: min = 0.2%, max = 1.2%
        """
        proximity_pct = max(0.2, min(1.2, atr_pct * 0.6))
        # Touch threshold represents a tighter zone for REACHED state
        # Usually 1/5th of proximity or max 0.1%
        touch_pct = min(proximity_pct * 0.2, 0.1)
        
        return proximity_pct, touch_pct

    def update_levels_from_scan(self, symbol: str, timeframe: str, active_levels: List[Any], radar_metrics: Any = None) -> List[AlertEvent]:
        """Updates the registry with fresh levels from a radar/math scan. Support dict or LevelCluster."""
        current_ids = []
        events = []
        now = datetime.utcnow()
        
        radar_ctx = {}
        if radar_metrics:
            radar_ctx = {
                'volume_24h_usd': getattr(radar_metrics, 'volume_24h_usd', 0.0),
                'atr_pct': getattr(radar_metrics, 'atr_pct', 0.0),
                'trade_count': getattr(radar_metrics, 'trade_count_24h', 0)
            }
        
        for lvl in active_levels:
            if isinstance(lvl, dict):
                price = lvl['price']
                side = lvl['type'] # SUPPORT | RESISTANCE
                source = lvl.get('source', 'KDE')
                touches = lvl.get('touches', 1)
                score = lvl.get('strength', 0)
                lid = lvl.get('level_id')
            else: # LevelCluster
                price = lvl.price
                side = str(lvl.side)
                source = "+".join(lvl.sources)
                touches = lvl.touches_total
                score = lvl.strength
                lid = lvl.level_id

            # v4.3: Spatial Deduping Check
            # If we don't have a specific lid, check if any existing level for this symbol/side is "close enough"
            existing_lid = None
            if not lid:
                for active_lid, runtime in self.levels.items():
                    if (runtime.ref.symbol == symbol and 
                        runtime.ref.side == side and 
                        runtime.state != LevelState.INVALID):
                        
                        dist_pct = abs(float(runtime.ref.level_price) - price) / price * 100
                        if dist_pct < 0.5: # 0.5% proximity threshold for IDENTITY
                            existing_lid = active_lid
                            break
            
            lvl_id = lid or existing_lid or self.generate_level_id(symbol, timeframe, side, price)
            current_ids.append(lvl_id)
            
            if lvl_id not in self.levels:
                ref = LevelRef(
                    symbol=symbol, timeframe=timeframe, side=side,
                    level_price=Decimal(str(price)), tolerance_pct=0.2,
                    source=source, touches=touches, score=score, level_id=lvl_id
                )
                self.levels[lvl_id] = LevelRuntime(
                    ref=ref, state=LevelState.ARMED, last_seen_ts=now,
                    radar_context=radar_ctx
                )
                if not isinstance(lvl, dict):
                    self.levels[lvl_id].htf_confluence['score'] = lvl.confluence_score
                    self.levels[lvl_id].htf_confluence['tf_tags'] = " ".join([s.split(":")[1] for s in lvl.sources if ":" in s])
                
                logger.debug(f"💎 Armed Level: {symbol} {side} @ {price} ({source})")
                
                if not self.is_in_cooldown(AlertType.LEVEL_ARMED, symbol, lvl_id):
                    events.append(self._create_alert(self.levels[lvl_id], AlertType.LEVEL_ARMED, {}))
                    self.set_cooldown(AlertType.LEVEL_ARMED, symbol, lvl_id, minutes=240)
            else:
                rt = self.levels[lvl_id]
                rt.last_seen_ts = now
                # Update attributes but keep the same ID and state
                rt.ref = LevelRef(
                    symbol=symbol, timeframe=timeframe, side=side,
                    level_price=Decimal(str(price)), # Update to latest calculated price
                    tolerance_pct=rt.ref.tolerance_pct,
                    source=source, touches=touches, score=score, level_id=lvl_id
                )
                if radar_ctx: rt.radar_context.update(radar_ctx)
                if rt.state == LevelState.INVALID: rt.state = LevelState.ARMED

        # Mark levels no longer in scan as INVALID
        # (Only for levels of THIS symbol and timeframe)
        for lid, runtime in self.levels.items():
            if runtime.ref.symbol == symbol and runtime.ref.timeframe == timeframe:
                if lid not in current_ids and runtime.state != LevelState.INVALID:
                    runtime.state = LevelState.INVALID
                    logger.debug(f"👻 Level Invalidated: {lid} ({symbol})")

        # v4.2: Inline eviction if state grows too large between save_state calls
        if len(self.levels) > self.MAX_STATE_LEVELS * 2:
            invalid_ids = [lid for lid, rt in self.levels.items() if rt.state == LevelState.INVALID]
            for lid in invalid_ids:
                del self.levels[lid]

        return events

    def is_in_cooldown(self, alert_type: AlertType, symbol: str, level_id: str = "") -> bool:
        return cooldown_store.is_active(alert_type, symbol, level_id)

    def set_cooldown(self, alert_type: AlertType, symbol: str, level_id: str = "", minutes: int = 15):
        cooldown_store.set(alert_type, symbol, level_id, minutes)

    async def process_price_update(self, symbol: str, price: float, atr_pct: float, candles: List[dict] = None) -> List[AlertEvent]:
        """Main state machine logic. Delegates evaluation to ScenarioEngine."""
        events = []
        now = datetime.utcnow()
        activity = trade_tracker.get_activity(symbol, atr_pct)
        
        relevant_levels = [
            l for l in self.levels.values() 
            if l.ref.symbol == symbol and l.state != LevelState.INVALID
        ]
        
        for runtime in relevant_levels:
            # 1. Structural confirmation (Only evaluate if close to level to save CPU)
            structures = []
            dist_pct = abs(price - float(runtime.ref.level_price)) / price * 100
            if candles and dist_pct < 1.0:
                # Map LevelRuntime to LevelCluster for ScenarioEngine
                cluster = LevelCluster(
                    symbol=symbol, price=float(runtime.ref.level_price),
                    side=LevelSide.SUPPORT if runtime.ref.side == "SUPPORT" else LevelSide.RESISTANCE,
                    sources=[runtime.ref.source], touches_total=runtime.ref.touches,
                    confluence_score=runtime.htf_confluence.get('score', 0),
                    strength=runtime.ref.score, 
                    tolerance_pct=runtime.ref.tolerance_pct / 100.0 if runtime.ref.tolerance_pct > 1 else runtime.ref.tolerance_pct,
                    level_id=runtime.ref.level_id
                )
                structures = detect_all(symbol, runtime.ref.timeframe, candles, cluster)
                # Cache results in runtime
                runtime.trade_intensity = activity.trade_intensity
                runtime.compression = next((s for s in structures if s.type == StructureType.COMPRESSION), None)
            else:
                # Use empty or cached if far away
                cluster = LevelCluster(
                    symbol=symbol, price=float(runtime.ref.level_price),
                    side=LevelSide.SUPPORT if runtime.ref.side == "SUPPORT" else LevelSide.RESISTANCE,
                    sources=[runtime.ref.source], touches_total=runtime.ref.touches,
                    confluence_score=runtime.htf_confluence.get('score', 0),
                    strength=runtime.ref.score, 
                    tolerance_pct=runtime.ref.tolerance_pct / 100.0 if runtime.ref.tolerance_pct > 1 else runtime.ref.tolerance_pct,
                    level_id=runtime.ref.level_id
                )
            
            # 2. Evaluate Scenario
            new_events = scenario_engine.evaluate(cluster, activity, structures, price)
            events.extend(new_events)
            
            # Simple state tracking for monitor-only
            dist_pct = abs(price - float(runtime.ref.level_price)) / price * 100
            if dist_pct < 0.8:
                runtime.state = LevelState.NEAR
            else:
                runtime.state = LevelState.ARMED
            
            runtime.last_seen_ts = now
            runtime.last_price = Decimal(str(price))

        return events

    def create_event_from_trigger(self, trigger: dict, score: int, priority: str) -> AlertEvent:
        """
        Adapts old-style TriggerDetector output to premium AlertEvent.
        """
        symbol = trigger.get('symbol', 'UNKNOWN')
        lid = trigger.get('level_id') or trigger.get('level_data', {}).get('level_id', '')
        
        # Update state if tracked
        if lid in self.levels:
            self.levels[lid].state = LevelState.TRIGGERED
            
        payload = {
            'level_price': float(trigger['level']),
            'side': trigger.get('direction', 'UNKNOWN'),
            'pattern': trigger.get('pattern', 'UNKNOWN'),
            'score': score,
            'priority': priority,
            'description': trigger.get('description', ''),
            'multi_bonus': trigger.get('multi_bonus', 0),
            'squeeze_bars': trigger.get('squeeze_bars'),
            'volume_ratio': trigger.get('volume_ratio'),
            'structure_type': trigger.get('pattern', '').lower().split('_')[0] 
        }
        
        return AlertEvent(
            type=AlertType.TRIGGER_FIRED,
            symbol=symbol,
            level_id=lid,
            severity="WARN" if priority in ('important', 'critical') else "INFO",
            payload=payload
        )

    def track_health_event(self, component: str, event_type: str):
        if component == "ws" and event_type == "reconnect":
            health_monitor.record_ws_reconnect()
        elif component == "ws" and event_type == "parse_error":
            health_monitor.record_ws_parse_error()
        elif component == "db":
            health_monitor.record_db_error()
        elif component == "tg":
            health_monitor.record_tg_error()
        
    async def check_system_health(self) -> List[AlertEvent]:
        """Returns health alerts based on quality metrics."""
        events = []
        report = health_monitor.status_report
        
        if health_monitor.is_ws_degraded:
            if not self.is_in_cooldown(AlertType.SYSTEM_DEGRADED, "system", "ws"):
                events.append(AlertEvent(
                    type=AlertType.SYSTEM_DEGRADED, severity="WARN",
                    payload={"msg": f"WS issues: {report['ws_reconnects_10m']} reconns, {report['ws_parse_errors_10m']} errors"}
                ))
                self.set_cooldown(AlertType.SYSTEM_DEGRADED, "system", "ws", 30)
                
        return events

    def _create_alert(self, runtime: LevelRuntime, alert_type: AlertType, payload: dict) -> AlertEvent:
        # Standardized payload
        full_payload = {
            'level_price': float(runtime.ref.level_price),
            'side': runtime.ref.side,
            'source': runtime.ref.source,
            'touches': runtime.ref.touches,
            'score': runtime.ref.score,
            'last_price': float(runtime.last_price) if runtime.last_price else None,
            'distance_pct': runtime.last_distance_pct,
            'run_id': self.run_id,
            'htf_confluence': runtime.htf_confluence.get('tf_tags', ''),
            'confluence_score': runtime.htf_confluence.get('score', 0.0),
            'structure': 'compression' if runtime.compression else 'none'
        }
        # Include radar context for formatting
        if runtime.radar_context:
            full_payload.update(runtime.radar_context)
        
        full_payload.update(payload)
        
        return AlertEvent(
            type=alert_type,
            symbol=runtime.ref.symbol,
            timeframe=runtime.ref.timeframe,
            level_id=runtime.ref.level_id,
            severity="INFO",
            payload=full_payload
        )

# Global Instance
alert_manager = AlertStateManager()
