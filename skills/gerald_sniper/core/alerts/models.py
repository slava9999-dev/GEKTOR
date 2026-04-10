from enum import StrEnum
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional
import uuid

class AlertType(StrEnum):
    RADAR_UPDATE = "radar_update"          # итог радара по watchlist
    LEVEL_ARMED = "level_armed"            # уровни пересчитаны/заряжены
    LEVEL_PROXIMITY = "level_proximity"    # цена близко к уровню
    TRIGGER_FIRED = "trigger_fired"        # реальный сигнал (пробой/отскок)
    SYSTEM_DEGRADED = "system_degraded"    # Проблемы с WS/DB
    SYSTEM_RECOVERED = "system_recovered"  # Восстановление

class LevelState(StrEnum):
    NEW = "new"                # только что создан
    ARMED = "armed"            # мониторится
    NEAR = "near"              # в зоне близости (proximity)
    COOLDOWN = "cooldown"      # недавно был алерт
    TRIGGERED = "triggered"    # триггер сработал
    INVALID = "invalid"        # больше не актуален
    DEGRADED = "degraded"      # ошибки мониторинга

@dataclass(frozen=True)
class LevelRef:
    symbol: str
    timeframe: str
    side: str                   # "SUPPORT" | "RESISTANCE"
    level_price: Decimal
    tolerance_pct: float
    source: str                 # "KDE", "ROUND_NUMBER", etc.
    touches: int
    score: float
    level_id: str               # Стабильный хэш

@dataclass
class LevelRuntime:
    ref: LevelRef
    state: LevelState = LevelState.NEW
    last_seen_ts: datetime = field(default_factory=datetime.utcnow)
    last_price: Optional[Decimal] = None
    last_distance_pct: Optional[float] = None
    last_alert_ts: Dict[str, datetime] = field(default_factory=dict)
    cooldown_until: Dict[str, datetime] = field(default_factory=dict)
    
    # Structural Context
    htf_confluence: Dict[str, Any] = field(default_factory=lambda: {'score': 0.0, 'matches': [], 'tf_tags': ''})
    compression: Optional[Any] = None # CompressionResult
    trendline: Optional[Any] = None # TrendlineResult
    trade_intensity: float = 1.0
    radar_context: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class AlertEvent:
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    ts: datetime = field(default_factory=datetime.utcnow)
    type: AlertType = AlertType.LEVEL_PROXIMITY
    severity: str = "INFO"      # "INFO" | "WARN" | "ERROR"
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    level_id: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
