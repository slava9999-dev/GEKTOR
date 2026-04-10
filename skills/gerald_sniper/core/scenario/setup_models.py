from enum import StrEnum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Set
from datetime import datetime
import uuid

class SetupStatus(StrEnum):
    OBSERVE = "observe"
    CANDIDATE = "candidate"
    CONFIRMED = "confirmed"
    SENT = "sent"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"

class StrategyType(StrEnum):
    BREAKOUT = "breakout"
    PULLBACK = "pullback"
    REJECTION = "rejection"

class SignalSide(StrEnum):
    LONG = "long"
    SHORT = "short"

@dataclass
class NormalizedEvent:
    event_type: str # 'VelocityShock', 'MicroMomentum', 'Acceleration', 'OBImbalance', 'Proximity', 'TriggerFired'
    symbol: str
    ts: float = field(default_factory=lambda: datetime.utcnow().timestamp())
    payload: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TradeSetup:
    symbol: str
    side: SignalSide
    strategy_type: StrategyType
    entry_zone: List[float]
    stop_loss: float
    take_profits: List[float]
    expires_at: float
    setup_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    rr_ratio: float = 0.0
    confidence: float = 0.0 # 0-100
    factors: Set[str] = field(default_factory=set)
    status: SetupStatus = SetupStatus.OBSERVE
    created_at: float = field(default_factory=lambda: datetime.utcnow().timestamp())
    telegram_msg_id: Optional[str] = None
