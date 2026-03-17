# core/scenario/signal_entity.py

from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
import uuid

class SignalState(Enum):
    DETECTED = "detected"          # Первичный триггер
    EVALUATING = "evaluating"      # В процессе проверки фильтрами
    APPROVED = "approved"          # Прошел все фильтры
    REJECTED = "rejected"          # Отклонен фильтрами
    POSITION_OPEN = "position_open"
    CLOSED = "closed"

@dataclass
class TradingSignal:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""
    detectors: List[str] = field(default_factory=list)
    state: SignalState = SignalState.DETECTED
    detected_at: datetime = field(default_factory=datetime.utcnow)
    state_history: List[dict] = field(default_factory=list) # [{state, ts, reason}]
    
    # Факторы для принятия решения
    btc_trend_1h: str = "FLAT"
    level_id: Optional[int] = None
    level_price: Optional[float] = None
    level_distance_pct: Optional[float] = None
    volume_spike_ratio: float = 1.0
    spread_pct: float = 0.0
    radar_score: int = 0
    priority_age_sec: float = 0.0
    liquidity_tier: str = "C"
    confidence_score: float = 0.0 # FIX T-FATAL: Added missing attribute
    
    # Исполнение
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    direction: str = "" # LONG/SHORT
    
    rejection_reason: Optional[str] = None

    def __post_init__(self):
        if not self.state_history:
            self.state_history.append({
                "state": self.state.value,
                "ts": datetime.utcnow().isoformat(),
                "reason": "Initial detection"
            })

    def transition_to(self, new_state: SignalState, reason: Optional[str] = None):
        """Метод смены состояния с логированием."""
        if self.state in [SignalState.REJECTED, SignalState.POSITION_OPEN, SignalState.CLOSED]:
            return # Block transitions from terminal states
            
        old_state = self.state
        self.state = new_state
        if reason:
            self.rejection_reason = reason
            
        entry = {
            "state": new_state.value,
            "ts": datetime.utcnow().isoformat(),
            "reason": reason
        }
        self.state_history.append(entry)
        
        from loguru import logger
        logger.info(f"🔄 [Signal {self.symbol}] {old_state.value} -> {new_state.value}" + (f" ({reason})" if reason else ""))
