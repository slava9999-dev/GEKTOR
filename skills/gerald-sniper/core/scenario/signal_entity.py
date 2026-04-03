# core/scenario/signal_entity.py

from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
import uuid

class SignalState(Enum):
    DETECTED = "detected"          # Initial Trigger
    EVALUATING = "evaluating"      # Processing filters
    APPROVED = "approved"          # Passed filters
    REJECTED = "rejected"          # Blocked by Engine
    POSITION_OPEN = "position_open"
    CLOSED = "closed"

class OrderState(Enum):
    IDLE = "idle"
    PENDING_OPEN = "pending_open"
    OPEN = "open"
    FILLED = "filled"
    PENDING_CANCEL = "pending_cancel"
    CANCELED = "canceled"
    REJECTED = "rejected"

@dataclass
class TradingSignal:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""
    detectors: List[str] = field(default_factory=list)
    state: SignalState = SignalState.DETECTED
    order_state: OrderState = OrderState.IDLE
    detected_at: datetime = field(default_factory=datetime.utcnow)
    state_history: List[dict] = field(default_factory=list) # [{state, ts, reason}]
    
    # Microstructure Safeguards
    initial_iceberg_vol: float = 0.0 # Snapshot for decay detection
    filled_qty: float = 0.0 # Atomic Fill Tracking (v2.1.4)
    order_id: Optional[str] = None # Exchange Order ID
    
    def __post_init__(self):
        self._lock = asyncio.Lock()
        
    async def transition_order_state(self, expected: List[OrderState], new: OrderState, reason: str = "") -> bool:
        """
        Atomic Transition (CAP/Consistency).
        Prevents WebSocket/REST race conditions (The Phantom Fill).
        """
        async with self._lock:
            if self.order_state not in expected:
                import loguru
                loguru.logger.warning(
                    f"⚠️ [Order {self.id}] Illegal transition: "
                    f"{self.order_state.name} -> {new.name} | Current expected: {[s.name for s in expected]}"
                )
                return False
            
            from loguru import logger
            logger.info(f"🔄 [Order {self.id}] State: {self.order_state.name} -> {new.name} ({reason})")
            self.order_state = new
            self.state_history.append({
                "order_state": new.name,
                "ts": datetime.utcnow().isoformat(),
                "reason": reason
            })
            return True
    
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
    market_regime: Optional[str] = None # Task 4.2: Dynamic Microstructure Label
    ob_imbalance: float = 1.0 # Bids / Asks ratio
    cvd_delta: float = 0.0 # Taker Buy - Taker Sell Volume
    is_absorption: bool = False # Price holding vs High CVD pressure
    
    # Исполнение
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    position_size: Optional[float] = None # Added for Risk Engine
    direction: str = "" # LONG/SHORT
    execution_type: str = "MARKET" # LIMIT or MARKET
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    
    # [NERVE REPAIR v4.2] Dynamic Trailing
    highest_mark: float = 0.0 # High water mark for LONG / Low for SHORT
    last_atr: float = 0.0
    
    rejection_reason: Optional[str] = None

    def __post_init__(self):
        import asyncio
        self._lock = asyncio.Lock()
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
