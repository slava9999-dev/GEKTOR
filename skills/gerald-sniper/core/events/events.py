# core/events/events.py

from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import time

class BaseEvent(BaseModel):
    timestamp: float = Field(default_factory=time.time)
    liquidity_tier: str = "C" # Default Tier

class DetectorEvent(BaseEvent):
    """Event fired when a real-time detector triggers (Rule 5)"""
    symbol: str = ""
    detector: str = "" # VELOCITY_SHOCK, ACCELERATION, etc.
    direction: str = "" # LONG, SHORT
    price: float = 0.0
    payload: Dict[str, Any] = Field(default_factory=dict)

class PriceUpdateEvent(BaseEvent):
    """Event fired when new prices arrive (Rule 2.3)"""
    prices: Dict[str, float] = Field(default_factory=dict)

class SignalEvent(BaseEvent):
    """Event fired when a new signal is processed by Tactical Orchestrator"""
    signal_id: str = "" # UUID for tracking across TG buttons
    symbol: str = ""
    direction: str = "" # LONG, SHORT
    confidence: float = 0.0
    price: float = 0.0
    factors: List[str] = Field(default_factory=list)
    market_regime: str = "FLAT"
    liquidity_tier: str = "C"
    metadata: Dict[str, Any] = Field(default_factory=dict)

class ManualExecutionEvent(BaseEvent):
    """Event fired from Telegram UI for human-in-the-loop actions"""
    signal_id: str = ""
    action: str = "" # EXECUTE, REJECT
    user_id: str = ""
    timestamp: float = 0.0
    # [GEKTOR v8.1] Macro Radar one-click context
    symbol: Optional[str] = None
    price: Optional[float] = None
    direction: Optional[str] = None  # LONG, SHORT
    source: Optional[str] = None     # "MACRO_RADAR", "SIGNAL_ENGINE"

class ExecutionEvent(BaseEvent):
    """Event fired when Execution Engine takes action"""
    symbol: str = ""
    action: str = "" # OPEN, CLOSE, TP, SL
    side: str = ""
    direction: Optional[str] = None # Backward compatibility/Hotfix
    price: float = 0.0
    status: str = "SUCCESS" # SUCCESS, FAILED
    error: Optional[str] = None

class OrderUpdateEvent(BaseEvent):
    """Real-time update from Private WebSocket (execution/order topics)"""
    order_id: str
    symbol: str
    status: str # Filled, PartiallyFilled, Cancelled, Rejected
    exec_qty: float = 0.0
    avg_price: float = 0.0
    remaining_qty: float = 0.0

class OrderExecutedEvent(BaseEvent):
    """[P0] High-fidelity Fill Event for WAL & TimescaleDB."""
    symbol: str
    order_id: str
    side: str # Buy, Sell
    price: float
    qty: float
    fee: float = 0.0
    realized_pnl: float = 0.0
    is_maker: bool = False

class PositionUpdateEvent(BaseEvent):
    """Real-time exposure update (Delta Tracking)."""
    symbol: str
    side: str # Long, Short, None
    size: float
    entry_price: float
    unrealized_pnl: float = 0.0

class RawWSEvent(BaseEvent):
    """Raw event from Bybit/Binance WebSocket streams (Rule 16.44)."""
    topic: str = ""
    data: Any = None
    u: Optional[int] = None
    seq: int = 0 # Bybit V5 Cross-Sequence
    type: str = "delta" # Bybit V5: snapshot or delta
    
    @property
    def is_snapshot(self) -> bool:
        return self.type == "snapshot"

class SystemAlertEvent(BaseEvent):
    """Event for high-priority system alerts (Rule 16.10)."""
    level: str  # INFO, WARNING, CRITICAL
    message: str

class AlertEvent(BaseEvent):
    """General notification event for Telegram."""
    title: str
    message: str
    priority: str = "normal" # critical, normal, low

class EmergencyAlertEvent(BaseEvent):
    """Fired by Risk Management for fast-exit or lock events."""
    message: str
    severity: str = "P1" # P0, P1, P2
    symbol: Optional[str] = None

class L2MetricsEvent(BaseEvent):
    """Event containing computed L2 metrics from a Shard Worker (Rule 16.48)."""
    symbol: str
    imbalance: float
    bid_vol_usd: float
    ask_vol_usd: float
    bid_price: float
    ask_price: float

class ConnectionRestoredEvent(BaseEvent):
    """
    Fired when WS/ZMQ connection is restored after disconnection.
    Triggers immediate StateReconciler._hard_sync to close the blind window.
    """
    source: str             # "BybitWS", "WSWorker_0", "ZMQ_Bridge"
    downtime_seconds: float # Measured gap duration
    reconnect_count: int = 0
