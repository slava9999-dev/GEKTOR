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

from utils.math_utils import generate_sortable_id

class SignalEvent(BaseEvent):
    """
    [GEKTOR v10.3] Advisory Signal Event with Time-Sorted IDs.
    """
    signal_id: str = Field(default_factory=generate_sortable_id)
    symbol: str = ""
    direction: str = "" # LONG, SHORT
    confidence: float = 0.0
    price: float = 0.0
    factors: List[str] = Field(default_factory=list)
    market_regime: str = "FLAT"
    liquidity_tier: str = "C"
    created_at: float = Field(default_factory=time.time)
    max_age_sec: int = 40
    rvol: float = 1.0 # Added for dynamic TTL
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # ArjanCodes: Строгий контракт сигнала с отслеживанием времени жизни (TTL)
    exchange_timestamp_ms: float = Field(default=0.0, description="Время зарождения аномалии на ядре биржи")
    local_generation_ms: float = Field(default_factory=lambda: time.time() * 1000)
    
    @property
    def signal_age_ms(self) -> float:
        """ Клеппман: Истинный возраст сигнала с учетом всех сетевых лагов """
        if self.exchange_timestamp_ms == 0.0:
            return 0.0
        return (time.time() * 1000) - self.exchange_timestamp_ms

class SignalLifecycleEvent(BaseEvent):
    """
    [GEKTOR v10.2] Event for synchronizing Signal State with UI (Telegram).
    """
    signal_id: str
    action: str # SENT, EXPIRED, CANCELLED
    chat_id: Optional[int] = None
    message_id: Optional[int] = None

# ─────────────────────────────────────────────────────────────────────────────
# [GEKTOR v2.0] DEPRECATED EXECUTION EVENTS (DECOMMISSIONED)
# The following events are remnants of the legacy Automated Combat Core.
# ─────────────────────────────────────────────────────────────────────────────

class ManualExecutionEvent(BaseEvent):
    """[DEPRECATED] Event fired from Telegram UI - interaction is now prohibited."""
    signal_id: str = ""
    action: str = "" # EXECUTE, REJECT
    user_id: str = ""
    timestamp: float = 0.0

class ExecutionEvent(BaseEvent):
    """[DEPRECATED] Event for legacy Execution Engine actions."""
    symbol: str = ""
    action: str = "" # OPEN, CLOSE, TP, SL
    side: str = ""
    status: str = "PROHIBITED" 

class OrderUpdateEvent(BaseEvent):
    """[DEPRECATED] Update from Private WebSocket - execution monitoring is decommissioned."""
    order_id: str
    symbol: str
    status: str 

class OrderExecutedEvent(BaseEvent):
    """[DEPRECATED] Fill event - trading is prohibited."""
    symbol: str
    order_id: str
    side: str 

class LimboResolutionEvent(BaseEvent):
    """[DEPRECATED] Reconcilation event for decommissioned OMS."""
    symbol: str
    cl_ord_id: str
    order_id: str

class PositionUpdateEvent(BaseEvent):
    """Real-time exposure update (Delta Tracking)."""
    symbol: str
    side: str # Long, Short, None
    size: float
    entry_price: float
    unrealized_pnl: float = 0.0

from dataclasses import dataclass, field

@dataclass(slots=True, frozen=True)
class RawWSEvent:
    """
    [GEKTOR v14.3] High-Performance Immutable DTO with __slots__.
    Eliminates Data Races and reduces GC overhead during bursts.
    """
    topic: str
    symbol: str
    data: Any # Raw data payload for general topics
    seq: int = 0
    u: Optional[int] = None
    exchange_ts: int = 0
    is_snapshot: bool = False
    # Atomic Orderbook Fields (Rule 18.9)
    bids: tuple[tuple[float, float], ...] = field(default_factory=tuple)
    asks: tuple[tuple[float, float], ...] = field(default_factory=tuple)
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def from_bybit_payload(cls, payload: dict) -> 'RawWSEvent':
        """ Factory to create immutable snapshot from mutable dict """
        topic = payload.get("topic", "")
        data = payload.get("data", {})
        msg_type = payload.get("type", "delta")
        is_snap = msg_type == "snapshot"
        
        # Orderbook Path: Convert lists to tuples to ensure immutability
        bids = tuple()
        asks = tuple()
        symbol = ""
        
        if topic:
            symbol = topic.split('.')[-1]
            if topic.startswith("orderbook"):
                if isinstance(data, dict):
                    # Convert to floats and wrap in immutable tuples
                    bids = tuple((float(b[0]), float(b[1])) for b in data.get("b", []))
                    asks = tuple((float(a[0]), float(a[1])) for a in data.get("a", []))
        
        return cls(
            topic=topic,
            symbol=symbol,
            data=data,
            seq=payload.get("seq", 0),
            u=data.get("u") if isinstance(data, dict) else None,
            exchange_ts=payload.get("ts", 0),
            is_snapshot=is_snap,
            bids=bids,
            asks=asks
        )

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

class SystemBlindnessEvent(BaseEvent):
    """
    [GEKTOR v10.0] Critical "Red Alert" Event.
    Fired when a Silent Zombie connection is detected (Exchange stops sending data, but TCP remains open).
    Invalidates Radar accuracy and alerts the operator.
    """
    source: str         # "WSWorker_0", "Shard_1", etc.
    symbol: str         # Symbol that went blind
    reason: str         # "Stale-Skew", "Zero-Derivative", "OS-Socket-Freeze"
    stale_ms: int       # Age of last valid message
    is_active: bool = True

class UniverseChangeEvent(BaseEvent):
    """
    [GEKTOR v11.9] Universe Rebalancing Event.
    Fired when the set of active symbols changes due to liquidity/volatility shifts.
    """
    new_universe: List[str]
    added: List[str]
    removed: List[str]
    reason: str = "SCHEDULED_REBALANCE"

class SystemAmnesiaEvent(BaseEvent):
    """
    [GEKTOR v14.7.3] Protocol for flushing all low-level state machines.
    Fired when drift is detected or manual reset is required.
    """
    reason: str = "MANUAL"

class SystemEpochChangeEvent(BaseEvent):
    """
    [GEKTOR v14.8.0] Fencing Token Event.
    Invalidates all calculations and signals from previous epochs.
    """
    epoch: int

class MarketHaltEvent(BaseEvent):
    """
    [GEKTOR v21.2] Trading Halt Triggered (Circuit Breaker or Maintenance).
    Triggers 'Lockdown' mode: mute signals, enter quarantine.
    """
    symbol: str
    gap_duration_sec: float
    reason: str = "EXCHANGE_HALT"

class MarketResumptionEvent(BaseEvent):
    """
    [GEKTOR v21.2] Trading Resumed after a Halt.
    Triggers 'Quarantine' mode: wait for buffer warmup before signals.
    """
    symbol: str
    quarantine_bars_required: int = 5
