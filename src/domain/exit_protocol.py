# src/domain/exit_protocol.py
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Protocol
from loguru import logger

class SignalState(Enum):
    ACTIVE = auto()
    INVALIDATED_TOXIC_FLOW = auto()
    INVALIDATED_VOLUME_SPIKE = auto()
    INVALIDATED_TIME_STOP = auto()
    CLOSED_MANUALLY = auto()

@dataclass(slots=True, frozen=True)
class MarketTick:
    symbol: str
    price: float
    volume: float
    side: str  # 'Buy' or 'Sell'
    exchange_ts: int  # ms
    conflated: bool = False

@dataclass(slots=True, frozen=True)
class MarketSnapshot:
    bid_price: float
    ask_price: float
    mid_price: float
    timestamp: float

@dataclass(slots=True)
class ActiveSignal:
    signal_id: str
    symbol: str
    entry_ts: int
    entry_price: float # Keep for legacy, but we use bid/ask for real analysis
    entry_bid: float
    entry_ask: float
    direction: int  # 1 for Long, -1 for Short
    
    # [LATENCY GUARD] Snapshot at t+1200ms to verify "Human Alpha"
    human_entry_bid: float = 0.0
    human_entry_ask: float = 0.0
    
    state: SignalState = SignalState.ACTIVE
    bars_observed: int = 0
    max_vpin: float = 0.0

class ExecutionSimulator:
    """
    [ФАЗА 6: АНТИ-ИЛЛЮЗИЯ PnL]
    Вычисление реального маркаута с учетом спреда и издержек.
    """
    def __init__(self, taker_fee_bps: float = 4.0):
        self.taker_fee_bps = taker_fee_bps

    def calculate_real_markout_bps(self, 
                                   direction: int, 
                                   entry_bid: float,
                                   entry_ask: float,
                                   exit_bid: float,
                                   exit_ask: float) -> float:
        """
        LONG: Enter @ Ask, Exit @ Bid.
        SHORT: Enter @ Bid, Exit @ Ask.
        """
        if direction > 0: # BUY
            # Real execution crosses the spread
            entry_price = entry_ask
            exit_price = exit_bid
        else: # SELL
            entry_price = entry_bid
            exit_price = exit_ask

        if entry_price == 0: return 0.0
        
        gross_pnl_bps = ((exit_price - entry_price) / entry_price) * 10000 * (1 if direction > 0 else -1)
        # Round-trip fee
        net_pnl_bps = gross_pnl_bps - (self.taker_fee_bps * 2)
        return net_pnl_bps

class InvalidationRule(Protocol):
    def check(self, signal: ActiveSignal, tick: MarketTick, current_vpin: float) -> Optional[SignalState]:
        """Returns SignalState if invalidated, else None."""
        ...

class TimeStopRule:
    def __init__(self, max_holding_bars: int):
        self.max_holding_bars = max_holding_bars

    def check(self, signal: ActiveSignal, tick: MarketTick, current_vpin: float) -> Optional[SignalState]:
        # В GEKTOR время измеряется барами объема. 
        # Если сигнал 'прокис' во времени (через bars_observed в MathCore), это ловит MathCore.
        # Здесь мы можем добавить жесткую отсечку по Exchange Time если нужно.
        return None

class VPINDecayRule:
    def __init__(self, decay_factor: float):
        self.decay_factor = decay_factor

    def check(self, signal: ActiveSignal, tick: MarketTick, current_vpin: float) -> Optional[SignalState]:
        if signal.max_vpin > 0:
            threshold = signal.max_vpin * self.decay_factor
            if current_vpin < threshold and signal.bars_observed > 3:
                return SignalState.INVALIDATED_TOXIC_FLOW
        return None

class MicrostructureSpikeRule:
    def __init__(self, critical_vol_mult: float = 5.0):
        self.critical_vol_mult = critical_vol_mult

    def check(self, signal: ActiveSignal, tick: MarketTick, current_vpin: float) -> Optional[SignalState]:
        # Логика детекции аномального удара по рынку против позиции
        # В реальном MathCore это требует истории волатильности объема.
        return None
