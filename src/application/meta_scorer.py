import asyncio
import logging
from typing import Dict, Deque
from collections import deque
from dataclasses import dataclass

logger = logging.getLogger("GEKTOR.MetaScoring")

@dataclass(slots=True)
class EvaluatedSignal:
    signal_id: str
    symbol: str
    direction: int  # 1 (Long), -1 (Short)
    entry_price: float
    timestamp: float
    take_profit: float
    stop_loss: float
    time_limit_ts: float
    status: str = "UNRESOLVED" # PROFIT, LOSS, EXPIRED

class MetaScorer:
    """[GEKTOR v2.0] Динамический трекинг Alpha Decay (Information Coefficient)."""
    __slots__ = ['window_size', 'min_win_rate', '_signal_history', '_wins', '_losses']

    def __init__(self, window_size: int = 100, min_win_rate: float = 0.55):
        self.window_size = window_size
        self.min_win_rate = min_win_rate
        # O(1) кольцевая структура для булевых исходов (True=Win, False=Loss)
        self._signal_history: Deque[bool] = deque(maxlen=window_size)
        self._wins: int = 0
        self._losses: int = 0

    def approve_signal(self, primary_signal_valid: bool) -> bool:
        """Решает, пропускать ли сигнал в Outbox на исполнение Оператору."""
        if not primary_signal_valid:
            return False
            
        total_resolved = self._wins + self._losses
        if total_resolved < 20: # Период прогрева (Hydration/Burn-in)
            return True
            
        current_win_rate = self._wins / total_resolved
        
        if current_win_rate < self.min_win_rate:
            logger.warning(f"🚷 [META-LABEL] Alpha Decay detected. Win Rate: {current_win_rate:.2f}. Signal Muted.")
            return False
            
        return True

    def resolve_signal(self, is_profit: bool) -> None:
        """Вызывается синхронно (во время бэктеста) или асинхронно, когда Трекинг-модуль фиксирует TP/SL."""
        if len(self._signal_history) == self.window_size:
            oldest = self._signal_history.popleft()
            if oldest:
                self._wins -= 1
            else:
                self._losses -= 1
                
        self._signal_history.append(is_profit)
        if is_profit:
            self._wins += 1
        else:
            self._losses += 1
        
        ic_score = (self._wins / (self._wins + self._losses)) * 2 - 1
        logger.info(f"📊 [META-SCORE] IC: {ic_score:.3f} | WR: {self._wins/(self._wins+self._losses):.2f}")

class SignalResolver:
    """O(1) Обработчик Тройного Барьера. Подчиняется строгой стреле времени."""
    __slots__ = ['scorer', '_active_signals']

    def __init__(self, scorer: MetaScorer):
        self.scorer = scorer
        self._active_signals: Dict[str, EvaluatedSignal] = {}

    def register_signal(self, sig: EvaluatedSignal):
        self._active_signals[sig.signal_id] = sig

    def process_tick(self, symbol: str, price: float, ts: float):
        """
        [EVENT SOURCING CAUSALITY]
        Синхронная проверка исходов на каждом новом тике.
        Гарантирует отсутствие Look-Ahead Bias при Hydration, так как тики поступают хронологически.
        """
        resolved_ids = []
        
        for sid, sig in self._active_signals.items():
            if sig.symbol != symbol:
                continue
                
            # Triple Barrier Check (O(1) per active signal)
            is_resolved = False
            is_profit = False
            
            # 1. Price Barrier (Take Profit / Stop Loss)
            if sig.direction == 1: # LONG
                if price >= sig.take_profit:
                    is_resolved, is_profit = True, True
                elif price <= sig.stop_loss:
                    is_resolved, is_profit = True, False
            else: # SHORT
                if price <= sig.take_profit:
                    is_resolved, is_profit = True, True
                elif price >= sig.stop_loss:
                    is_resolved, is_profit = True, False
            
            # 2. Time Barrier
            if not is_resolved and ts >= sig.time_limit_ts:
                is_resolved = True
                # Если время вышло, смотрим текущий PnL
                if sig.direction == 1:
                    is_profit = price > sig.entry_price
                else:
                    is_profit = price < sig.entry_price

            if is_resolved:
                # 3. Resolve to MetaScorer
                self.scorer.resolve_signal(is_profit)
                sig.status = "PROFIT" if is_profit else "LOSS"
                resolved_ids.append(sid)
                
        # O(m) Удаление резолвнутых сигналов
        for sid in resolved_ids:
            self._active_signals.pop(sid)
