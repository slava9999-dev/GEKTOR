import math
import logging
from typing import Optional, List
from dataclasses import dataclass

from src.domain.dollar_bar import DollarBar

logger = logging.getLogger("GEKTOR.QuantEngine")

@dataclass(slots=True)
class VPINSignal:
    vpin_value: float
    z_score: float
    is_anomaly: bool
    absorption_detected: bool  # Structural Filter: Iceberg Protection

class O1VPINEngine:
    """
    [GEKTOR v2.0] O(1) Кольцевой буфер для расчета VPIN и динамического Z-Score.
    Встроенный фильтр поглощения: Trade Imbalance vs Price Impact.
    """
    __slots__ = [
        'window_size', 'volume_threshold', '_imbalances', '_index', '_is_filled',
        '_running_imbalance_sum', '_vpin_history', '_vpin_sum', '_vpin_sq_sum', 
        '_anomaly_threshold_z', '_price_history'
    ]

    def __init__(self, window_size: int = 50, volume_threshold: float = 1_000_000.0, z_threshold: float = 2.5):
        self.window_size = window_size
        self.volume_threshold = volume_threshold
        self._anomaly_threshold_z = z_threshold
        
        # Предаллоцированная память (Ring Buffer)
        self._imbalances: List[float] = [0.0] * window_size
        self._index: int = 0
        self._is_filled: bool = False
        self._running_imbalance_sum: float = 0.0

        # Стейт для скользящего Z-Score (инкрементальная дисперсия)
        self._vpin_history: List[float] = [0.0] * window_size
        self._vpin_sum: float = 0.0
        self._vpin_sq_sum: float = 0.0
        
        # [PRICE IMPACT FILTER] Ring buffer for price series
        self._price_history: List[float] = [0.0] * window_size

    def process_bar(self, bar: DollarBar) -> Optional[VPINSignal]:
        buy_vol = bar.buy_volume_quote
        sell_vol = bar.sell_volume_quote
        price = bar.close_price
        
        imbalance = buy_vol - sell_vol
        abs_imbalance = abs(imbalance)

        current_idx = self._index
        
        # O(1) Обновление кольца цен
        old_price = self._price_history[current_idx]
        self._price_history[current_idx] = price

        # O(1) Инкрементальное обновление суммы дисбаланса
        old_abs_imbalance = self._imbalances[current_idx]
        self._running_imbalance_sum += (abs_imbalance - old_abs_imbalance)
        self._imbalances[current_idx] = abs_imbalance

        # Сдвиг каретки кольцевого буфера
        self._index += 1
        if self._index >= self.window_size:
            self._index = 0
            self._is_filled = True

        if not self._is_filled:
            return None

        # Расчет текущего VPIN
        total_volume = self.volume_threshold * self.window_size
        current_vpin = self._running_imbalance_sum / total_volume

        # O(1) Обновление статистики для Z-Score
        old_vpin = self._vpin_history[current_idx]
        self._vpin_sum += (current_vpin - old_vpin)
        self._vpin_sq_sum += (current_vpin**2 - old_vpin**2)
        self._vpin_history[current_idx] = current_vpin

        # Математика Z-Score (Welford-подобная аппроксимация окна)
        mean_vpin = self._vpin_sum / self.window_size
        variance = (self._vpin_sq_sum / self.window_size) - (mean_vpin**2)
        std_dev = math.sqrt(variance) if variance > 1e-9 else 1e-9
        
        z_score = (current_vpin - mean_vpin) / std_dev
        is_anomaly = z_score > self._anomaly_threshold_z

        # [STRUCTURAL FILTER - ICEBERG VERIFICATION]
        # Проверяем, сдвинулась ли цена в сторону дисбаланса.
        # Цена начала окна: self._price_history[self._index] (т.к. self._index уже сдвинулся на самый старый бар)
        price_start_window = self._price_history[self._index]
        price_return = price - price_start_window
        
        # Если дисбаланс лонговый (buy_vol > sell_vol), но price_return <= 0 -> Absorption (Iceberg)
        # Если дисбаланс шортовый (buy_vol < sell_vol), но price_return >= 0 -> Absorption (Iceberg)
        absorption_detected = False
        if is_anomaly:
            if imbalance > 0 and price_return <= 0:
                absorption_detected = True
            elif imbalance < 0 and price_return >= 0:
                absorption_detected = True

        return VPINSignal(
            vpin_value=current_vpin,
            z_score=z_score,
            is_anomaly=is_anomaly,
            absorption_detected=absorption_detected
        )
