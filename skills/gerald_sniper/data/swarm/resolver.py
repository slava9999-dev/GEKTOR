import numpy as np
from typing import Dict, List

class DataConflictResolver:
    """Разрешает конфликты между источниками данных с помощью взвешенного голосования и выявления аномалий"""
    
    def __init__(self):
        self.source_weights = {
            'bybit': 0.45,
            'binance': 0.45,
            'okx': 0.10
        }
        
    def resolve_price_conflict(self, prices: Dict[str, float]) -> float:
        """Использует взвешенное голосование + выявление аномалий"""
        if not prices:
            return 0.0
            
        weighted_sum = 0
        total_weight = 0
        
        for source, price in prices.items():
            weight = self.source_weights.get(source, 0.1)
            
            if self.is_anomaly(price, prices):
                weight *= 0.1  # Снижаем вес аномального источника (защита от "шпилек" на одной бирже)
            
            weighted_sum += price * weight
            total_weight += weight
            
        if total_weight == 0:
            return sum(prices.values()) / len(prices)
            
        return weighted_sum / total_weight
        
    def is_anomaly(self, price: float, prices: Dict[str, float]) -> bool:
        if len(prices) < 3:
            return False  # Нужен хотя бы минимальный консенсус (3 источника) для выявления аномалии
            
        vals = list(prices.values())
        mean = np.mean(vals)
        std_dev = np.std(vals)
        if std_dev == 0:
            return False
            
        # Считаем аномалией отклонение больше чем на 2 сигмы, либо абсолютную 'шпильку' > 0.5% от mean
        z_score = abs(price - mean) / std_dev
        return z_score > 2.0 or (abs(price - mean) / mean) > 0.005

    def detect_manipulation(self, prices: Dict[str, float]) -> bool:
        """Выявляет ценовые манипуляции на основе расхождений между биржами"""
        if len(prices) < 2:
            return False
        vals = list(prices.values())
        std_dev = np.std(vals)
        mean = np.mean(vals)
        # Если расхождение между Tier-1 биржами > 0.3% → идет атака на конкретный стакан одной площадки
        return (std_dev / mean) > 0.003

conflict_resolver = DataConflictResolver()
