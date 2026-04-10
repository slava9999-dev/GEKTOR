# skills/gerald_sniper/core/realtime/conflator.py
import asyncio
from typing import Dict
from .telemetry import TelemetryManager

class MetricConflator:
    """
    [GEKTOR v21.50] Zero-Friction Aggregator (ASP Sentinel).
    Исключает 'Эффект Наблюдателя', агрегируя метрики горячего пути (15k+ тиков/сек)
    и сбрасывая их в Prometheus с частотой 1Гц.
    """
    def __init__(self, flush_interval: float = 1.0):
        self.vpin_cache: Dict[str, float] = {}
        self.flush_interval = flush_interval
        self._running = False

    def push_vpin(self, symbol: str, value: float):
        """O(1) Операция. Практически нулевой оверхед на горячем пути."""
        self.vpin_cache[symbol] = value

    async def flush_loop(self):
        """Фоновый воркер для выгрузки метрик в мониторинг."""
        self._running = True
        while self._running:
            await asyncio.sleep(self.flush_interval)
            # Копируем ключи для предотвращения RuntimeError при изменении словаря во время итерации
            symbols = list(self.vpin_cache.keys())
            for symbol in symbols:
                val = self.vpin_cache.get(symbol)
                if val is not None:
                    TelemetryManager.update_vpin_metric(symbol, val)
