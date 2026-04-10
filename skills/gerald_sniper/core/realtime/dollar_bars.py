import asyncio
import time
from typing import Optional, Deque, List
from collections import deque
from pydantic import BaseModel, Field
from loguru import logger
from core.events.events import MarketHaltEvent, MarketResumptionEvent
from core.realtime.temporal_integrity import clock_sync

# Контракт сформированного бара
class DollarBar(BaseModel):
    symbol: str
    timestamp_start: int
    timestamp_end: int
    open: float
    high: float
    low: float
    close: float
    volume_base: float = 0.0
    volume_usd: float = 0.0
    ticks_count: int = 0
    ofi_base: float = 0.0 # [GEKTOR v14.9] Microstructural Imbalance (Accumulated)
    threshold_at_creation: float = 0.0 # Для отладки динамики
    is_stable: bool = True # False во время карантина (GEKTOR v21.2)
    vwas_ms: float = 0.0 # [GEKTOR v21.6] Network-Weighted Average Staleness (Data Age)
    core_entry_perf: float = 0.0 # [GEKTOR v21.7] Internal Core Entry Monotonic (Lag detection)

class AdaptiveThresholdPilot:
    """
    [GEKTOR v12.4] Электронный пилот порога.
    Решает Парадокс Сжатия Времени. Поддерживает "Информационную Плотность" баров,
    адаптируясь к режимам волатильности и ликвидности.
    """
    def __init__(self, initial_threshold: float, window_size: int = 24):
        self.base_threshold = initial_threshold
        self.current_threshold = initial_threshold
        
        # Параметры адаптации
        self.alpha = 0.1 # EWMA smoothing
        self.regime_multiplier = 1.0 # Динамический множитель (x10 во время шока)
        
        # Состояние режима (NORMAL, SHOCK, CALM)
        self._current_regime = "NORMAL"

    def set_regime(self, regime: str):
        """
        [Architecture] Внешний сигнал от MacroRadar (Shock Detector).
        Мгновенно масштабирует порог, не дожидаясь EWMA-реакции.
        """
        self._current_regime = regime
        if regime == "SHOCK":
            self.regime_multiplier = 10.0 # Взрывной рост ликвидности -> x10 порог
            logger.warning(f"🚀 [Regime] SHOCK DETECTED. Порог масштабирован x10 для сохранения макро-фокуса.")
        elif regime == "CALM":
            self.regime_multiplier = 0.5  # Мертвый рынок -> снижаем порог, чтобы не ждать баров вечно
        else:
            self.regime_multiplier = 1.0

    def update_and_get_threshold(self, last_bar_vol_usd: float) -> float:
        """
        EWMA-адаптация + Режимный множитель.
        """
        # Плавно подстраиваем внутреннюю базу под текущую ликвидность
        self.current_threshold = (self.alpha * last_bar_vol_usd) + (1 - self.alpha) * self.current_threshold
        
        # Накладываем режимный множитель и защищаем от падения ниже абсолютного минимума
        active_threshold = max(self.base_threshold, self.current_threshold) * self.regime_multiplier
        
        return active_threshold

class DollarBarEngine:
    def __init__(self, symbol: str, initial_dollar_threshold: float = 1_000_000.0):
        self.symbol = symbol
        self.pilot = AdaptiveThresholdPilot(initial_dollar_threshold)
        
        # Кольцевой буфер для хранения готовых баров (защита от OOM)
        self.bars_history: Deque[DollarBar] = deque(maxlen=1000)
        
        # Внутренний стейт собираемого бара
        self._reset_current_bar()
        self._last_tick_ts: float = 0.0
        self._halt_gap_threshold: float = 60.0 # Секунд (настраиваемо)
        self._is_quarantined: bool = False
        self._quarantine_count: int = 0
        self._lock = asyncio.Lock()

    def _reset_current_bar(self):
        self._current_open: Optional[float] = None
        self._current_high: float = float('-inf')
        self._current_low: float = float('inf')
        self._current_close: Optional[float] = None
        self._current_vol_base: float = 0.0
        self._current_vol_usd: float = 0.0
        self._current_ticks: int = 0
        self._current_ofi: float = 0.0
        self._current_vwas_accum: float = 0.0 # [GEKTOR v21.6]
        self._core_entry_perf: float = 0.0 # [GEKTOR v21.7]
        self._ts_start: Optional[int] = None

    async def ingest_tick(self, price: float, size: float, timestamp: int, 
                          side: str, alert_queue: asyncio.Queue, ofi_delta: float = 0.0, 
                          staleness_ms: float = 0.0):
        """
        Асинхронный O(1) ingestion сырых сделок. 
        Применяет Адаптивный Порог для борьбы с Time Compression Paradox.
        """
        tick_usd_value = price * size
        current_ts = timestamp / 1000.0 # Unix TS in seconds

        async with self._lock:
            # ── [GAP DETECTION GEKTOR v21.2] ──
            if self._last_tick_ts > 0:
                gap = current_ts - self._last_tick_ts
                if gap > self._halt_gap_threshold:
                    logger.warning(f"🕳️ [GAP] Detected {gap:.1f}s silence for {self.symbol}. Triggering Halt Protocol.")
                    await bus.publish(MarketHaltEvent(symbol=self.symbol, gap_duration_sec=gap))
                    
                    # Entering Quarantine
                    self._is_quarantined = True
                    self._quarantine_count = 0 
                    await bus.publish(MarketResumptionEvent(symbol=self.symbol, quarantine_bars_required=5))

            self._last_tick_ts = current_ts
            
            # Изучаем текущий динамический порог
            active_threshold = self.pilot.active_threshold

            # Инициализация нового бара
            if self._current_open is None:
                self._current_open = price
                self._ts_start = timestamp

            # Обновление OHLC
            if price > self._current_high: self._current_high = price
            if price < self._current_low: self._current_low = price
            
            self._current_close = price
            self._current_vol_base += size
            self._current_vol_usd += tick_usd_value
            self._current_ticks += 1
            self._current_ofi += ofi_delta
            
            # [GEKTOR v21.7] Enhanced VWAS: Corrected for Clock Drift
            corrected_staleness = clock_sync.get_corrected_staleness(timestamp)
            self._current_vwas_accum += corrected_staleness * tick_usd_value
            
            if self._current_ticks == 1:
                # Mark first tick arrival for internal lag benchmark
                self._core_entry_perf = time.perf_counter()

            # Проверка порога (Dynamic Threshold Breach)
            if self._current_vol_usd >= active_threshold:
                # ── [QUARANTINE PROGRESSION GEKTOR v21.2] ──
                if self._is_quarantined:
                    self._quarantine_count += 1
                    if self._quarantine_count >= 5: # Параметр карантина
                        self._is_quarantined = False
                        logger.success(f"🩹 [RECOVERY] {self.symbol} quarantine lifted. Signals ACTIVE.")

                completed_bar = DollarBar(
                    symbol=self.symbol,
                    timestamp_start=self._ts_start,
                    timestamp_end=timestamp,
                    open=self._current_open,
                    high=self._current_high,
                    low=self._current_low,
                    close=self._current_close,
                    volume_base=self._current_vol_base,
                    volume_usd=self._current_vol_usd,
                    ticks_count=self._current_ticks,
                    ofi_base=self._current_ofi,
                    threshold_at_creation=active_threshold,
                    is_stable=not self._is_quarantined,
                    vwas_ms=self._current_vwas_accum / (self._current_vol_usd + 1e-9),
                    core_entry_perf=self._core_entry_perf
                )
                
                # 1. Сохраняем в историю
                self.bars_history.append(completed_bar)
                
                # 2. Отправляем в аналитическую шину
                try:
                    alert_queue.put_nowait(completed_bar)
                    # logger.info(f"🆕 [BAR] {self.symbol} generated (Stable={completed_bar.is_stable})")
                except asyncio.QueueFull:
                    logger.error(f"🚨 [OVERLOAD] Очередь аналитики переполнена! Дропаем бар {self.symbol}")

                # Сброс стейта
                self._reset_current_bar()
