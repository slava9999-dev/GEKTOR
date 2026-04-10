import asyncio
import aiohttp
import time
import os
from typing import Optional
from loguru import logger

class ClockSynchronizer:
    """[GEKTOR v21.58] Resilient Clock Sync with Drift Protection."""
    def __init__(self, endpoint: str = "https://api.bybit.com/v5/market/time"):
        self.endpoint = endpoint
        self.time_offset_ms: float = 0.0
        self._last_calibration = 0.0
        self.is_calibrating = False

    async def calibrate(self, proxy: Optional[str] = None) -> tuple[float, float]:
        """Perform time synchronization with latency gate verification."""
        if self.is_calibrating: return 0.0, 0.0
        self.is_calibrating = True
        
        try:
            async with aiohttp.ClientSession() as session:
                start_ping = time.perf_counter()
                async with session.get(self.endpoint, timeout=3.0, proxy=proxy) as response:
                    data = await response.json()
                    exchange_time_ms = int(data['time'])
                
                end_ping = time.perf_counter()
                rtt_ms = (end_ping - start_ping) * 1000 
                estimated_exchange_time = exchange_time_ms + (rtt_ms / 2.0)
                local_time_ms = time.time() * 1000
                
                self.time_offset_ms = estimated_exchange_time - local_time_ms
                self._last_calibration = time.time()
                
                # Validation Logic
                if not LatencyGate.validate(rtt_ms, self.time_offset_ms):
                    raise RuntimeError(f"ENVIRONMENT_TOXIC: RTT {rtt_ms:.1f}ms exceeds safety limits.")
                
                return rtt_ms, self.time_offset_ms
        finally:
            self.is_calibrating = False

    def get_synchronized_time(self) -> float:
        """Возвращает локальное время, скорректированное под часы биржи."""
        return (time.time() * 1000) + self.time_offset_ms

class StateInvalidationError(Exception):
    """Выбрасывается при фатальной потере консистентности потока данных."""
    pass

class LatencyGate:
    """[GEKTOR v21.64] Adaptive Environment Integrity Guard."""
    # Profiles: HFT (Singapore/Tokyo) vs SWING (Global/SNG)
    PROFILE = os.getenv("EXECUTION_PROFILE", "SWING").upper()
    
    MAX_ALLOWED_RTT = 200.0 if PROFILE == "HFT" else 1000.0
    STRICT_RECOVERY_RTT = 150.0 if PROFILE == "HFT" else 800.0
    MAX_ALLOWED_OFFSET = 500.0 if PROFILE == "HFT" else 2000.0
    STALE_LIMIT = 500.0 if PROFILE == "HFT" else 5000.0

    @classmethod
    def validate(cls, rtt: float, offset: float, strict: bool = False) -> bool:
        threshold = cls.STRICT_RECOVERY_RTT if strict else cls.MAX_ALLOWED_RTT
        if rtt > threshold:
            logger.critical(f"🚨 [GateKeeper] RTT TOO HIGH: {rtt:.1f}ms (Limit <{threshold}ms). Profile: {cls.PROFILE}")
            return False
        if abs(offset) > cls.MAX_ALLOWED_OFFSET:
            logger.critical(f"🚨 [GateKeeper] CLOCK DRIFT UNACCEPTABLE: {offset:.1f}ms.")
            return False
        return True

class MarketQuarantineGuard:
    """
    [GEKTOR v21.49] Контроллер математического карантина.
    Замораживает ядро при деградации сети и требует 'прожима' (warmup) 
    для восстановления доверия к VPIN/CUSUM.
    """
    def __init__(self, clock_sync, warmup_required: int = 20, max_latency: int = 3000):
        self.sync = clock_sync
        self.warmup_required = warmup_required
        self.max_latency = max_latency
        self._is_quarantined = True  # Start in quarantine until first calibration
        self._ticks_since_recovery = 0
        self.current_rtt = 200.0 # Default fallback
        
        # [GEKTOR v21.55] Anti-Flapping Shield
        self.stability_window = 5.0  # seconds
        self.last_gap_time = 0.0
        self.consecutive_gaps = 0
        self.amnesty_pending_until = 0.0
        self.gap_count_reset_at = 0.0
        self.last_oscillation_time = 0.0
        self.DEBOUNCE_WINDOW = 5.0 # [GEKTOR v21.64]
        
        # [GEKTOR v21.64] Sync with Execution Profile
        self.LAG_THRESHOLD = 500.0 if LatencyGate.PROFILE == "HFT" else 3500.0
        self.is_draining = False # Set by GapGuard
        
        # [GEKTOR v21.57] Burst Monitoring
        self.last_arrival_ms = 0
        self.burst_count = 0
        # В режиме SWING мы допускаем сильную буферизацию прокси (до 150 тиков в пачке)
        self.MAX_BURST_DENSITY = 50 if LatencyGate.PROFILE == "HFT" else 150 
        self.max_latency = self.LAG_THRESHOLD # Sync with profile

    def inspect_tick(self, exchange_ts_ms: int):
        """
        [GEKTOR v21.57] Усиленный инспектор с Burst и Anti-Flapping защитой.
        """
        if not exchange_ts_ms or exchange_ts_ms <= 0:
            return

        corrected_local_ms = int(self.sync.get_synchronized_time())
        latency_ms = abs(corrected_local_ms - int(exchange_ts_ms))
        now = time.time()
        
        # 0. BURST PROTECTION (Детекция буферизации прокси)
        if corrected_local_ms == self.last_arrival_ms:
            self.burst_count += 1
            if self.burst_count > self.MAX_BURST_DENSITY:
                if not self._is_quarantined:
                    self._trigger_quarantine(latency_ms)
                    logger.critical(f"🌊 [QUARANTINE] Burst Detected: {self.burst_count} ticks/ms. Proxy buffering suspected.")
                    raise StateInvalidationError("NETWORK_BURST_DETECTED")
        else:
            self.last_arrival_ms = corrected_local_ms
            self.burst_count = 0

        # [GEKTOR v21.64] DYNAMIC STALE TICK PROTECTION
        # Порог адаптируется под базовый пинг, но ограничен профилем
        base_stale = LatencyGate.STALE_LIMIT
        dynamic_threshold = max(base_stale, self.current_rtt + 250.0)
        
        # [GEKTOR v21.63] Обработка тиков "из будущего"
        lifetime_ms = corrected_local_ms - int(exchange_ts_ms)
        
        if lifetime_ms > dynamic_threshold:
             # В режиме SWING мы просто предупреждаем, если лаг не фатален
             if not self._is_quarantined:
                 if LatencyGate.PROFILE == "SWING" and lifetime_ms < 10000:
                     logger.warning(f"🐢 [GapGuard] Lazy Tick: {lifetime_ms}ms (Threshold {dynamic_threshold}ms)")
                 else:
                    self._trigger_quarantine(lifetime_ms)
             
             if LatencyGate.PROFILE == "HFT" or lifetime_ms > 15000:
                raise StateInvalidationError(f"STALE_TICK_LIFETIME_EXCEEDED: {lifetime_ms:.0f}ms (Limit: {dynamic_threshold:.0f}ms)")

        # 1. Сброс счетчика гэпов каждые 60 секунд
        if now > self.gap_count_reset_at:
            self.consecutive_gaps = 0
            self.gap_count_reset_at = now + 60.0

        if self._is_quarantined:
            # Если амнистия в режиме ожидания (Pending), проверяем стабильность канала
            if self.amnesty_pending_until > 0:
                # [GEKTOR v21.64.3] Используем STRICT_RECOVERY_RTT (800мс для SWING / 150мс для HFT)
                stability_threshold = LatencyGate.STRICT_RECOVERY_RTT
                if latency_ms > stability_threshold:
                    logger.warning(f"🛡️ [Quarantine] Stability FAILED (lag {latency_ms:.0f}ms > {stability_threshold}ms). Resetting window.")
                    self.amnesty_pending_until = now + self.stability_window
                    return # Still quarantined
                
                if now >= self.amnesty_pending_until:
                    self._lift_quarantine()
                    self.amnesty_pending_until = 0.0
                return # Processing but still in window

            if latency_ms > self.LAG_THRESHOLD:
                self._ticks_since_recovery = 0
                return # Blind

            self._ticks_since_recovery += 1
            if self._ticks_since_recovery >= self.warmup_required:
                self._lift_quarantine()
            return 

        # Стандартный поток: Проверка на возникновение нового гэпа
        if latency_ms > self.LAG_THRESHOLD:
            # В режиме DRAIN мы подавляем триггеры, чтобы не было "Шизофрении"
            if self.is_draining:
                logger.debug(f"⚠️ [Quarantine] Suppressing lag {latency_ms}ms during DRAIN.")
                return

            self._trigger_quarantine(latency_ms)
            raise StateInvalidationError(f"FATAL_LAG_DETECTED: {latency_ms:.0f}ms")

    def grant_amnesty(self, current_latency_ms: int) -> bool:
        """
        [GEKTOR v21.55] Условная амнистия.
        Система переходит в спящий режим ожидания стабильности.
        """
        if not self._is_quarantined:
            return True

        # Если лаг при выходе из Backfill уже > 1с, амнистия бессмысленна
        if current_latency_ms > 1000:
            logger.error(f"🛡️ [Quarantine] Amnesty ABORTED. Lag too high post-backfill: {current_latency_ms}ms")
            return False

        logger.info(f"🛡️ [Quarantine] Warm Recovery detected. Entering STABILITY WINDOW ({self.stability_window}s).")
        self.amnesty_pending_until = time.time() + self.stability_window
        return True

    def _trigger_quarantine(self, lag: float):
        now = time.time()
        
        # [GEKTOR v21.64] Oscillation Debounce
        if now - self.last_oscillation_time < self.DEBOUNCE_WINDOW:
            logger.warning(f"🛡️ [Quarantine] Debouncing oscillation trigger (Lag: {lag:.0f}ms)")
            self._is_quarantined = True # Still quarantine, but don't increment kill-count
            return

        self.consecutive_gaps += 1
        self.last_gap_time = now
        self.last_oscillation_time = now
        
        logger.critical(f"🛑 [QUARANTINE] Triggered by {lag:.0f}ms lag. Oscillation: {self.consecutive_gaps}/3")
        
        if self.consecutive_gaps >= 3:
            logger.critical("🚨 [GEKTOR] ANTI-FLAPPING PANIC: Persistent network instability. Executing Atomic Shutdown.")
            # Hard kill to prevent Zombie-Loop in toxic environments
            import sys
            sys.exit("CRITICAL_NETWORK_OSCILLATION")
        
        self._is_quarantined = True
        self._ticks_since_recovery = 0

    def _lift_quarantine(self):
        logger.success("🟢 [QUARANTINE] Stability verified. Mathematical integrity restored. System ARMED.")
        self._is_quarantined = False
        self.amnesty_pending_until = 0.0
    
    async def run_sentinel(self, on_degrade_callback: callable):
        """
        [GEKTOR v21.60] Periodic Health Sentinel.
        Monitors RTT and Clock Drift via ClockSynchronizer. 
        Triggers the hot-swap protocol if environment becomes toxic or Zombified.
        """
        logger.info("📡 [Sentinel] Proactive health monitoring ENGAGED.")
        check_interval = 10.0 # seconds
        
        while True:
            try:
                # 1. Perform calibration (RTT + Clock Offset)
                # Use strict threshold (150ms) if already quarantined to avoid flapping
                rtt, offset = await self.sync.calibrate()
                self.current_rtt = rtt # Update dynamic threshold base
                is_valid = LatencyGate.validate(rtt, offset, strict=self._is_quarantined or self.amnesty_pending_until > 0)
                
                # 2. Evaluate environment toxicity
                # [GEKTOR v21.64.3] Мы уходим в гибернацию только если САМ КАНАЛ токсичен.
                # Если мы в карантине, но пинг в норме (is_valid=True) — ждем Warm Recovery от GapGuard.
                is_toxic = not is_valid 
                
                if is_toxic:
                    logger.warning(f"🧟 [Sentinel] Environment TOXIC (RTT: {rtt:.1f}ms, Offset: {offset:.1f}ms). Triggering Backoff.")
                    await on_degrade_callback()
                    # Increase interval during degradation to avoid flapping
                    await asyncio.sleep(30)
                else:
                    # Environment is fine. If we are in quarantine, GapGuard is working locally.
                    await asyncio.sleep(check_interval)
                    
            except Exception as e:
                logger.error(f"⚠️ [Sentinel] Health check failed: {e}")
                # If check fails repeatedly, suspect total network failure and trigger migration
                await on_degrade_callback()
                await asyncio.sleep(5)

    @property
    def is_quarantined(self) -> bool:
        return self._is_quarantined
