import time
import asyncio
from loguru import logger
from typing import Any

class StaleTimeError(Exception):
    """Выбрасывается, когда офсет времени не обновлялся дольше допустимого TTL."""
    pass

class ChronosGuard:
    """
    Application-Level Time Sync (Менеджер Временного Офсета).
    Защита от Clock Drift в Docker/WSL2 и основа для зажима recvWindow.
    """
    def __init__(self, http_client: Any):
        self.client = http_client
        self._offset_ms: float = 0.0
        self._is_calibrated: bool = False
        self._last_calibrated_at: float = 0.0
        # Инженерный стандарт: Максимальное время жизни офсета до признания его протухшим (5 минут)
        self._max_drift_age_sec: float = 300.0  
        self._lock = asyncio.Lock()

    async def calibrate(self) -> None:
        """Initial NTP-like Calibration (Захват Дельты)."""
        async with self._lock:
            try:
                # Фиксируем локальное время ПЕРЕД отправкой запроса (в мс)
                t_send = time.time() * 1000
                
                # Пинг эндпоинта времени Bybit V5
                response = await self.client.get("https://api.bybit.com/v5/market/time")
                
                # Проверка успешности (если клиент поддерживает raise_for_status, как httpx/aiohttp)
                if hasattr(response, 'raise_for_status'):
                    response.raise_for_status()
                
                # Обработка json в зависимости от клиента
                data = await response.json() if asyncio.iscoroutinefunction(response.json) else response.json()
                
                # Фиксируем локальное время ПОСЛЕ получения ответа (в мс)
                t_recv = time.time() * 1000
                
                # Bybit V5 возвращает timeSecond и timeNano
                server_time_ms = float(data["result"]["timeSecond"]) * 1000 + (float(data["result"]["timeNano"]) / 1_000_000)
                
                # Вычисление RTT (Round-Trip Time)
                rtt = t_recv - t_send
                
                # Истинное время сервера в момент t_recv (с учетом RTT/2)
                estimated_server_time_at_recv = server_time_ms + (rtt / 2)
                
                # Вычисляем смещение (Офсет) наших часов относительно биржи
                self._offset_ms = estimated_server_time_at_recv - t_recv
                self._is_calibrated = True
                self._last_calibrated_at = time.monotonic()
                
                logger.debug(f"⏳ [CHRONOS] Калибровка: RTT {rtt:.2f}ms | Смещение {self._offset_ms:.2f}ms")
            
            except Exception as e:
                logger.error(f"FATAL: Сбой калибровки ChronosGuard: {e}")
                raise # Fail-Fast на старте системы

    def get_timestamp(self) -> str:
        """
        Хирургическое Подписание.
        Возвращает скорректированный timestamp в миллисекундах для HMAC API-запросов.
        """
        if not self._is_calibrated:
            logger.critical("[CHRONOS] Запрос времени до калибровки! Падение системы.")
            raise RuntimeError("ChronosGuard is not calibrated. Cannot sign payload.")
            
        # Жесткая защита от протухания офсета (Stale Time)
        drift_age = time.monotonic() - self._last_calibrated_at
        if drift_age > self._max_drift_age_sec:
            logger.critical(f"🛑 [CHRONOS] Офсет времени протух! Возраст: {drift_age:.0f} сек (Макс: {self._max_drift_age_sec}с). Риск Clock Drift. Вызов StaleTimeError.")
            raise StaleTimeError(f"Time offset is too old ({drift_age:.0f}s). Potential Clock Drift.")
            
        # Инженерный стандарт: Истинное время = Локальное время + Офсет
        true_time_ms = (time.time() * 1000) + self._offset_ms
        return str(int(true_time_ms))

    async def run_background_recalibration(self, interval_seconds: int = 60) -> None:
        """Фоновая коррекция дрейфа часов ОС (Background Daemon)."""
        logger.info(f"[CHRONOS] Запущен фоновый воркер синхронизации (интервал {interval_seconds}с).")
        while True:
            try:
                await asyncio.sleep(interval_seconds)
                await self.calibrate()
            except asyncio.CancelledError:
                logger.info("[CHRONOS] Воркер рекалибровки остановлен.")
                break
            except Exception as e:
                logger.error(f"[CHRONOS] Сбой фоновой рекалибровки. Exponential backoff... Ошибка: {e}")
                await asyncio.sleep(5.0) # Backoff при сбое сети, чтобы не спамить API
