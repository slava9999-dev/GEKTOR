import asyncio
import time
from enum import IntEnum
from loguru import logger

class Priority(IntEnum):
    CRITICAL = 0   # Исполнение ордеров, экстренный Flattening (Hot-Swap)
    BACKGROUND = 1 # AuditLoop, сбор метрик, поллинг баланса

class BinanceWeightManager:
    """
    Проактивный менеджер весов Binance USDⓈ-M Futures.
    Гарантирует резервирование лимитов для CRITICAL задач и соблюдение Retry-After.
    """
    def __init__(self, limit_1m: int = 2400, safe_threshold: int = 2000):
        self._limit_1m = limit_1m
        self._safe_threshold = safe_threshold
        
        self._current_weight = 0
        self._banned_until: float = 0.0
        
        # Lock для атомарного обновления стейта
        self._state_lock = asyncio.Lock()
        
        # Event-флаг для контроля фоновых задач. Если clear() — фоновые корутины замирают.
        self._bg_allowed = asyncio.Event()
        self._bg_allowed.set()

    async def acquire(self, priority: Priority) -> None:
        """
        Gatekeeper перед отправкой HTTP запроса.
        """
        now = time.perf_counter()
        
        # 1. Глобальная проверка на HTTP 429 / 418 бан (Audit 15.3)
        if now < self._banned_until:
            sleep_time = self._banned_until - now
            if priority == Priority.CRITICAL:
                logger.error(f"🚨 [Binance] CRITICAL request waiting {sleep_time:.2f}s due to 429 BAN.")
            await asyncio.sleep(sleep_time)
            
            # Проверка после слипа: если бан прошел, пробуем восстановить BG
            now = time.perf_counter()
            if now >= self._banned_until and not self._bg_allowed.is_set() and self._current_weight < self._safe_threshold:
                self._bg_allowed.set()

        # 2. Проверка приоритета: фоновые задачи ждут, если мы превысили Safe Threshold
        if priority == Priority.BACKGROUND:
            # Если бан уже истек, но флаг не поднят — поднимаем (Recovery)
            if now >= self._banned_until and not self._bg_allowed.is_set() and self._current_weight < self._safe_threshold:
                self._bg_allowed.set()
                
            await self._bg_allowed.wait()


    async def update_from_headers(self, status_code: int, headers: dict) -> None:
        """
        Атомарное обновление стейта на основе ответа биржи.
        Вызывается в блоке finally HTTP-клиента.
        """
        async with self._state_lock:
            # Обработка жесткого лимита (Retry-After)
            if status_code in (429, 418):
                retry_after_str = headers.get('Retry-After') or headers.get('retry-after')
                retry_after = int(retry_after_str) if retry_after_str else 60
                self._banned_until = time.perf_counter() + retry_after
                self._bg_allowed.clear()
                logger.critical(f"🚫 [Binance] IP BANNED (Code {status_code}). AuditLoop Suspended. Retry in {retry_after}s.")
                return

            # Парсинг текущего потребления
            weight_str = headers.get('x-mbx-used-weight-1m') or headers.get('X-MBX-USED-WEIGHT-1M')
            if weight_str:
                self._current_weight = int(weight_str)

                # Проактивное отключение AuditLoop при подходе к лимиту
                if self._current_weight >= self._safe_threshold:
                    if self._bg_allowed.is_set():
                        logger.warning(
                            f"⚠️ [Binance] Weight threshold breached ({self._current_weight}/{self._limit_1m}). "
                            "Throttling BACKGROUND operations to preserve routing capacity."
                        )
                        self._bg_allowed.clear()
                else:
                    # Восстановление работы AuditLoop, если окно сбросилось
                    now = time.perf_counter()
                    if not self._bg_allowed.is_set() and now > self._banned_until:
                        logger.info(f"✅ [Binance] Weight recovered ({self._current_weight}/{self._limit_1m}). Resuming BACKGROUND operations.")
                        self._bg_allowed.set()
