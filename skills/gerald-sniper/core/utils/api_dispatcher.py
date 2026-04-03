import hmac
import hashlib
import asyncio
import aiohttp
import orjson
from loguru import logger
from typing import Any, Dict, Optional

class APIDispatcher:
    """
    Глобальный сетевой мультиплексор Bybit V5.
    Интегрирует QoS (Token Leasing), Chronos (L7 Time Sync), KillSwitch и трекер In-Flight запросов.
    """
    def __init__(
        self, 
        session: aiohttp.ClientSession, 
        chronos: Any, # ChronosGuard 
        kill_switch: Any, # KillSwitch
        buckets: Dict[str, Any], # Матрица TokenBucket
        api_key: str, 
        api_secret: str
    ):
        self.session = session
        self.chronos = chronos
        self.kill_switch = kill_switch
        self.buckets = buckets
        self.api_key = api_key
        self.api_secret = api_secret.encode('utf-8')
        self.base_url = "https://api.bybit.com"
        self.recv_window = "1000" # Параноидальный HFT-стандарт (1 секунда)
        
        # Трекер 'летящих' запросов для Graceful Shutdown
        self._in_flight_requests: int = 0
        self._drain_event = asyncio.Event()
        self._drain_event.set()

    async def execute_with_qos(
        self, 
        method: str, 
        endpoint: str, 
        endpoint_type: str, 
        priority: int, # Enum Priority (0=P0_CRIT, 1=P1_HIGH)
        payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        
        # 1. Проверка Рубильника
        if priority != 0 and await self.kill_switch.is_halted(): 
            logger.warning(f"[DISPATCHER] Запрос к {endpoint} заблокирован: HALT стейт.")
            raise RuntimeError("System Halted. Non-P0 requests blocked.")

        # 2. Аренда лимитов
        bucket = self.buckets.get(endpoint_type)
        if not bucket:
            raise ValueError(f"CRITICAL: Неизвестный endpoint_type '{endpoint_type}'")
            
        is_p0 = (priority == 0)
        has_tokens = await bucket.consume(is_p0=is_p0)
        
        if not has_tokens:
            raise ConnectionRefusedError(f"[DISPATCHER] Rate Limit исчерпан для {endpoint_type}.")

        # 3. ChronosGuard подпись
        try:
            timestamp = self.chronos.get_timestamp()
        except Exception as e:
            logger.critical(f"[DISPATCHER] Смещение времени превысило TTL! ZERO-DAY HALT.")
            await self.kill_switch.engage(reason="CHRONOS_DRIFT_EXPIRED", trigger_id="APIDispatcher")
            raise

        payload_str = orjson.dumps(payload).decode('utf-8') if payload else ""
        
        signature_payload = timestamp + self.api_key + self.recv_window + payload_str
        signature = hmac.new(
            self.api_secret, 
            signature_payload.encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "Content-Type": "application/json"
        }

        url = f"{self.base_url}{endpoint}"
        req_timeout = aiohttp.ClientTimeout(total=1.5 if is_p0 else 2.5)

        # 4. Исполнение I/O с In-Flight Трекером
        self._in_flight_requests += 1
        self._drain_event.clear()
        
        try:
            async with self.session.request(method, url, headers=headers, data=payload_str, timeout=req_timeout) as response:
                response_data = await response.json(loads=orjson.loads)
                if response_data.get("retCode") != 0:
                    logger.error(f"[BYBIT API] Ошибка {response_data.get('retCode')}: {response_data.get('retMsg')} | Payload: {payload_str}")
                return response_data
                
        except asyncio.TimeoutError:
            logger.error(f"FATAL: Network Timeout к {endpoint}. Состояние пакета неизвестно (Schrödinger).")
            raise
        except aiohttp.ClientError as e:
            logger.error(f"FATAL: Ошибка соединения (ClientError) к {endpoint}: {e}")
            raise
        finally:
            self._in_flight_requests -= 1
            if self._in_flight_requests == 0:
                self._drain_event.set()

    async def wait_until_drained(self, timeout: float = 3.0) -> bool:
        """
        Graceful Shutdown (Узел Смерти). 
        Ждет, пока все отправленные в сеть пакеты не вернутся из Bybit ALB.
        Возвращает True, если очистка успешна, False при таймауте.
        """
        try:
            await asyncio.wait_for(self._drain_event.wait(), timeout=timeout)
            logger.info("🌊 [DISPATCHER] In-flight буфер пуст. Разрешено закрытие сокетов.")
            return True
        except asyncio.TimeoutError:
            logger.error(f"⚠️ [DISPATCHER] Таймаут Draining'а! Осталось {self._in_flight_requests} зависших запросов.")
            return False
