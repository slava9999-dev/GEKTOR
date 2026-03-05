import aiohttp
import asyncio
import time
from loguru import logger
from typing import List, Dict, Any


class BybitREST:
    """
    Bybit REST API v5 клиент.
    Обрабатывает rate limits, ретраи, и реализует circuit breaker
    с автопереключением на fallback URL при потере связи.
    """

    # Circuit breaker пороги
    CB_FAILURE_THRESHOLD = 3       # последовательных ошибок до активации
    CB_INITIAL_COOLDOWN = 30       # секунд до первой повторной попытки
    CB_MAX_COOLDOWN = 300          # максимальный кулдаун (5 минут)
    CB_COOLDOWN_MULTIPLIER = 2     # множитель экспоненциального бэкоффа

    def __init__(self, base_url: str = "https://api.bybit.com",
                 fallback_url: str = "https://api.bytick.com"):
        self.primary_url = base_url
        self.fallback_url = fallback_url
        self.session = None

        # Circuit breaker состояние
        self._consecutive_failures = 0
        self._circuit_open = False
        self._circuit_open_until = 0.0
        self._current_cooldown = self.CB_INITIAL_COOLDOWN
        self._last_error_msg = ""

        # Fallback состояние
        self._using_fallback = False
        self._primary_fail_streak = 0
        self._fallback_switch_threshold = 3     # После 3 провалов — переключиться
        self._last_primary_check = 0.0
        self._primary_recheck_interval = 600    # Каждые 10 мин пробовать primary
        
        self._rate_semaphore = asyncio.Semaphore(7)  # Выше concurrency для 120 тикеров
        self._rate_delay = 0.2  # 200ms между запросами (чтобы не душить API)
        self._max_rate_delay = 5.0
        self._min_rate_delay = 0.1

    @property
    def _active_url(self) -> str:
        """Текущий активный базовый URL."""
        return self.fallback_url if self._using_fallback else self.primary_url

    @property
    def is_healthy(self) -> bool:
        """True если клиент считает API доступным."""
        if not self._circuit_open:
            return True
        return time.monotonic() >= self._circuit_open_until

    async def _get_session(self):
        if self.session is None or self.session.closed:
            # Увеличенный таймаут сессии для предотвращения отвалов на плохом коннекте
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _record_success(self):
        """Сброс circuit breaker и fallback при успешном запросе."""
        if self._consecutive_failures > 0 or self._circuit_open:
            logger.info(f"🟢 Bybit API connection restored via {self._active_url}")
        self._consecutive_failures = 0
        self._circuit_open = False
        self._current_cooldown = self.CB_INITIAL_COOLDOWN
        self._last_error_msg = ""
        # Успех на primary → сброс fallback счётчика
        if not self._using_fallback:
            self._primary_fail_streak = 0

    def _record_failure(self, error_msg: str):
        """Трекинг ошибок и активация circuit breaker."""
        self._consecutive_failures += 1
        self._last_error_msg = error_msg

        if self._consecutive_failures >= self.CB_FAILURE_THRESHOLD:
            # Если circuit был закрыт (или мы в режиме half-open и снова упали)
            if not self._circuit_open or time.monotonic() >= self._circuit_open_until:
                self._circuit_open = True
                self._circuit_open_until = time.monotonic() + self._current_cooldown
                logger.warning(
                    f"🔴 Circuit breaker OPEN — {self._consecutive_failures} consecutive failures. "
                    f"Last error: {error_msg}. "
                    f"Will retry in {self._current_cooldown}s."
                )
                self._current_cooldown = min(
                    self._current_cooldown * self.CB_COOLDOWN_MULTIPLIER,
                    self.CB_MAX_COOLDOWN,
                )

    def _switch_to_fallback(self):
        """Переключение на fallback URL."""
        if not self._using_fallback and self.fallback_url:
            self._using_fallback = True
            self._primary_fail_streak = 0
            # Сброс circuit breaker при переключении — даём шанс fallback
            self._circuit_open = False
            self._consecutive_failures = 0
            self._current_cooldown = self.CB_INITIAL_COOLDOWN
            logger.warning(f"🔄 Switching REST to fallback: {self.fallback_url}")

    def _switch_to_primary(self):
        """Возврат на primary URL."""
        if self._using_fallback:
            self._using_fallback = False
            self._primary_fail_streak = 0
            logger.info(f"🔄 Switched back to primary REST: {self.primary_url}")

    async def _try_primary_probe(self, endpoint: str, params: Dict = None):
        """Периодическая проверка primary URL, если сидим на fallback."""
        if not self._using_fallback:
            return

        now = time.monotonic()
        if now - self._last_primary_check < self._primary_recheck_interval:
            return

        self._last_primary_check = now
        try:
            session = await self._get_session()
            test_url = f"{self.primary_url}{endpoint}"
            async with session.request("GET", test_url, params=params,
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        self._switch_to_primary()
        except Exception:
            pass  # Остаёмся на fallback

    async def _request(self, method: str, endpoint: str, params: Dict = None, retries: int = 3) -> Dict:
        async with self._rate_semaphore:
            await asyncio.sleep(self._rate_delay)
            # Периодически пробуем вернуться на primary
            await self._try_primary_probe(endpoint, params)

        # Circuit breaker проверка
        if self._circuit_open:
            now = time.monotonic()
            if now < self._circuit_open_until:
                # Если застряли на fallback и он тоже мёртв — пробуем primary
                if self._using_fallback and self._consecutive_failures >= self.CB_FAILURE_THRESHOLD:
                    self._switch_to_primary()
                    self._circuit_open = False
                    self._consecutive_failures = 0
                    self._current_cooldown = self.CB_INITIAL_COOLDOWN
                    logger.info(f"🔄 Cycling REST back to primary: {self.primary_url}")
                else:
                    remaining = int(self._circuit_open_until - now)
                    logger.debug(
                        f"Circuit breaker open, skipping {endpoint}. Retry in {remaining}s."
                    )
                    return {}
            logger.info(f"🟡 Circuit breaker half-open — probing {endpoint}...")

        session = await self._get_session()
        url = f"{self._active_url}{endpoint}"

        for attempt in range(retries):
            try:
                # Индивидуальный явный таймаут для запроса
                req_timeout = aiohttp.ClientTimeout(total=15)
                async with session.request(method, url, params=params, timeout=req_timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        ret_code = data.get("retCode")
                        if ret_code == 0:
                            self._record_success()
                            # Decrease rate delay gracefully upon success
                            self._rate_delay = max(self._min_rate_delay, self._rate_delay * 0.95)
                            return data.get("result", {})
                        elif ret_code in (10006, 10018) or "Too many visits" in str(data.get("retMsg")):
                            # Adaptive rate limiter: slow down exponentially if warned
                            self._rate_delay = min(self._max_rate_delay, self._rate_delay * 2.0)
                            wait_time = (2 ** attempt) + self._rate_delay
                            logger.warning(f"Rate limited (retCode {ret_code}) on {url}. Adaptive delay updated to {self._rate_delay:.2f}s. Waiting {wait_time:.2f}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"Bybit API Error: {data.get('retMsg')} for url {url}")
                            return {}
                    elif response.status == 429:
                        self._rate_delay = min(self._max_rate_delay, self._rate_delay * 2.0)
                        wait_time = (2 ** attempt) + self._rate_delay
                        logger.warning(f"Rate limited (429) on {url}. Adaptive delay updated to {self._rate_delay:.2f}s. Waiting {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"HTTP Error {response.status} on {url}")
                        return {}

            except asyncio.TimeoutError:
                logger.warning(f"Timeout on {url}. Attempt {attempt + 1}/{retries}")
                await asyncio.sleep(1 + attempt)

            except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, OSError) as e:
                # Сетевые ошибки (DNS, connection refused и т.д.)
                if attempt == retries - 1:
                    logger.error(f"Network error on {url}: {e} (after {retries} attempts)")
                    self._record_failure(str(e))

                    # Если primary провалился — пробуем fallback
                    if not self._using_fallback and self.fallback_url:
                        self._primary_fail_streak += 1
                        if self._primary_fail_streak >= self._fallback_switch_threshold:
                            self._switch_to_fallback()
                            # Немедленная попытка через fallback
                            try:
                                fb_url = f"{self.fallback_url}{endpoint}"
                                async with session.request(method, fb_url, params=params) as fb_resp:
                                    if fb_resp.status == 200:
                                        fb_data = await fb_resp.json()
                                        if fb_data.get("retCode") == 0:
                                            self._record_success()
                                            return fb_data.get("result", {})
                            except Exception as fb_e:
                                logger.error(f"Fallback also failed: {fb_e}")
                else:
                    await asyncio.sleep(1 + attempt)

            except Exception as e:
                logger.error(f"Unexpected error on {url}: {e}. Attempt {attempt + 1}/{retries}")
                await asyncio.sleep(1 + attempt)

        return {}

    async def get_tickers(self) -> List[Dict]:
        """Fetch all linear tickers."""
        result = await self._request("GET", "/v5/market/tickers", params={"category": "linear"})
        return result.get("list", [])

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[Dict]:
        """Fetch klines."""
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        result = await self._request("GET", "/v5/market/kline", params=params)
        klines = result.get("list", [])
        klines.reverse()  # Bybit returns descending, we want ascending
        return klines

    async def get_open_interest(self, symbol: str, interval_time: str, limit: int = 5) -> List[Dict]:
        """Fetch historical Open Interest."""
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval_time, "limit": limit}
        result = await self._request("GET", "/v5/market/open-interest", params=params)
        return result.get("list", [])
