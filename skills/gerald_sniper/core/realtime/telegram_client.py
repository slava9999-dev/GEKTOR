# skills/gerald_sniper/core/realtime/telegram_client.py
"""
[GEKTOR v21.34] Hardened Telegram Egress Layer.

Архитектурные Инварианты:
  1. Каждый сетевой вызов обёрнут в asyncio.timeout() — защита от Silent Drop / Blackhole.
  2. ProxyRotator — автоматическая ротация прокси при отказе. Прямое соединение запрещено.
  3. Circuit Breaker — 3 провала подряд → OPEN (10s карантин) → HALF_OPEN (пробный запрос).
  4. Fallback Egress — если ВСЕ каналы мертвы, оповещение через локальные средства:
     - winsound.Beep() (Windows аварийная сирена)
     - Файл-семафор CRITICAL_ALERT.txt
     - Desktop Toast Notification (Windows 10+)

Зависимости: aiohttp, aiohttp_socks (pip install aiohttp-socks), loguru
"""

import asyncio
import sys
import time
import os
from enum import Enum
from collections import deque
from typing import Optional, List, Dict, Any
from loguru import logger

import aiohttp

# Conditional imports for proxy support
try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False
    logger.warning("⚠️ [TG] aiohttp_socks not installed. SOCKS5 proxies unavailable. Using HTTP proxy only.")


# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

_REQUEST_TIMEOUT = 3.0        # Таймаут на каждый HTTP-запрос (секунды)
_CIRCUIT_OPEN_DURATION = 10.0 # Время в состоянии OPEN (секунды)
_MAX_CONSECUTIVE_FAILURES = 3 # Провалов до открытия Circuit Breaker
_MAX_RETRY_BACKOFF = 8.0      # Макс задержка экспоненциального бэкоффа
_TG_API_BASE = "https://api.telegram.org"


# ═══════════════════════════════════════════════════════════════════════════════
# Circuit Breaker
# ═══════════════════════════════════════════════════════════════════════════════

class _CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class _CircuitBreaker:
    """
    Thread-safe Circuit Breaker для Egress Layer.
    CLOSED  → нормальная работа
    OPEN    → все запросы блокируются, ждём recovery_timeout
    HALF_OPEN → пробный запрос; если OK → CLOSED, если fail → OPEN
    """
    __slots__ = ['state', 'failure_count', 'threshold', 'recovery_timeout', 'last_failure_ts']

    def __init__(self, threshold: int = _MAX_CONSECUTIVE_FAILURES, recovery_timeout: float = _CIRCUIT_OPEN_DURATION):
        self.state = _CircuitState.CLOSED
        self.failure_count = 0
        self.threshold = threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_ts = 0.0

    def can_attempt(self) -> bool:
        """Можно ли сейчас делать запрос?"""
        if self.state == _CircuitState.CLOSED:
            return True
        if self.state == _CircuitState.HALF_OPEN:
            return True
        # OPEN: проверяем, не истёк ли карантин
        if time.monotonic() - self.last_failure_ts > self.recovery_timeout:
            self.state = _CircuitState.HALF_OPEN
            logger.info("🔄 [Circuit] HALF_OPEN: Пробный запрос через прокси...")
            return True
        return False

    def record_success(self):
        if self.state == _CircuitState.HALF_OPEN:
            logger.success("✅ [Circuit] Восстановление подтверждено. CLOSED.")
        self.state = _CircuitState.CLOSED
        self.failure_count = 0

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_ts = time.monotonic()
        if self.failure_count >= self.threshold:
            self.state = _CircuitState.OPEN
            logger.critical(
                f"🔒 [Circuit] OPEN: {self.failure_count} провалов подряд. "
                f"Карантин {self.recovery_timeout}с. Все запросы заблокированы."
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Proxy Rotator
# ═══════════════════════════════════════════════════════════════════════════════

class ProxyRotator:
    """
    Ротатор прокси с поддержкой HTTP и SOCKS5 (через aiohttp-socks).
    Прокси загружаются из переменной окружения PROXY_POOL (через запятую)
    или единственный из PROXY_URL.
    """

    def __init__(self, proxies: Optional[List[str]] = None):
        raw = proxies or self._load_from_env()
        self._proxies: List[str] = [p.strip() for p in raw if p.strip()]
        self._index = 0
        self._dead: set = set()  # Прокси, забаненные в текущем цикле

        if not self._proxies:
            logger.critical("🚨 [ProxyRotator] Пул прокси ПУСТ. Telegram будет недоступен.")
        else:
            logger.info(f"🌐 [ProxyRotator] Загружено {len(self._proxies)} прокси.")

    @staticmethod
    def _load_from_env() -> List[str]:
        pool = os.getenv("PROXY_POOL", "")
        if pool:
            return pool.split(",")
        single = os.getenv("PROXY_URL", "")
        return [single] if single else []

    @property
    def has_proxies(self) -> bool:
        return len(self._proxies) > 0

    def current(self) -> Optional[str]:
        """Текущий активный прокси."""
        if not self._proxies:
            return None
        alive = [p for p in self._proxies if p not in self._dead]
        if not alive:
            return None
        return alive[self._index % len(alive)]

    def rotate(self, failed_proxy: str):
        """Переключить на следующий прокси, пометить текущий как мёртвый."""
        self._dead.add(failed_proxy)
        alive = [p for p in self._proxies if p not in self._dead]
        if alive:
            self._index = 0
            logger.warning(f"🔄 [ProxyRotator] Переключение. Живых прокси: {len(alive)}")
        else:
            logger.critical("🚨 [ProxyRotator] ВСЕ прокси мертвы. Пул исчерпан.")

    def reset(self):
        """Сброс мёртвых прокси (вызывается после восстановления Circuit Breaker)."""
        if self._dead:
            logger.info(f"♻️ [ProxyRotator] Сброс {len(self._dead)} мёртвых прокси для повторной проверки.")
        self._dead.clear()

    def all_exhausted(self) -> bool:
        alive = [p for p in self._proxies if p not in self._dead]
        return len(alive) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Local Fallback Egress (Последний рубеж обороны)
# ═══════════════════════════════════════════════════════════════════════════════

class LocalFallbackEgress:
    """
    Аварийное оповещение через локальные каналы, когда интернет мёртв.
    Не требует сетевого доступа — работает через ОС.
    """

    @staticmethod
    async def alert(text: str, severity: str = "CRITICAL"):
        """Комбинированное локальное оповещение."""
        # 1. Файл-семафор (можно мониторить скриптом / AutoHotkey)
        try:
            alert_path = os.path.join(os.getcwd(), "CRITICAL_ALERT.txt")
            with open(alert_path, "a", encoding="utf-8") as f:
                f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [{severity}] {text}\n")
            logger.warning(f"📄 [Fallback] Алерт записан в {alert_path}")
        except Exception as e:
            logger.error(f"❌ [Fallback] Ошибка записи файла: {e}")

        # 2. Звуковая сирена (Windows)
        if sys.platform == "win32":
            try:
                import winsound
                # Три коротких сигнала — как биржевой звонок
                for _ in range(3):
                    winsound.Beep(2500, 300)  # 2500Hz, 300ms
                    await asyncio.sleep(0.15)
                logger.warning("🔊 [Fallback] Аварийный звуковой сигнал воспроизведён.")
            except Exception as e:
                logger.error(f"❌ [Fallback] Ошибка звукового сигнала: {e}")

            # 3. Windows Toast Notification
            try:
                # PowerShell-based notification (zero dependencies)
                import subprocess
                ps_cmd = (
                    f'[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, '
                    f'ContentType = WindowsRuntime] | Out-Null; '
                    f'$xml = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent(0); '
                    f'$xml.GetElementsByTagName("text")[0].AppendChild($xml.CreateTextNode('
                    f'"GEKTOR CRITICAL: {text[:80]}")) | Out-Null; '
                    f'[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('
                    f'"GEKTOR APEX").Show([Windows.UI.Notifications.ToastNotification]::new($xml))'
                )
                subprocess.Popen(
                    ["powershell", "-WindowStyle", "Hidden", "-Command", ps_cmd],
                    creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0,
                )
                logger.warning("🔔 [Fallback] Windows Toast Notification отправлено.")
            except Exception as e:
                logger.error(f"❌ [Fallback] Ошибка Toast: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TelegramClient (Main Egress Interface)
# ═══════════════════════════════════════════════════════════════════════════════

class TelegramClient:
    """
    [GEKTOR v21.34] Production-Grade Telegram Egress.

    Гарантии:
    - Каждый POST обёрнут в asyncio.timeout() — защита от Blackhole.
    - ProxyRotator — автоматическая ротация при отказе.
    - Circuit Breaker — защита от спама мёртвых сокетов.
    - LocalFallbackEgress — локальное оповещение при полной изоляции.
    - Compacting — при восстановлении из очереди отправляются только TOP-K.
    """

    def __init__(
        self, 
        token: str, 
        chat_id: str, 
        proxy: Optional[str] = None,
        proxies: Optional[List[str]] = None,
    ):
        self.token = token
        self.chat_id = chat_id
        self._url = f"{_TG_API_BASE}/bot{self.token}/sendMessage"

        # Если передан единственный proxy — добавляем в пул
        proxy_list = list(proxies or [])
        if proxy and proxy not in proxy_list:
            proxy_list.insert(0, proxy)

        self.rotator = ProxyRotator(proxy_list)
        self.breaker = _CircuitBreaker()
        self.fallback = LocalFallbackEgress()

        # Очередь недоставленных сообщений (для Compacting при восстановлении)
        self._pending_queue: deque = deque(maxlen=500)
        self._fallback_triggered = False

    async def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """
        Отправка сообщения с полным контуром защиты:
        1. Проверяем Circuit Breaker
        2. Пробуем все живые прокси с asyncio.timeout()
        3. При полном отказе — LocalFallbackEgress
        """

        # ── Circuit Breaker Check ──
        if not self.breaker.can_attempt():
            logger.warning("🔒 [TG] Circuit OPEN. Сообщение в очередь ожидания.")
            self._pending_queue.append({"text": text, "parse_mode": parse_mode, "ts": time.time()})
            return False

        # ── Attempt Delivery via Proxy Pool ──
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }

        while not self.rotator.all_exhausted():
            proxy_url = self.rotator.current()
            if proxy_url is None:
                break

            try:
                success = await self._send_via_proxy(proxy_url, payload)
                if success:
                    self.breaker.record_success()
                    self._fallback_triggered = False

                    # При восстановлении — отправляем накопленные
                    if self._pending_queue:
                        asyncio.create_task(self._flush_pending_queue())

                    return True
                else:
                    self.rotator.rotate(proxy_url)

            except Exception:
                self.rotator.rotate(proxy_url)

        # ── Все прокси исчерпаны ──
        self.breaker.record_failure()
        self._pending_queue.append({"text": text, "parse_mode": parse_mode, "ts": time.time()})

        # При первом полном отказе прокси — активировать локальный фоллбэк
        if self.breaker.state == _CircuitState.OPEN:
            self.rotator.reset()  # Сбросим dead-list для следующего цикла
            if not self._fallback_triggered:
                self._fallback_triggered = True
                await self.fallback.alert(text[:200], severity="CRITICAL")

        return False

    async def _send_via_proxy(self, proxy_url: str, payload: dict) -> bool:
        """
        Атомарная отправка одного сообщения через конкретный прокси.
        Обёрнута в asyncio.timeout() — 3 секунды максимум.
        """
        connector = None
        session = None

        try:
            # Если SOCKS5 прокси и есть aiohttp_socks
            if HAS_SOCKS and proxy_url.startswith("socks"):
                connector = ProxyConnector.from_url(proxy_url)
                session = aiohttp.ClientSession(connector=connector)
                proxy_param = None
            else:
                session = aiohttp.ClientSession()
                proxy_param = proxy_url

            # ┌──────────────────────────────────────────────────┐
            # │ CRITICAL: asyncio.timeout() на каждый POST        │
            # │ Если прокси "тихо умер" (Blackhole/DPI),          │
            # │ мы не ждём дольше 3 секунд.                       │
            # └──────────────────────────────────────────────────┘
            async with asyncio.timeout(_REQUEST_TIMEOUT):
                async with session.post(
                    self._url,
                    json=payload,
                    proxy=proxy_param,
                ) as resp:
                    if resp.status == 200:
                        return True
                    elif resp.status == 429:
                        # Telegram Rate Limit — не ротировать прокси, просто подождать
                        retry_after = int(resp.headers.get("Retry-After", "5"))
                        logger.warning(f"⏳ [TG] Rate Limited (429). Ожидание {retry_after}с.")
                        await asyncio.sleep(min(retry_after, 30))
                        return False
                    else:
                        body = await resp.text()
                        logger.error(f"❌ [TG] HTTP {resp.status}: {body[:200]}")
                        return False

        except TimeoutError:
            logger.error(f"⏰ [TG] TIMEOUT ({_REQUEST_TIMEOUT}s) через {proxy_url[:30]}...")
            return False

        except aiohttp.ClientError as e:
            logger.error(f"🔴 [TG] Сетевая ошибка через {proxy_url[:30]}...: {type(e).__name__}")
            return False

        except Exception as e:
            logger.error(f"❌ [TG] Неизвестная ошибка: {type(e).__name__}: {e}")
            return False

        finally:
            if session and not session.closed:
                await session.close()

    async def _flush_pending_queue(self):
        """
        Compacting: при восстановлении связи отправляем максимум 3 самых свежих
        сообщения, остальные суммируем в одно уведомление.
        """
        if not self._pending_queue:
            return

        logger.info(f"📨 [TG] Восстановление связи. В очереди: {len(self._pending_queue)} сообщений.")

        all_pending = list(self._pending_queue)
        self._pending_queue.clear()

        # Сортировка по времени (самые свежие — последние)
        all_pending.sort(key=lambda x: x.get("ts", 0))

        # TOP-3 самых свежих  
        top_k = 3
        winners = all_pending[-top_k:]
        losers_count = len(all_pending) - len(winners)

        # Сначала уведомление о пропущенных
        if losers_count > 0:
            compact_msg = (
                f"⚠️ <b>Пропущено {losers_count} устаревших сигналов</b>\n"
                f"Причина: сбой сети ({_CIRCUIT_OPEN_DURATION}с карантин)\n"
                f"Отправлены {len(winners)} самых актуальных:"
            )
            await self.send_message(compact_msg)
            await asyncio.sleep(0.5)  # Anti-flood

        # Отправляем топ-K
        for item in winners:
            await self.send_message(item["text"], parse_mode=item.get("parse_mode", "HTML"))
            await asyncio.sleep(0.5)  # Anti-flood (Telegram: max 30 msg/sec)
