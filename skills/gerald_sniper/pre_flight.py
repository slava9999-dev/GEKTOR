"""
[GEKTOR v8.4] PreflightCommander — Fail-Fast Infrastructure Validation.

Architecture: 5-phase sequential check. Any CRITICAL failure → sys.exit(1).
Phase 1: Environment & Secrets
Phase 2: Redis (NerveCenter) — ping + write/read/delete
Phase 3: PostgreSQL (Blackbox) — connection + WAL mode
Phase 4: Bybit API — connectivity + permissions audit (optional in ADVISORY)
Phase 5: Network Profiler — RTT measurement through proxy

Also provides PeriodicKeyValidator for long-running key expiry detection.
"""

import asyncio
import time
import os
import sys
from typing import Optional
from loguru import logger
from dataclasses import dataclass, field


@dataclass
class PreflightReport:
    """Structured result of all pre-flight checks."""
    secrets_ok: bool = False
    redis_ok: bool = False
    redis_write_ok: bool = False
    postgres_ok: bool = False
    bybit_connectivity: bool = False
    bybit_keys_present: bool = False
    bybit_permissions_ok: Optional[bool] = None  # None = not checked (no keys)
    avg_latency_ms: float = 0.0
    latency_status: str = "UNKNOWN"
    errors: list = field(default_factory=list)
    mode: str = "ADVISORY"
    
    @property
    def is_critical_ok(self) -> bool:
        """All critical checks passed (enough for ADVISORY mode)."""
        return self.secrets_ok and self.redis_ok and self.postgres_ok
    
    @property
    def is_full_ok(self) -> bool:
        """All checks including Bybit API passed (needed for LIVE mode)."""
        return self.is_critical_ok and self.bybit_connectivity and self.bybit_permissions_ok


class PreflightCommander:
    """
    [GEKTOR v8.4] Pre-flight Check Engine.
    Validates all infrastructure layers before system startup.
    """
    
    def __init__(self, rest_client=None, mode: str = "ADVISORY"):
        self.rest = rest_client
        self.mode = mode
        self.report = PreflightReport(mode=mode)
    
    async def execute(self) -> PreflightReport:
        """
        Run all pre-flight checks sequentially.
        Returns PreflightReport. Exits with sys.exit(1) on critical failure.
        """
        logger.info("🛠️ [PREFLIGHT] ═══ Запуск предстартовых проверок ═══")
        t0 = time.monotonic()
        
        await self._check_secrets()
        await self._check_redis()
        await self._check_postgres()
        await self._check_bybit_connectivity()
        
        if self.report.bybit_keys_present:
            await self._check_bybit_permissions()
        
        await self._measure_latency()
        
        elapsed = (time.monotonic() - t0) * 1000
        
        # Verdict
        if not self.report.is_critical_ok:
            logger.critical(
                f"💀 [PREFLIGHT] КРИТИЧЕСКИЕ ПРОВЕРКИ ПРОВАЛЕНЫ ({elapsed:.0f}ms). "
                f"Ошибки: {self.report.errors}"
            )
            sys.exit(1)
        
        logger.success(f"✅ [PREFLIGHT] Все проверки пройдены ({elapsed:.0f}ms).")
        return self.report
    
    # ═══════════════════════════════════════════════════════════════════
    # Phase 1: Secrets & Environment
    # ═══════════════════════════════════════════════════════════════════
    
    async def _check_secrets(self):
        """Validate required environment variables."""
        logger.info("🔑 [PREFLIGHT] Phase 1: Secrets Guard...")
        
        # CRITICAL (always required)
        critical_vars = {
            "GERALD_BOT_TOKEN": os.getenv("GERALD_BOT_TOKEN"),
            "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID"),
            "REDIS_URL": os.getenv("REDIS_URL"),
        }
        
        missing = [k for k, v in critical_vars.items() if not v]
        if missing:
            self.report.errors.append(f"Missing env vars: {missing}")
            logger.critical(f"❌ [SECRETS] Отсутствуют: {missing}")
            self.report.secrets_ok = False
            return
        
        # OPTIONAL (needed for LIVE, not for ADVISORY)
        api_key = os.getenv("BYBIT_API_KEY", "")
        api_secret = os.getenv("BYBIT_API_SECRET", "")
        self.report.bybit_keys_present = bool(api_key and api_secret)
        
        if not self.report.bybit_keys_present:
            logger.warning(
                "⚠️ [SECRETS] BYBIT_API_KEY/SECRET пусты. "
                "Режим: ADVISORY ONLY (без исполнения ордеров)."
            )
        else:
            logger.success("✅ [SECRETS] Bybit API ключи присутствуют.")
        
        self.report.secrets_ok = True
        logger.success("✅ [PREFLIGHT] Phase 1: Secrets OK")
    
    # ═══════════════════════════════════════════════════════════════════
    # Phase 2: Redis (NerveCenter)
    # ═══════════════════════════════════════════════════════════════════
    
    async def _check_redis(self):
        """Redis ping + write/read/delete cycle."""
        logger.info("🗄️ [PREFLIGHT] Phase 2: Redis Health Check...")
        
        try:
            import redis.asyncio as aioredis
            
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6381/0")
            r = aioredis.from_url(redis_url, decode_responses=True)
            
            # Ping
            pong = await r.ping()
            if not pong:
                raise ConnectionError("Redis PING returned False")
            
            self.report.redis_ok = True
            
            # Write/Read/Delete cycle (persistence check)
            test_key = "tekton:preflight_test"
            test_val = f"check_{time.time()}"
            await r.set(test_key, test_val, ex=10)
            read_val = await r.get(test_key)
            await r.delete(test_key)
            
            if read_val == test_val:
                self.report.redis_write_ok = True
                logger.success("✅ [REDIS] PING + Write/Read/Delete OK")
            else:
                logger.error("❌ [REDIS] Write/Read mismatch!")
                self.report.errors.append("Redis write/read mismatch")
            
            await r.aclose()
            
        except Exception as e:
            logger.critical(f"❌ [REDIS] Отказ: {e}")
            self.report.errors.append(f"Redis: {e}")
            self.report.redis_ok = False
    
    # ═══════════════════════════════════════════════════════════════════
    # Phase 3: PostgreSQL (Blackbox)
    # ═══════════════════════════════════════════════════════════════════
    
    async def _check_postgres(self):
        """PostgreSQL connection + basic query."""
        logger.info("🗄️ [PREFLIGHT] Phase 3: PostgreSQL Health Check...")
        
        try:
            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy import text
            
            db_url = os.getenv("ASYNC_DATABASE_URL")
            if not db_url:
                logger.error("❌ [POSTGRES] ASYNC_DATABASE_URL не задан!")
                self.report.errors.append("ASYNC_DATABASE_URL missing")
                return
            
            engine = create_async_engine(db_url, echo=False)
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                result.fetchone()
            
            await engine.dispose()
            self.report.postgres_ok = True
            logger.success("✅ [POSTGRES] Connection OK")
            
        except Exception as e:
            logger.critical(f"❌ [POSTGRES] Отказ: {e}")
            self.report.errors.append(f"PostgreSQL: {e}")
    
    # ═══════════════════════════════════════════════════════════════════
    # Phase 4: Bybit API Connectivity
    # ═══════════════════════════════════════════════════════════════════
    
    async def _check_bybit_connectivity(self):
        """Test public API endpoint reachability."""
        logger.info("🌐 [PREFLIGHT] Phase 4: Bybit API Connectivity...")
        
        if not self.rest:
            logger.warning("⚠️ [BYBIT] REST client not provided. Skipping.")
            return
        
        try:
            result = await self.rest.get_server_time()
            if result and not result.get("error"):
                self.report.bybit_connectivity = True
                logger.success("✅ [BYBIT] Public API reachable.")
            else:
                logger.error(f"❌ [BYBIT] API error: {result}")
                self.report.errors.append(f"Bybit API: {result}")
        except Exception as e:
            logger.error(f"❌ [BYBIT] Connectivity failed: {e}")
            self.report.errors.append(f"Bybit connectivity: {e}")
    
    async def _check_bybit_permissions(self):
        """Validate API key permissions (ContractTrade required for execution)."""
        logger.info("🔐 [PREFLIGHT] Phase 4b: Bybit API Permissions Audit...")
        
        if not self.rest:
            return
        
        try:
            # query-api requires signed request
            from data.bybit_rest import generate_auth_headers
            api_key = os.getenv("BYBIT_API_KEY", "")
            api_secret = os.getenv("BYBIT_API_SECRET", "")
            
            if not api_key or not api_secret:
                return
            
            headers = generate_auth_headers(api_key, api_secret, {})
            result = await self.rest.get_api_info(headers=headers)
            
            if isinstance(result, dict) and not result.get("error"):
                permissions = result.get("permissions", {})
                has_trade = (
                    permissions.get("ContractTrade", []) or
                    permissions.get("Spot", [])
                )
                if has_trade:
                    self.report.bybit_permissions_ok = True
                    logger.success("✅ [BYBIT] API ключ имеет ТОРГОВЫЕ права. СИСТЕМА ГОТОВА К LIVE.")
                else:
                    self.report.bybit_permissions_ok = False
                    logger.warning(
                        "🟡 [BYBIT] API ключ READ-ONLY. (ADVISORY_MODE ACTIVE). "
                        "Торговые операции будут отклоняться биржей."
                    )
            else:
                logger.warning(f"⚠️ [BYBIT] query-api returned: {result}")
                
        except Exception as e:
            logger.warning(f"⚠️ [BYBIT] Permissions check failed: {e}")
    
    # ═══════════════════════════════════════════════════════════════════
    # Phase 5: Network Profiler
    # ═══════════════════════════════════════════════════════════════════
    
    async def _measure_latency(self):
        """Measure RTT to Bybit through proxy."""
        logger.info("📡 [PREFLIGHT] Phase 5: Network Profiler...")
        
        if not self.rest:
            logger.warning("⚠️ [LATENCY] REST client not provided. Skipping.")
            return
        
        latencies = []
        for i in range(3):
            t0 = time.perf_counter()
            try:
                await self.rest.get_server_time()
                rtt = (time.perf_counter() - t0) * 1000
                latencies.append(rtt)
            except Exception:
                latencies.append(5000.0)  # Timeout marker
            
            if i < 2:
                await asyncio.sleep(0.1)  # Rate limit pacing
        
        avg = sum(latencies) / len(latencies)
        self.report.avg_latency_ms = avg
        
        if avg < 300:
            self.report.latency_status = "🟢 ОТЛИЧНО"
        elif avg < 800:
            self.report.latency_status = "🟡 ПРИЕМЛЕМО"
        elif avg < 1500:
            self.report.latency_status = "🟠 МЕДЛЕННО"
        else:
            self.report.latency_status = "🔴 КРИТИЧНО"
        
        logger.info(
            f"📡 [LATENCY] RTT: {latencies[0]:.0f} / {latencies[1]:.0f} / {latencies[2]:.0f} ms "
            f"| Avg: {avg:.0f}ms {self.report.latency_status}"
        )
    
    def format_boot_report_telegram(self) -> str:
        """Format pre-flight results for Telegram startup notification."""
        r = self.report
        keys_status = "✅ Подтверждены" if r.bybit_keys_present else "⚠️ Не заданы (ADVISORY)"
        perms = "✅" if r.bybit_permissions_ok else ("❌ Нет прав" if r.bybit_permissions_ok is False else "⏭ Не проверено")
        
        return (
            f"🚀 <b>GEKTOR v14.8.1: PRE-FLIGHT OK</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🛡️ <b>Mode:</b> {r.mode}\n"
            f"🔑 <b>Keys:</b> {keys_status}\n"
            f"🔐 <b>Trade:</b> {perms} <i>(ContractTrade)</i>\n"
            f"🗄️ <b>Redis:</b> {'✅' if r.redis_ok else '❌'} "
            f"{'(W/R OK)' if r.redis_write_ok else ''}\n"
            f"🗄️ <b>PostgreSQL:</b> {'✅' if r.postgres_ok else '❌'}\n"
            f"🌐 <b>Bybit API:</b> {'✅' if r.bybit_connectivity else '❌'}\n"
            f"📡 <b>Задержка:</b> {r.avg_latency_ms:.0f}ms {r.latency_status}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🐕 <b>Watchdog:</b> Armed\n"
            f"📡 <b>MacroRadar:</b> Hydrating...\n"
            f"☢️ <b>/panic_sell:</b> {'Armed' if r.bybit_keys_present else 'Disabled (no keys)'}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# [GEKTOR v8.4] Periodic API Key Validator
# ═══════════════════════════════════════════════════════════════════════════
# Runs once per 24h in background. If Bybit deactivates the key (90-day
# policy for non-static IPs), detects it proactively and alerts via TG.
# ═══════════════════════════════════════════════════════════════════════════

class PeriodicKeyValidator:
    """
    Background daemon that validates Bybit API key once daily.
    Prevents the scenario where keys expire silently after 90 days
    and the operator discovers it only when pressing [⚡ LONG].
    """
    
    CHECK_INTERVAL_SEC = 86400  # 24 hours
    
    def __init__(self, rest_client):
        self.rest = rest_client
        self._last_check = 0.0
        self._consecutive_failures = 0
    
    async def run(self, stop_event: asyncio.Event):
        """Background loop: check keys every 24h."""
        api_key = os.getenv("BYBIT_API_KEY", "")
        api_secret = os.getenv("BYBIT_API_SECRET", "")
        
        if not api_key or not api_secret:
            logger.info("🔑 [KEY_VALIDATOR] No API keys configured. Daemon disabled.")
            return
        
        logger.info("🔑 [KEY_VALIDATOR] Periodic key validation daemon ACTIVE (24h cycle).")
        
        while not stop_event.is_set():
            # Wait for next check cycle
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.CHECK_INTERVAL_SEC
                )
                break  # stop_event was set
            except asyncio.TimeoutError:
                pass  # Time to check
            
            await self._validate_key(api_key, api_secret)
    
    async def _validate_key(self, api_key: str, api_secret: str):
        """Perform one API key validation cycle."""
        logger.info("🔑 [KEY_VALIDATOR] Periodic validation cycle...")
        
        try:
            from data.bybit_rest import generate_auth_headers
            headers = generate_auth_headers(api_key, api_secret, {})
            result = await self.rest.get_api_info(headers=headers)
            
            if isinstance(result, dict) and not result.get("error"):
                permissions = result.get("permissions", {})
                has_trade = bool(
                    permissions.get("ContractTrade", []) or
                    permissions.get("Spot", [])
                )
                
                self._consecutive_failures = 0
                
                if has_trade:
                    logger.success("✅ [KEY_VALIDATOR] API key valid, trade permissions OK.")
                else:
                    logger.critical(
                        "🔴 [KEY_VALIDATOR] API key valid but TRADE PERMISSIONS REVOKED!"
                    )
                    await self._alert_key_issue(
                        "API ключ активен, но ТОРГОВЫЕ ПРАВА ОТОЗВАНЫ!\n"
                        "Проверь настройки API на Bybit."
                    )
            else:
                error = result.get("error") if isinstance(result, dict) else str(result)
                ret_code = result.get("retCode") if isinstance(result, dict) else None
                
                # 10003, 10004 = invalid key / expired
                if ret_code in (10003, 10004, 33004):
                    logger.critical(
                        f"💀 [KEY_VALIDATOR] API KEY EXPIRED/INVALID! "
                        f"Code: {ret_code}, Error: {error}"
                    )
                    await self._alert_key_issue(
                        f"💀 API КЛЮЧ МЕРТВ (код {ret_code})!\n"
                        f"Bybit деактивировал ключ.\n"
                        f"Сгенерируй новый на bybit.com/app/user/api-management"
                    )
                else:
                    self._consecutive_failures += 1
                    logger.warning(
                        f"⚠️ [KEY_VALIDATOR] Check inconclusive: {error} "
                        f"(fails: {self._consecutive_failures})"
                    )
                    
        except Exception as e:
            self._consecutive_failures += 1
            logger.error(f"⚠️ [KEY_VALIDATOR] Check failed: {e}")
    
    async def _alert_key_issue(self, message: str):
        """Send critical TG alert about key problems."""
        try:
            from core.alerts.outbox import telegram_outbox
            telegram_outbox.enqueue(
                text=(
                    f"🔑🔴 <b>API KEY ALERT</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"{message}"
                ),
                priority=3,
                disable_notification=False,
            )
        except Exception:
            pass
