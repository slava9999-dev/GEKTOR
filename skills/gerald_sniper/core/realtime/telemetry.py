# skills/gerald_sniper/core/realtime/telemetry.py
import asyncio
import time
from functools import wraps
from prometheus_client import Gauge, start_http_server
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import QueuePool
from sqlalchemy import text

# СТРОГАЯ РЕГИСТРАЦИЯ МЕТРИК
METRIC_EVENT_LOOP_LAG = Gauge('gektor_event_loop_lag_seconds', 'Asyncio event loop latency')
METRIC_OUTBOX_QUEUE = Gauge('gektor_outbox_pending_total', 'Unresolved signals in Outbox')
METRIC_VPIN = Gauge('gektor_vpin_state', 'VPIN Indicator', ['symbol'])
METRIC_CORO_LATENCY = Gauge('gektor_coro_latency_seconds', 'Coroutine execution time', ['path'])
METRIC_POOL_CHECKED_OUT = Gauge('gektor_db_pool_checked_out', 'DB connections currently in use')
METRIC_POOL_OVERFLOW = Gauge('gektor_db_pool_overflow', 'DB connections in overflow')

def trace_async_path(func):
    """
    Легковесный декоратор для измерения времени выполнения корутин.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        try:
            return await func(*args, **kwargs)
        finally:
            latency = time.perf_counter() - start_time
            METRIC_CORO_LATENCY.labels(path=func.__name__).set(latency)
    return wrapper

class TelemetryManager:
    """
    Глобальный менеджер обсерваемости GEKTOR v21.51.
    Мониторит Event Loop, DB Pool и Transactional Outbox.
    """
    def __init__(self, engine: AsyncEngine, metrics_port: int = 9090):
        self.engine = engine
        self._pool = engine.pool
        self._port = metrics_port
        self._is_running = False

    def start_exporter(self):
        """Поднимает HTTP-сервер Prometheus."""
        try:
            start_http_server(self._port)
            self._is_running = True
            logger.info(f"📡 [Telemetry] Prometheus Exporter ACTIVE on port {self._port}")
        except Exception as e:
            logger.error(f"❌ [Telemetry] Failed to start Prometheus server: {e}")

    @trace_async_path
    async def monitor_event_loop_health(self):
        """Измеряет микро-задержки Event Loop'а"""
        expected_sleep = 0.1
        while self._is_running:
            start_time = time.perf_counter()
            await asyncio.sleep(expected_sleep)
            actual_sleep = time.perf_counter() - start_time
            
            lag = max(0.0, actual_sleep - expected_sleep)
            METRIC_EVENT_LOOP_LAG.set(lag)
            
            if lag > 0.05:
                # Агрессивно логируем лаг > 50мс
                logger.warning(f"⚠️ [Telemetry] Event Loop Lag Spike: {lag * 1000:.1f}ms")

    @trace_async_path
    async def monitor_db_stats(self):
        """[GEKTOR v21.51] Инженерный стандарт мониторинга пула соединений SQLAlchemy."""
        while self._is_running:
            if isinstance(self._pool, QueuePool):
                stats = {
                    "size": self._pool.size(),
                    "checkedin": self._pool.checkedin(),
                    "checkedout": self._pool.checkedout(),
                    "overflow": self._pool.overflow(),
                }
                
                METRIC_POOL_CHECKED_OUT.set(stats["checkedout"])
                METRIC_POOL_OVERFLOW.set(stats["overflow"])

                if stats["checkedout"] > (stats["size"] * 0.8):
                    logger.warning(f"⚠️ [Telemetry] DB Pool Saturation: {stats['checkedout']}/{stats['size']} (Overflow: {stats['overflow']})")
            else:
                logger.error(f"❌ [Telemetry] Unexpected pool type: {type(self._pool)}")
            
            await asyncio.sleep(10.0)

    @trace_async_path
    async def monitor_outbox_lag(self):
        """
        Контролирует длину очереди Transactional Outbox.
        Использует statement_timeout для предотвращения зависания при Deadlock в БД.
        """
        while self._is_running:
            try:
                # Используем engine напрямую с таймаутом выполнения запроса (2 секунды)
                async with self.engine.connect() as conn:
                    # Устанавливаем таймаут для текущей сессии для защиты Event Loop
                    await conn.execute(text("SET statement_timeout = '2s'"))
                    
                    result = await conn.execute(
                        text("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING';")
                    )
                    count = result.scalar()
                    METRIC_OUTBOX_QUEUE.set(count if count is not None else 0)
            except Exception as e:
                logger.error(f"❌ [Telemetry] Outbox check failed (Deadlock risk?): {e}")
            
            await asyncio.sleep(5.0)

    @staticmethod
    def update_vpin_metric(symbol: str, vpin_value: float):
        METRIC_VPIN.labels(symbol=symbol).set(vpin_value)
