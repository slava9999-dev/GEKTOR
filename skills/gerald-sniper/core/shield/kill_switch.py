"""
Global Kill Switch & Circuit Breaker (TEKTON_ALPHA Architecture).
Provides absolute HALT command across the entire cluster using Dual Path Persistence
(Redis for <1ms latency, PostgreSQL for cold-recovery state).
"""

import asyncio
import asyncpg
from loguru import logger
from redis.asyncio import Redis
from typing import Optional, Any

class KillSwitch:
    """
    Абсолютный блокиратор торгов. Двойная запись стейта (Memory + Disk).
    Защищает капитал от Schrödinger's Orders и рассинхронизации стейта.
    """
    def __init__(self, redis: Redis, db_pool: asyncpg.Pool, publisher: Any):
        """
        :param redis: Асинхронный клиент Redis.
        :param db_pool: Пул подключений asyncpg к PostgreSQL (TimescaleDB).
        :param publisher: Компонент EventBus/SafePublish для трансляции SystemHaltEvent по кластеру.
        """
        self.redis = redis
        self.db = db_pool
        self.publisher = publisher
        self._halt_key = "shield:global_trade_halt"

    async def engage(self, reason: str, trigger_id: str = "GLOBAL") -> None:
        """
        Активирует протокол ZERO-DAY (Мгновенная остановка всех торговых систем).
        Обернут в asyncio.shield(), чтобы предотвратить прерывание процесса остановки.
        """
        logger.critical(f"🛡️ [SHIELD] KILL SWITCH ENGAGED! Reason: {reason} | Trigger: {trigger_id}")
        
        # Гарантируем, что операция не будет отменена, даже если вызывающая корутина упадет
        await asyncio.shield(self._execute_halt_sequence(reason, trigger_id))

    async def _execute_halt_sequence(self, reason: str, trigger_id: str) -> None:
        """
        Внутренний механизм двойной фиксации стейта.
        """
        try:
            # 1. Cold Path: Персистентная фиксация в БД
            async with self.db.acquire() as conn:
                # Используем транзакцию для безопасности
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO system_status (component, status, reason, updated_at) 
                        VALUES ('TRADING_ENGINE', 'HALTED', $1, NOW())
                        ON CONFLICT (component) DO UPDATE 
                        SET status = 'HALTED', reason = $1, updated_at = NOW()
                    """, f"{reason} [{trigger_id}]")
            
            # 2. Hot Path: Мгновенная блокировка в Redis (TTL намеренно не ставится).
            await self.redis.set(self._halt_key, b"1")

            # 3. Panic Broadcast в шину событий (NerveCenter)
            if self.publisher:
                try:
                    event_payload = {
                        "event_type": "SystemHaltEvent",
                        "reason": reason,
                        "trigger_id": trigger_id,
                        # Используется локальное время для логов, ChronosGuard - для биржи
                        "timestamp_ms": int(asyncio.get_event_loop().time() * 1000) 
                    }
                    if hasattr(self.publisher, 'safe_publish'):
                        # safe_publish фасад гарантирует отправку в Redis Streams
                        await self.publisher.safe_publish(event_payload)
                    elif hasattr(self.publisher, 'publish'):
                        await self.publisher.publish("events:critical:halt", event_payload)
                except Exception as e:
                    logger.error(f"⚠️ [SHIELD] Ошибка трансляции SystemHaltEvent: {e}")

            logger.success("🔴 [SHIELD] KILL SWITCH SEQUENCE COMPLETED. Cluster locked.")

        except Exception as e:
            logger.critical(f"FATAL: Ошибка при активации KillSwitch! Система МОЖЕТ БЫТЬ НЕ ОСТАНОВЛЕНА: {e}")
            # Пытаемся хотя бы заблокировать Redis как Fallback
            try:
                await self.redis.set(self._halt_key, b"1")
                logger.info("🔴 [SHIELD] Fallback Redis HALT applied.")
            except:
                pass

    async def disengage(self, reason: str = "MANUAL_UNHALT") -> None:
        """
        Снятие блокировки торгов (UNHALT).
        Вызывается из StateReconciler только после 'токсичной ликвидации'.
        """
        logger.warning(f"🟢 [SHIELD] Инициация UNHALT (Снятие блокировки). Причина: {reason}")
        await asyncio.shield(self._execute_unhalt_sequence(reason))

    async def _execute_unhalt_sequence(self, reason: str) -> None:
        try:
            async with self.db.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                        UPDATE system_status 
                        SET status = 'ACTIVE', reason = $1, updated_at = NOW()
                        WHERE component = 'TRADING_ENGINE'
                    """, f"RECOVERED: {reason}")
            
            # Атомарное снятие флага в Redis
            await self.redis.delete(self._halt_key)
            logger.success("🟢 [SHIELD] Блокировка снята. Система вернулась в строй (ACTIVE).")

        except Exception as e:
            logger.critical(f"FATAL: Сбой при снятии HALT: {e}")
            raise

    async def is_halted(self) -> bool:
        """
        Быстрая проверка состояния (Hot Path).
        Вызывается ExecutionGuard для каждого SignalEvent.
        """
        state = await self.redis.get(self._halt_key)
        return state == b"1"
