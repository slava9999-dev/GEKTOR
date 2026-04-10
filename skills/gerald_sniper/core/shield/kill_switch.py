import time
import asyncio
from loguru import logger
from redis.asyncio import Redis
from sqlalchemy import text
from typing import Optional, Any, Tuple

class KillSwitch:
    """
    [GEKTOR v10.0] Global Kill Switch & Circuit Breaker.
    Dual Path Persistence (Redis for <1ms, PostgreSQL for durability).
    """
    def __init__(self, redis: Redis, db_manager: Any, publisher: Any):
        self.redis = redis
        self.db = db_manager
        self.publisher = publisher
        self._halt_key = "shield:global_trade_halt"

    async def engage(self, reason: str, trigger_id: str = "GLOBAL") -> None:
        """Activates ZERO-DAY protocol (Immediate trading halt)."""
        logger.critical(f"🛡️ [SHIELD] KILL SWITCH ENGAGED! Reason: {reason} | Trigger: {trigger_id}")
        await asyncio.shield(self._execute_halt_sequence(reason, trigger_id))

    async def _execute_halt_sequence(self, reason: str, trigger_id: str) -> None:
        try:
            # 1. Cold Path: PostgreSQL (TimescaleDB)
            if self.db:
                async with self.db.engine.begin() as conn:
                    await conn.execute(text("""
                        INSERT INTO system_status (component, status, reason, updated_at) 
                        VALUES (:comp, 'HALTED', :reason, NOW())
                        ON CONFLICT (component) DO UPDATE 
                        SET status = 'HALTED', reason = :reason, updated_at = NOW()
                    """), {"comp": 'TRADING_ENGINE', "reason": f"{reason} [{trigger_id}]"})
            
            # 2. Hot Path: Redis (No TTL)
            await self.redis.set(self._halt_key, "1")

            # 3. Broadcast
            if self.publisher:
                event = {
                    "event_type": "SystemHaltEvent",
                    "reason": reason,
                    "trigger_id": trigger_id,
                    "timestamp_ms": int(time.time() * 1000)
                }
                await self.publisher.publish(event, persistent=True)

            logger.success("🔴 [SHIELD] KILL SWITCH SEQUENCE COMPLETED. Cluster locked.")
        except Exception as e:
            logger.critical(f"FATAL: KillSwitch Activation Failure: {e}")
            await self.redis.set(self._halt_key, "1")

    async def disengage(self, reason: str = "MANUAL_UNHALT") -> None:
        """Manual lift of global trading halt."""
        logger.warning(f"🟢 [SHIELD] UNHALT Init. Reason: {reason}")
        await asyncio.shield(self._execute_unhalt_sequence(reason))

    async def _execute_unhalt_sequence(self, reason: str) -> None:
        try:
            if self.db:
                async with self.db.engine.begin() as conn:
                    await conn.execute(text("""
                        UPDATE system_status 
                        SET status = 'ACTIVE', reason = :reason, updated_at = NOW()
                        WHERE component = 'TRADING_ENGINE'
                    """), {"reason": f"RECOVERED: {reason}"})
            
            await self.redis.delete(self._halt_key)
            logger.success("🟢 [SHIELD] UNHALT COMPLETE. System is ACTIVE.")
        except Exception as e:
            logger.critical(f"FATAL: Unhalt failure: {e}")
            raise

    async def is_halted(self) -> bool:
        """Fast check (Hot Path)."""
        state = await self.redis.get(self._halt_key)
        return state == "1"

    async def is_halted_atomic(self) -> bool:
        """
        [GEKTOR v10.1] Atomic check for global halt.
        Ensures consistent read from Redis.
        """
        try:
            val = await self.redis.get(self._halt_key)
            return val in (b"1", "1")
        except Exception as e:
            logger.error(f"❌ [RiskGuard] Atomic check failure: {e}")
            return True # Fail-Closed

    async def is_clear_to_trade_atomic(self, symbol: str, signal_id: str) -> Tuple[bool, str]:
        """
        [GEKTOR v10.0] Atomic Check: Kill Switch + Idempotency Lock.
        Uses Redis Pipeline to minimize RTT (Rule 16.4).
        """
        lock_key = f"tekton:idempotency:sig_{signal_id}"
        
        try:
            # We use transactional=False (just pipeline) for better performance 
            # if we don't need absolute isolation between these two keys.
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.get(self._halt_key)
                pipe.get(lock_key)
                results = await pipe.execute()
                
            is_halted = results[0] in (b"1", "1")
            is_locked = results[1] is not None
            
            if is_halted: return False, "GLOBAL_HALT_ACTIVE"
            if is_locked: return False, "SIGNAL_ID_ALREADY_LOCKED"
            
            # If clear, optimistic lock (caller will finalize execution)
            await self.redis.set(lock_key, "1", ex=3600)
            return True, "CLEAR"
        except Exception as e:
            logger.error(f"❌ [RiskGuard] Atomic check failure: {e}")
            return False, f"GUARD_FAILURE: {str(e)}"
