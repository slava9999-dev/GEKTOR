import asyncio
from typing import List
from loguru import logger
from core.shield.circuit_breaker import AsyncCircuitBreaker, CircuitBreakerState

class DiagnosticGuard:
    """
    HealthGuard: Level 1 and Level 2 Sentinel for Infrastructure Runtime Resilience.
    """
    __slots__ = ('_breakers', '_critical_timeout', '_check_interval', '_is_running', '_teardown_callback')

    def __init__(self, breakers: List[AsyncCircuitBreaker], teardown_callback, critical_timeout: float = 30.0):
        self._breakers = breakers
        self._teardown_callback = teardown_callback
        self._critical_timeout = critical_timeout
        self._check_interval = 1.0 # Сканирование каждую секунду
        self._is_running = False

    async def start(self):
        self._is_running = True
        logger.info("🛡️ Diagnostic Guard (HealthGuard) ACTIVATED.")
        await self._monitor_loop()

    def stop(self):
        self._is_running = False

    async def _monitor_loop(self):
        loop = asyncio.get_running_loop()
        
        while self._is_running:
            current_time = loop.time()
            doomsday_triggered = False
            
            for breaker in self._breakers:
                if breaker.state == CircuitBreakerState.OPEN:
                    time_in_open = current_time - breaker.last_failure_time_mono
                    logger.warning(f"HEALTH GUARD: {breaker.name} has been DEAD for {time_in_open:.1f}s.")
                    
                    # LEVEL 1 REACTION: Информирование системы (например, проброс флага "CORRUPTED")
                    # cortex.is_infrastructure_corrupted = True
                    
                    # LEVEL 2 REACTION: Экстренная Ликвидация (Судный День)
                    if time_in_open >= self._critical_timeout:
                        logger.critical(f"🚨 DOOMSDAY PROTOCOL: {breaker.name} dead for >{self._critical_timeout}s. INFRASTRUCTURE LOST.")
                        logger.critical("🚨 INITIATING TEARDOWN PROTOCOL (FLATTEN POSITIONS AND EXIT).")
                        doomsday_triggered = True
                        break

            if doomsday_triggered:
                self._is_running = False
                # Передаем управление Протоколу Эвакуации (инициируем Фазы 3-4 из Оркестратора)
                asyncio.create_task(self._teardown_callback(timeout=15))
                break
                
            await asyncio.sleep(self._check_interval)
