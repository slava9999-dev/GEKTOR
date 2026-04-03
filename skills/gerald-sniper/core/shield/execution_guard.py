import asyncio
from dataclasses import dataclass
from loguru import logger
from typing import Any

@dataclass(slots=True, frozen=True)
class OrderIntent:
    symbol: str
    side: str
    size: float
    price: float
    order_type: str = "Limit"
    time_in_force: str = "FOK"
# Отложенный/Динамический импорт или Type Hinting для избежания циклических зависимостей
# from core.events.events import SignalEvent
from core.shield.kill_switch import KillSwitch

class ExecutionGuard:
    """
    Gatekeeper (Привратник) уровня Оркестратора.
    Паттерн: Circuit Breaker Interceptor.
    Отвечает за жесткую фильтрацию потока управления перед Execution Engine.
    """
    def __init__(self, kill_switch: KillSwitch):
        """
        :param kill_switch: Экземпляр глобального рубильника (TEKTON_ALPHA).
        """
        self.kill_switch = kill_switch

    async def intercept(self, event: Any) -> bool:
        """
        Проверяет стейт системы (Hot Path Redis).
        
        Возвращает:
            True  - Сигнал авторовзован, путь свободен.
            False - Сигнал токсичен, дропаем и XACK-аем.
        """
        # Сверхбыстрая проверка горячего стейта (Redis, <1мс)
        is_halted = await self.kill_switch.is_halted()
        
        if is_halted:
            # Жесткий дроп в инсинератор.
            # Мы намеренно возвращаем False, чтобы вызывающий код завершился УСПЕШНО без Exception,
            # и шина событий (NerveCenter) выполнила XACK (Acknowledge), навсегда удалив этот сигнал.
            event_id = getattr(event, 'event_id', 'UNKNOWN')
            logger.warning(f"🛡️ [GUARD] DROPPED Signal [{event_id}]. Причина: Система в состоянии HALT. Сигнал сожжен (XACK).")
            return False
            
        # Задел на будущее: проверка Risk-лимитов (Fat Finger, Exposure Matrix)
        # if not await self._check_risk_limits(event): 
        #     return False

        return True
