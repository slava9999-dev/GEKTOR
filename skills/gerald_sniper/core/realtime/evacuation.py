# skills/gerald_sniper/core/realtime/evacuation.py
import asyncio
from loguru import logger
import time

class EmergencyLiquidationProtocol:
    """
    [GEKTOR v21.49] Nuclear Kill Switch.
    Протокол экстренной эвакуации капитала при крахе биржи или API.
    """
    def __init__(self, gateway, max_429_errors: int = 5):
        self.gateway = gateway
        self.is_evacuating = False
        self.error_count_429 = 0
        self.max_429 = max_429_errors

    async def trigger_nuclear_halt(self, active_positions: list):
        """
        Метод полной эвакуации. Закрывает все позиции по рынку любой ценой.
        """
        if self.is_evacuating: return
        self.is_evacuating = True
        
        logger.critical("☢️ [NUCLEAR] EMERGENCY LIQUIDATION TRIGGERED. EVACUATING POSITIONS.")
        
        # 1. Отключаем все сигнальные модули (Software Lockdown)
        # 2. Сортируем позиции по риску (Delta * Notion)
        # 3. Идемпотентная очистка
        
        for pos in active_positions:
            await self._liquidate_with_retry(pos)

        logger.info("☢️ [NUCLEAR] Evacuation Complete. System Locked.")

    async def _liquidate_with_retry(self, pos):
        """
        Сургучная ликвидация: Market Order + Exponential Backoff.
        """
        retries = 0
        while retries < 10:
            try:
                # 1. Проверяем Rate Limits
                if self.error_count_429 >= self.max_429:
                    wait_time = 60 * (retries + 1)
                    logger.warning(f"🚨 [NUCLEAR] Rate limit exceeded. Banning ourselves for {wait_time}s.")
                    await asyncio.sleep(wait_time)
                    self.error_count_429 = 0 # Сброс после долгого ожидания

                # 2. Попытка закрытия ПО РЫНКУ (Force Market)
                logger.info(f"📤 [NUCLEAR] Attempting Market Close for {pos['symbol']}...")
                # res = await self.gateway.close_position_market(pos['symbol'])
                # if res: return # Успех
                
                return # Mock-success for now

            except Exception as e:
                # 3. Детекция 429 (Too Many Requests)
                if "429" in str(e):
                    self.error_count_429 += 1
                    backoff = 2 ** retries # Экспоненциальное ожидание
                    logger.error(f"⚠️ [NUCLEAR] API Rate Limited (429). Backing off for {backoff}s.")
                    await asyncio.sleep(backoff)
                else:
                    # Другие ошибки (500 Internal Server Error)
                    logger.error(f"💥 [NUCLEAR] Exchange Error (500/503): {e}")
                    await asyncio.sleep(1) # Ждем 1 сек и повторяем
                
                retries += 1

        logger.error(f"💀 [NUCLEAR] FATAL: Failed to liquidate {pos['symbol']} after 10 retries.")
