import asyncio
import time
import logging
from typing import Dict, Tuple

logger = logging.getLogger("GEKTOR.ESCROW")

class LiquidationEchoGuard:
    """
    [ФАЗА 5: ИНВАЛИДАЦИЯ И ЭСКАЛАЦИЯ]
    Асинхронный Эскроу-буфер для защиты от запаздывающих ликвидаций.
    """
    def __init__(self, escrow_window_ms: int = 150):
        self.escrow_window_sec = escrow_window_ms / 1000.0
        # Храним монотонное время последней ликвидации и её объем
        # Структура: symbol -> (monotonic_timestamp, side, usd_volume)
        self._recent_liquidations: Dict[str, Tuple[float, str, float]] = {}
        self._mute_threshold_usd = 50_000.0

    def register_liquidation_event(self, symbol: str, side: str, usd_volume: float):
        """Вызывается синхронно из WebSocket-ингестора ликвидаций"""
        if usd_volume < self._mute_threshold_usd:
            return
            
        logger.debug(f"🩸 [LIQUIDATION REGISTERED] {symbol} | {side} | ${usd_volume:,.0f}")
        self._recent_liquidations[symbol] = (time.monotonic(), side, usd_volume)

    async def escort_signal(self, symbol: str, signal_side: str, ofi_usd: float) -> bool:
        """
        Пропускает сигнал через временной карантин.
        Возвращает True, если сигнал чист, и False, если он убит ликвидацией.
        """
        # Неблокирующее ожидание, пока прилетят пакеты из смежного сокета
        await asyncio.sleep(self.escrow_window_sec)

        # Проверяем, не прилетел ли маржин-колл за время задержки
        if symbol in self._recent_liquidations:
            last_time, liq_side, liq_vol = self._recent_liquidations[symbol]
            time_since_liq = time.monotonic() - last_time

            # Если ликвидация была менее 1.5 секунд назад
            if time_since_liq < 1.5:
                # Маппинг: Ликвидация Шортистов (BUY) выглядит как ACCUMULATION (BUY)
                # Ликвидация Лонгистов (SELL) выглядит как DISTRIBUTION (SELL)
                if signal_side == liq_side:
                    logger.warning(
                        f"🛑 [SIGNAL ABORTED] {symbol} | {signal_side} | "
                        f"Опознан ликвидационный каскад (${liq_vol:,.0f}). Сигнал сожжен."
                    )
                    return False

        # Сигнал чист от ликвидаций — выпускаем в боевой канал
        return True
