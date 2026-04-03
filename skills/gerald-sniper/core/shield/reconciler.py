import asyncio
from loguru import logger
from typing import Any

class StateReconciler:
    """
    Ликвидатор Временных Парадоксов.
    Автоматически восстанавливает консистентность стейта после разрывов сети.
    Включает в себя Предохранитель Карантина (Quarantine Breaker) для защиты от Flash Crashes.
    """
    def __init__(
        self, 
        dispatcher: Any, # APIDispatcher
        portfolio: Any,  # PortfolioManager
        kill_switch: Any # KillSwitch
    ):
        self.dispatcher = dispatcher
        self.portfolio = portfolio
        self.kill_switch = kill_switch
        
        # Порог Паники: % от эквити, при котором мы прекращаем точечный реконсайл 
        # и сбрасываем ядерную бомбу Cancel All (например, 20% маржи в зависшем состоянии).
        self.quarantine_panic_threshold_pct = 0.20 

    async def handle_orphaned_order(self, event: Any) -> None:
        """Точка входа из Шины при срабатывании Garbage Collector'а в PortfolioManager."""
        client_oid = getattr(event, 'client_order_id', 'UNKNOWN')
        quarantined_amount = getattr(event, 'amount_usd', 0.0)
        
        # 1. Проверка Порога Паники (Quarantine Breaker)
        # Если сумма всех зависших ордеров превышает лимит, мы переходим от хирургии к ампутации.
        total_quarantined_usd = self.portfolio.get_total_quarantined_usd()
        current_equity = self.portfolio._confirmed_balance
        
        if current_equity > 0 and (total_quarantined_usd / current_equity) >= self.quarantine_panic_threshold_pct:
            logger.critical(
                f"🚨 [QUARANTINE BREAKER] Заморожено ${(total_quarantined_usd):.2f} (>{self.quarantine_panic_threshold_pct*100}% эквити). "
                f"API Bybit недоступен или стагнрует. Активация PANIC PROTOCOL."
            )
            await self._execute_panic_protocol()
            return

        logger.warning(f"🔍 [RECONCILER] Инициация расследования по сироте: {client_oid}")

        try:
            # Хирургический REST-опрос (P1_HIGH приоритет, так как это восстановление стейта)
            response = await self.dispatcher.execute_with_qos(
                method="GET",
                endpoint="/v5/order/realtime",
                endpoint_type="order",
                priority=1, 
                payload={"category": "linear", "orderLinkId": client_oid}
            )

            result = response.get("result", {})
            order_list = result.get("list", [])

            if not order_list:
                # Биржа не видела ордер. Он умер в TCP/TLS канале до Matching Engine.
                logger.info(f"✅ [RECONCILER] Ордер {client_oid} не найден (Lost in Transit). Снимаем Карантин.")
                await self.portfolio.release_reservation(client_oid)
                return

            order_status = order_list[0].get("orderStatus")

            if order_status in ["Filled", "PartiallyFilled"]:
                logger.critical(f"⚠️ [RECONCILER] Сирота {client_oid} ИСПОЛНЕН на бирже! Теневой учет спас депозит.")
                await self.portfolio.release_reservation(client_oid)
                # TODO: Кинуть эвент ToxicPositionCheck (сравнить с альфа-моделью).
                
            elif order_status in ["Cancelled", "Rejected", "Deactivated"]:
                logger.info(f"✅ [RECONCILER] Ордер {client_oid} отменен биржей. Восстанавливаем маржу.")
                await self.portfolio.release_reservation(client_oid)

            else:
                # Untriggered, New, Created. Ордер жив, висит в книге заявок, но мы потеряли его след по WS.
                logger.error(f"☢️ [RECONCILER] Ордер {client_oid} жив в стакане (Статус: {order_status}).")
                await self._assassinate_live_orphan(client_oid, order_list[0].get("symbol"))

        except Exception as e:
            logger.error(f"🔥 [RECONCILER] Сбой расследования {client_oid}: {e}. Остается в Карантине.")
            # Сообщение не получит XACK и будет перепроверено после Backoff.

    async def _assassinate_live_orphan(self, client_oid: str, symbol: str) -> None:
        """P0 (CRIT) отмена зависшего ордера."""
        logger.warning(f"🔪 [RECONCILER] Попытка убийства ордера {client_oid}...")
        try:
            await self.dispatcher.execute_with_qos(
                method="POST",
                endpoint="/v5/order/cancel",
                endpoint_type="order",
                priority=0, # P0_CRIT
                payload={"category": "linear", "symbol": symbol, "orderLinkId": client_oid}
            )
            # При успехе PortfolioManager уберет резервацию при получении REST 200 OK или WS Cancel.
        except Exception:
            logger.critical(f"FATAL: Не удалось убить {client_oid}. Инициирован ZERO-DAY HALT.")
            await self.kill_switch.engage(reason="ORPHAN_ASSASSINATION_FAILED", trigger_id=client_oid)

    async def _execute_panic_protocol(self) -> None:
        """
        Протокол Судного Дня. Вызывается при раздувании Карантинной Зоны (отвал биржи).
        """
        if await self.kill_switch.is_halted():
            return # Уже в панике

        # 1. Блокируем весь новый поток I/O
        await self.kill_switch.engage(reason="QUARANTINE_BREAKER_OVERFLOW", trigger_id="StateReconciler")
        
        logger.critical("☢️☢️☢️ [PANIC PROTOCOL] Отправка CANCEL ALL ACTIVE ORDERS! ☢️☢️☢️")
        
        # 2. Одна ядерная ракета (Удаляем все заявки по аккаунту)
        try:
            await self.dispatcher.execute_with_qos(
                method="POST",
                endpoint="/v5/order/cancel-all",
                endpoint_type="order",
                priority=0, # Наивысший
                payload={"category": "linear"}
            )
            logger.critical("✅ [PANIC PROTOCOL] Cancel-All успешно отправлен.")
            
            # 3. ТОКСИЧНЫЙ ЛИКВИДАТОР (Toxic Liquidator) 
            # Cancel-all не закрывает уже исполненные рыночные/лимитные позиции.
            logger.warning("☣️ [TOXIC LIQUIDATOR] Запрос актуальных позиций для ликвидации...")
            pos_response = await self.dispatcher.execute_with_qos(
                method="GET",
                endpoint="/v5/position/realtime",
                endpoint_type="position",
                priority=0,
                payload={"category": "linear", "settleCoin": "USDT"}
            )
            
            open_positions = [
                p for p in pos_response.get("result", {}).get("list", [])
                if float(p.get("size", 0)) > 0
            ]
            
            for pos in open_positions:
                symbol = pos.get("symbol")
                side = pos.get("side")
                size = pos.get("size")
                logger.critical(f"☣️ [TOXIC LIQUIDATOR] ОБНАРУЖЕНА НЕУЧТЕННАЯ ПОЗИЦИЯ: {symbol} {side} Size: {size}. ЛИКВИДАЦИЯ!")
                
                # Market close for the exact size
                close_side = "Sell" if side == "Buy" else "Buy"
                await self.dispatcher.execute_with_qos(
                    method="POST",
                    endpoint="/v5/order/create",
                    endpoint_type="order",
                    priority=0,
                    payload={
                        "category": "linear",
                        "symbol": symbol,
                        "side": close_side,
                        "orderType": "Market",
                        "qty": str(size),
                        "reduceOnly": True,
                        "timeInForce": "IOC",
                        "positionIdx": 0 # One-way mode. Change if using hedge mode (1 or 2)
                    }
                )
                logger.critical(f"✅ [TOXIC LIQUIDATOR] Приказ на ликвидацию {symbol} отправлен.")

            # 4. Только теперь безопасно чистим локальный леджер
            await self.portfolio.force_clear_all_reservations()
            logger.info("🛡️ [PANIC PROTOCOL] Стейт очищен. Система в безопасном режиме HALT.")
            
        except Exception as e:
            logger.critical(f"FATAL ☠️: CANCEL-ALL ИЛИ ЛИКВИДАЦИЯ ПРОВАЛЕНА. Ручное вмешательство необходимо: {e}")
