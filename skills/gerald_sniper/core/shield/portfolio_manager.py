import asyncio
import time
from dataclasses import dataclass
from loguru import logger
from typing import Dict, Any, Optional

@dataclass
class Reservation:
    amount: float
    timestamp: float

class PortfolioManager:
    """
    Теневой Учет (Shadow Accounting) & Risk Manager.
    Реализует паттерн Split Ledger с защитой от гонок данных и утечек маржи (TTL GC).
    """
    def __init__(self, initial_equity: float, max_account_risk_pct: float = 0.05, reservation_ttl_sec: float = 5.0):
        # 1. Source of Truth (Обновляется только из WS wallet)
        self._confirmed_balance: float = initial_equity
        
        # 2. Локальные блокировки маржи (Ключ: event_id / order_link_id)
        self._pending_reservations: Dict[str, Reservation] = {}
        
        self._max_risk_pct = max_account_risk_pct
        self._reservation_ttl = reservation_ttl_sec
        self._margin_lock = asyncio.Lock()
        
        # Запуск Сборщика Мусора (Garbage Collector)
        self._gc_task = asyncio.create_task(self._garbage_collector_loop())

    @property
    def effective_margin(self) -> float:
        """3. Динамически вычисляемый доступный капитал."""
        locked = sum(res.amount for res in self._pending_reservations.values())
        return self._confirmed_balance - locked

    async def allocate_and_size_position(
        self, 
        event: Any, # SignalEvent
        entry_price: float, 
        stop_loss_price: float
    ) -> Optional[Any]: # Возвращает обогащенный сигнал
        """
        [HOT PATH] Атомарный расчет объема и создание резервации под Lock'ом.
        """
        symbol = getattr(event, 'symbol', 'UNKNOWN')
        event_id = getattr(event, 'event_id', str(time.time()))
        
        if entry_price <= 0 or stop_loss_price <= 0 or entry_price == stop_loss_price:
            logger.error(f"🛑 [PORTFOLIO] Невалидные цены {symbol}. Сигнал сожжен.")
            return None

        # Оценка риска в %
        price_risk_pct = max(abs(entry_price - stop_loss_price) / entry_price, 0.001)

        async with self._margin_lock:
            # Расчет на базе Effective Margin!
            current_effective = self.effective_margin
            max_risk_usd = current_effective * self._max_risk_pct
            
            target_qty = max_risk_usd / (entry_price * price_risk_pct)
            required_margin = target_qty * entry_price

            if required_margin > current_effective:
                logger.warning(
                    f"⚠️ [PORTFOLIO] Insufficient Margin ({symbol}). "
                    f"Req: ${required_margin:.2f}, Eff: ${current_effective:.2f}"
                )
                return None 

            # Атомарная заморозка (Создание записи в Леджере)
            self._pending_reservations[event_id] = Reservation(
                amount=required_margin,
                timestamp=time.time()
            )
            
            logger.info(f"💼 [PORTFOLIO] Заблокировано ${required_margin:.2f} под сигнал [{event_id}].")
            
            # Обогащение сигнала рассчитанным объемом
            event.qty = target_qty
            return event

    async def hard_sync_from_exchange(self, real_wallet_balance: float) -> None:
        """
        [COLD PATH] Обновление Source of Truth из WS 'wallet'.
        """
        async with self._margin_lock:
            drift = real_wallet_balance - self._confirmed_balance
            self._confirmed_balance = real_wallet_balance
            
            if abs(drift) > 0.5:
                # Effective margin автоматически пересчитается при следующем вызове
                logger.debug(f"🔄 [PORTFOLIO] Стейт обновлен. Сдвиг: {drift:+.2f} USD. Source of Truth: ${self._confirmed_balance:.2f}")

    async def release_reservation(self, event_id: str) -> None:
        """
        Снятие блокировки. Вызывается по факту получения WS 'execution' или 'order'.
        """
        async with self._margin_lock:
            if event_id in self._pending_reservations:
                res = self._pending_reservations.pop(event_id)
                logger.debug(f"🔓 [PORTFOLIO] Заморозка {res.amount:.2f}$ снята для [{event_id}].")

    async def _garbage_collector_loop(self) -> None:
        """
        [BACKGROUND] Защита от потери WS пакетов и "Утечек Маржи".
        Сканирует словарь каждые 2 секунды.
        """
        while True:
            await asyncio.sleep(2.0)
            now = time.time()
            orphaned_ids = []

            async with self._margin_lock:
                for eid, res in list(self._pending_reservations.items()):
                    if (now - res.timestamp) > self._reservation_ttl:
                        orphaned_ids.append((eid, res.amount))
                        del self._pending_reservations[eid]

            for eid, amt in orphaned_ids:
                logger.critical(
                    f"⚠️ [PORTFOLIO] Сборщик мусора обнаружил Orphaned Reservation [{eid}] на ${amt:.2f}! "
                    f"WS пакет утерян. Требуется сверка StateReconciler (REST API)!"
                )
                # TODO: Опубликовать событие в шину для StateReconciler, 
                # чтобы он сходил по REST и проверил фактический статус ордера.
