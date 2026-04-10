# core/realtime/sequence_guard.py
import asyncio
import collections
from loguru import logger
from typing import Dict, List, Optional, Any, Tuple

class SequenceViolationError(Exception):
    """Выбрасывается при византийском сбое последовательности."""
    pass

class OrderBookState:
    """ [STATE] Replicated State Machine - Container for strict L2 consistency. """
    def __init__(self):
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: int = -1
        self.is_recovering: bool = True
        self._recovery_buffer: collections.deque = collections.deque(maxlen=2000)

    def halt_and_catch_fire(self):
        """Ритуальное самоубийство стейта при обнаружении яда."""
        logger.critical("☢️ [STATE POISONED] Initiating Halt and Catch Fire. Purging memory.")
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = -1
        self.is_recovering = True
        self._recovery_buffer.clear()

class SequenceGuard:
    """
    [BEAZLEY-KLEPPMAN v21.1] Non-blocking Sequence Integrity Guard.
    Ensures Causal Consistency and provides fail-fast recovery for HFT streams.
    """
    def __init__(self, rest_client, l2_engine):
        self.state = OrderBookState()
        self.rest_client = rest_client
        self.l2_engine = l2_engine # Integration with HFT Quant Engine
        self._lock = asyncio.Lock()

    async def process_delta(self, delta: dict):
        """
        Main entry point for WebSocket deltas. 
        Implements the 'Fail-Fast' pattern for temporal anomalies.
        """
        current_u = delta.get('u')   # Current update_id (u)
        prev_u = delta.get('pu')     # Previous update_id (pu - Bybit/Binance format)

        if self.state.is_recovering:
            # Ожидаем рекалибровку, буферизируем входящий поток
            self.state._recovery_buffer.append(delta)
            return

        try:
            # [STRICT CAUSAL CONSISTENCY CHECK]
            if self.state.last_update_id != -1:
                # 1. Запрет на retrograde updates (путешествие во времени назад)
                if current_u <= self.state.last_update_id:
                    raise SequenceViolationError(
                        f"Retrograde delta: {current_u} <= {self.state.last_update_id}. "
                        "Possible gateway causal breach."
                    )
                
                # 2. Проверка на разрыв последовательности (пустота в матрице)
                if prev_u is not None and prev_u != self.state.last_update_id:
                    raise SequenceViolationError(
                        f"Gap detected: Expected {self.state.last_update_id}, got pu={prev_u}. "
                        "State consistency compromised."
                    )

            # Apply to both local guard state and the heavy L2 Engine
            self._apply_to_internal_state(delta)
            
            # Sync with the main L2 liquidity engine for signals
            self.l2_engine.apply_delta(delta.get('b', []), delta.get('a', []))
            
            self.state.last_update_id = current_u

        except SequenceViolationError as e:
            logger.error(f"💀 [BYZANTINE FAULT] {e}")
            self.state.halt_and_catch_fire()
            # Инженерный стандарт: Фоновое воскрешение без блокировки Event Loop
            asyncio.create_task(self._resync_state())

    def _apply_to_internal_state(self, delta: dict):
        """ Атомарное обновление уровней в контейнере охраны. """
        for p, s in delta.get('b', []):
            if float(s) == 0: self.state.bids.pop(float(p), None)
            else: self.state.bids[float(p)] = float(s)
            
        for p, s in delta.get('a', []):
            if float(s) == 0: self.state.asks.pop(float(p), None)
            else: self.state.asks[float(p)] = float(s)

    async def _resync_state(self):
        """
        Автономное воскрешение стейта через REST-снимок.
        Реализует паттерн 'Play Forward' для буферизованных дельт.
        """
        async with self._lock:
            try:
                logger.info("📡 [RESYNC] Fetching authoritative REST snapshot from exchange...")
                # Предполагаем, что rest_client возвращает унифицированный снепшот
                snapshot = await self.rest_client.fetch_orderbook_snapshot()
                
                self.state.bids = {float(p): float(v) for p, v in snapshot['bids']}
                self.state.asks = {float(p): float(v) for p, v in snapshot['asks']}
                self.state.last_update_id = snapshot['lastUpdateId']
                
                # Обновляем HFT Engine
                self.l2_engine.apply_delta(snapshot['bids'], snapshot['asks'])

                logger.info(f"✨ [STITCH] Replaying {len(self.state._recovery_buffer)} buffered deltas...")
                
                # Воспроизводим (Play forward) валидные дельты из буфера
                while self.state._recovery_buffer:
                    buffered_delta = self.state._recovery_buffer.popleft()
                    u = buffered_delta['u']
                    pu = buffered_delta.get('pu')

                    # 1. Игнорируем дельты, которые уже покрыты снепшотом
                    if u <= self.state.last_update_id:
                        continue 
                    
                    # 2. Жесткая сшивка: pu должен соответствовать абсолютному хвосту стейта
                    if pu is not None and pu == self.state.last_update_id:
                        self._apply_to_internal_state(buffered_delta)
                        self.l2_engine.apply_delta(buffered_delta.get('b', []), buffered_delta.get('a', []))
                        self.state.last_update_id = u
                    else:
                        # Буфер разорван или не сшивается - рекурсивная рекалибровка
                        raise SequenceViolationError("Recovery buffer discontinuity. Critical drift.")

                self.state.is_recovering = False
                logger.success("✅ [RESURRECTED] OrderBook state successfully synchronized and LIVE.")
                
            except Exception as e:
                logger.error(f"❌ [RESYNC_FAILED] {e}. Retrying recovery in 2000ms...")
                await asyncio.sleep(2.0)
                asyncio.create_task(self._resync_state())
