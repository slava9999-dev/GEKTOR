import logging
from dataclasses import dataclass
from typing import Optional, List

logger = logging.getLogger("GEKTOR.QuantEngine")

@dataclass(slots=True)
class DollarBar:
    symbol: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume_base: float
    volume_quote: float  # Это наш $ объем
    buy_volume_quote: float
    sell_volume_quote: float
    tick_count: int
    start_timestamp: float
    end_timestamp: float

class FractionalDollarBarGenerator:
    """
    [GEKTOR v2.0] Инкрементальный компрессор хронологического времени в Событийное Время 
    с поддержкой Осколочного Шардирования (Fractional Splintering) макро-тиков.
    """
    __slots__ = ['symbol', 'threshold_usd', '_current_bar']

    def __init__(self, symbol: str, threshold_usd: float):
        self.symbol = symbol
        self.threshold_usd = threshold_usd
        self._current_bar: Optional[DollarBar] = None

    def process_tick(self, price: float, volume: float, is_buyer_maker: bool, ts: float) -> List[DollarBar]:
        """
        O(1) обработка каждого тика с динамическим шардированием Китовых ордеров.
        Возвращает список завершенных Долларовых Корзин (от 0 до N).
        """
        volume_usd = price * volume
        completed_bars = []
        
        while volume_usd > 0:
            if not self._current_bar:
                self._current_bar = DollarBar(
                    symbol=self.symbol,
                    open_price=price, high_price=price, low_price=price, close_price=price,
                    volume_base=0.0, volume_quote=0.0,
                    buy_volume_quote=0.0, sell_volume_quote=0.0,
                    tick_count=0, start_timestamp=ts, end_timestamp=ts
                )
                
            available_capacity = self.threshold_usd - self._current_bar.volume_quote
            
            # Если тик полностью помещается в корзину
            if volume_usd < available_capacity:
                self._fill_bucket(price, volume_usd, is_buyer_maker, ts)
                volume_usd = 0.0 # Тик исчерпан
            else:
                # Тик больше или равен остатку корзины (Сплиттинг)
                self._fill_bucket(price, available_capacity, is_buyer_maker, ts)
                completed_bars.append(self._current_bar)
                self._current_bar = None
                
                volume_usd -= available_capacity

        return completed_bars

    def _fill_bucket(self, price: float, volume_usd_chunk: float, is_buyer_maker: bool, ts: float):
        b = self._current_bar
        b.high_price = price if price > b.high_price else b.high_price
        b.low_price = price if price < b.low_price else b.low_price
        b.close_price = price
        
        volume_base_chunk = volume_usd_chunk / price
        b.volume_base += volume_base_chunk
        b.volume_quote += volume_usd_chunk
        
        if is_buyer_maker:
            b.sell_volume_quote += volume_usd_chunk
        else:
             b.buy_volume_quote += volume_usd_chunk
             
        b.tick_count += 1
        b.end_timestamp = ts
