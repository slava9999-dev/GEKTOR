from core.candle_cache import CandleCache


class DummyCandle:
    def __init__(self, symbol, interval, start_ts, open_, high, low, close, volume):
        self.symbol = symbol
        self.interval = interval
        self.start_ts = start_ts
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume


def test_update_closed_candle_initializes_symbol_and_computes_atr():
    cache = CandleCache(None, None, None)
    symbol = "BTCUSDT"

    for i in range(20):
        c = DummyCandle(
            symbol=symbol,
            interval="5",
            start_ts=1000 + i * 300,
            open_=100 + i,
            high=101 + i,
            low=99 + i,
            close=100.5 + i,
            volume=1000 + i,
        )
        cache.update_closed_candle(c)

    assert symbol in cache.data
    atr_abs = cache.get_atr_abs(symbol, "5")
    assert atr_abs is not None
    assert atr_abs > 0
