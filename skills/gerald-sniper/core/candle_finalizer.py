from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class FinalizedCandle:
    symbol: str
    interval: str
    start_ts: int
    end_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    confirmed: bool
    seq_id: int


class CandleFinalizer:
    """
    Accepts candle updates from WS and releases ONLY final confirmed candles.
    Also protects against:
    - duplicate final packets
    - out-of-order packets
    - stale partial packets
    """
    def __init__(self):
        self.latest_partial: Dict[Tuple[str, str, int], FinalizedCandle] = {}
        self.processed_final: set[Tuple[str, str, int]] = set()

    def ingest(self, candle: FinalizedCandle) -> Optional[FinalizedCandle]:
        key = (candle.symbol, candle.interval, candle.start_ts)

        # Ignore duplicate already-processed final candles
        if key in self.processed_final:
            return None

        existing = self.latest_partial.get(key)

        # Ignore stale / out-of-order packet
        if existing is not None and candle.seq_id < existing.seq_id:
            return None

        # Update latest packet for this candle
        self.latest_partial[key] = candle

        # Release only confirmed final candle
        if candle.confirmed:
            self.processed_final.add(key)
            self.latest_partial.pop(key, None)
            return candle

        return None
