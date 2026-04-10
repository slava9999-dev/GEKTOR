import asyncio
import pytest

from core.candle_finalizer import CandleFinalizer, FinalizedCandle
from core.trigger_dedup import TriggerDeduplicator


class DummyQueue:
    def __init__(self):
        self.items = []

    async def put(self, item):
        self.items.append(item)


class DummyTriggerDetector:
    def process_closed_candle(self, symbol, candle):
        return [{
            "symbol": symbol,
            "interval": candle.interval,
            "candle_start_ts": candle.start_ts,
            "trigger_type": "BREAKOUT",
            "level": candle.close,
        }]


@pytest.mark.asyncio
async def test_final_candle_enqueues_once():
    queue = DummyQueue()
    dedup = TriggerDeduplicator()

    candle = FinalizedCandle(
        symbol="BTCUSDT",
        interval="5",
        start_ts=1000,
        end_ts=1299,
        open=1,
        high=2,
        low=0.5,
        close=1.5,
        volume=100,
        turnover=150,
        confirmed=True,
        seq_id=2,
    )

    detector = DummyTriggerDetector()

    triggers = detector.process_closed_candle(candle.symbol, candle)
    for trigger in triggers:
        fp = dedup.build_fingerprint(
            symbol=trigger["symbol"],
            interval=trigger["interval"],
            candle_start_ts=trigger["candle_start_ts"],
            trigger_type=trigger["trigger_type"],
            level_price=trigger["level"],
        )
        if not dedup.is_duplicate(fp):
            dedup.mark_seen(fp)
            await queue.put(trigger)

    # duplicate final
    triggers = detector.process_closed_candle(candle.symbol, candle)
    for trigger in triggers:
        fp = dedup.build_fingerprint(
            symbol=trigger["symbol"],
            interval=trigger["interval"],
            candle_start_ts=trigger["candle_start_ts"],
            trigger_type=trigger["trigger_type"],
            level_price=trigger["level"],
        )
        if not dedup.is_duplicate(fp):
            dedup.mark_seen(fp)
            await queue.put(trigger)

    assert len(queue.items) == 1
