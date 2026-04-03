import pytest

from core.candle_finalizer import CandleFinalizer, FinalizedCandle

@pytest.mark.asyncio
async def test_partial_candle_does_not_reach_closed_handler():
    finalizer = CandleFinalizer()

    partial = FinalizedCandle(
        symbol="BTCUSDT",
        interval="5",
        start_ts=1000,
        end_ts=1299,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=100.0,
        turnover=150.0,
        confirmed=False,
        seq_id=1,
    )

    released = finalizer.ingest(partial)
    assert released is None
