from core.candle_finalizer import CandleFinalizer, FinalizedCandle

def make_candle(seq_id: int, confirmed: bool):
    return FinalizedCandle(
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
        confirmed=confirmed,
        seq_id=seq_id,
    )

def test_partial_does_not_release():
    f = CandleFinalizer()
    result = f.ingest(make_candle(seq_id=1, confirmed=False))
    assert result is None

def test_final_releases_once():
    f = CandleFinalizer()
    assert f.ingest(make_candle(seq_id=1, confirmed=False)) is None
    final = f.ingest(make_candle(seq_id=2, confirmed=True))
    assert final is not None
    duplicate = f.ingest(make_candle(seq_id=2, confirmed=True))
    assert duplicate is None

def test_out_of_order_is_ignored():
    f = CandleFinalizer()
    assert f.ingest(make_candle(seq_id=2, confirmed=False)) is None
    result = f.ingest(make_candle(seq_id=1, confirmed=False))
    assert result is None
    final = f.ingest(make_candle(seq_id=3, confirmed=True))
    assert final is not None
