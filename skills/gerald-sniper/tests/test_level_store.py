from core.level_store import LevelStore, LevelState


def test_add_level():
    store = LevelStore()
    store.add_or_merge_level("BTCUSDT", 100.0, "ROUND_NUMBER", 3, 55.0)
    levels = store.get_all_levels("BTCUSDT")
    assert len(levels) == 1
    assert levels[0].price == 100.0


def test_merge_level_within_tolerance():
    store = LevelStore(merge_tolerance_pct=0.01)
    store.add_or_merge_level("BTCUSDT", 100.0, "ROUND_NUMBER", 2, 50.0)
    store.add_or_merge_level("BTCUSDT", 100.5, "WEEK_EXTREME", 5, 70.0)
    levels = store.get_all_levels("BTCUSDT")
    assert len(levels) == 1
    assert levels[0].touches == 5
    assert levels[0].score == 70.0


def test_fresh_to_active_transition():
    store = LevelStore(fresh_candles_required=3)
    store.add_or_merge_level("BTCUSDT", 100.0, "WEEK_EXTREME", 3, 60.0)

    store.update_symbol("BTCUSDT", current_price=100.1, atr_abs=1.0)
    assert store.get_all_levels("BTCUSDT")[0].state == LevelState.FRESH

    store.update_symbol("BTCUSDT", current_price=100.1, atr_abs=1.0)
    assert store.get_all_levels("BTCUSDT")[0].state == LevelState.FRESH

    store.update_symbol("BTCUSDT", current_price=100.1, atr_abs=1.0)
    assert store.get_all_levels("BTCUSDT")[0].state == LevelState.ACTIVE


def test_level_becomes_stale():
    store = LevelStore()
    store.add_or_merge_level("BTCUSDT", 100.0, "WEEK_EXTREME", 3, 60.0)

    for _ in range(3):
        store.update_symbol("BTCUSDT", current_price=100.0, atr_abs=1.0)

    level = store.get_all_levels("BTCUSDT")[0]
    assert level.state == LevelState.ACTIVE

    store.update_symbol("BTCUSDT", current_price=103.0, atr_abs=1.0)
    level = store.get_all_levels("BTCUSDT")[0]
    assert level.state == LevelState.STALE


def test_level_becomes_dead_and_removed():
    store = LevelStore()
    store.add_or_merge_level("BTCUSDT", 100.0, "WEEK_EXTREME", 3, 60.0)
    store.update_symbol("BTCUSDT", current_price=106.0, atr_abs=1.0)
    levels = store.get_all_levels("BTCUSDT")
    assert len(levels) == 0
