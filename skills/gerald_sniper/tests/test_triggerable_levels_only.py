from core.level_store import LevelStore, LevelState


def test_only_active_levels_are_triggerable():
    store = LevelStore(fresh_candles_required=3)
    store.add_or_merge_level("BTCUSDT", 100.0, "WEEK_EXTREME", 3, 60.0)

    assert len(store.get_triggerable_levels("BTCUSDT")) == 0

    for _ in range(3):
        store.update_symbol("BTCUSDT", current_price=100.1, atr_abs=1.0)

    triggerable = store.get_triggerable_levels("BTCUSDT")
    assert len(triggerable) == 1
    assert triggerable[0].state == LevelState.ACTIVE

    store.update_symbol("BTCUSDT", current_price=103.0, atr_abs=1.0)
    assert len(store.get_triggerable_levels("BTCUSDT")) == 0
