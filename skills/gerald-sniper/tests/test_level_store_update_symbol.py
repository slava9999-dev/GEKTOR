from core.level_store import LevelStore, LevelState


def test_update_symbol_invalid_atr_makes_level_stale():
    store = LevelStore()
    store.add_or_merge_level("BTCUSDT", 100.0, "WEEK_EXTREME", 3, 60.0)
    store.update_symbol("BTCUSDT", current_price=100.0, atr_abs=0.0)
    level = store.get_all_levels("BTCUSDT")[0]
    assert level.state == LevelState.STALE
