from core.level_store import StoredLevel
from core.trigger_pipeline import TriggerPipeline


def test_normalize_level_dataclass_and_dict():
    tp = TriggerPipeline.__new__(TriggerPipeline)

    dc = StoredLevel(
        symbol="BTCUSDT",
        price=100.0,
        source="WEEK_EXTREME",
        touches=3,
        score=55.0,
    )
    d = {
        "price": 100.0,
        "source": "WEEK_EXTREME",
        "touches": 3,
        "score": 55.0,
    }

    n1 = tp._normalize_level(dc, 99.0)
    n2 = tp._normalize_level(d, 99.0)

    assert n1 == n2
    assert n1["type"] == "RESISTANCE"
