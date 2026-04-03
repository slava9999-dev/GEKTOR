from core.trigger_dedup import TriggerDeduplicator

def test_fingerprint_duplicate():
    d = TriggerDeduplicator()
    fp = d.build_fingerprint("BTCUSDT", "5", 1000, "BREAKOUT", 60000.0)
    assert d.is_duplicate(fp) is False
    d.mark_seen(fp)
    assert d.is_duplicate(fp) is True
