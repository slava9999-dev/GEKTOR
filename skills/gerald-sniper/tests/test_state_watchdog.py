import time
from core.state_watchdog import StateWatchdog


def test_watchdog_not_stale_immediately():
    wd = StateWatchdog(stale_after_seconds=10)
    wd.mark_updated("BTCUSDT")
    assert wd.is_stale("BTCUSDT") is False


def test_watchdog_stale_after_timeout():
    wd = StateWatchdog(stale_after_seconds=1)
    wd.mark_updated("BTCUSDT")
    time.sleep(1.1)
    assert wd.is_stale("BTCUSDT") is True
