from core.safe_log import log_guard_event


class DummyLogger:
    def __init__(self):
        self.messages = []

    def warning(self, msg):
        self.messages.append(msg)


def test_safe_log_writes_message():
    logger = DummyLogger()
    log_guard_event(logger, "test_event", symbol="BTCUSDT", reason="x")
    assert len(logger.messages) == 1
    assert "[GUARD] test_event" in logger.messages[0]
