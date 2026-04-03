import time
from loguru import logger

EPS = 1e-9

def safe_float(value):
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0

class LogThrottler:
    """Prevents log spamming by tracking cooldowns per key."""
    def __init__(self):
        self._last_logs = {}

    def should_log(self, key: str, cooldown: int = 30) -> bool:
        now = time.time()
        if key not in self._last_logs or (now - self._last_logs[key]) > cooldown:
            self._last_logs[key] = now
            return True
        return False

log_throttler = LogThrottler()
