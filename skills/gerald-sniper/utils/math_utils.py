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

def generate_sortable_id() -> str:
    """[GEKTOR v10.3] Time-Sorted ID (Timestamp + Random) for Redis ZSET scoring."""
    import time
    from uuid import uuid4
    # MS Precision + Random Hex (Total: 13 + 8 = 21 chars)
    return f"{int(time.time() * 1000)}_{uuid4().hex[:8]}"
