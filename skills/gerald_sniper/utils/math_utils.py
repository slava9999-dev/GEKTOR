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

import numpy as np

class RobustMath:
    """[GEKTOR v12.7] Robust Statistics Engine (Outlier-resistant Z-Score)"""
    def __init__(self):
        pass

    def get_z_score_robust(self, current_val: float, history: np.ndarray) -> float:
        if len(history) < 2:
            return 0.0
        # Median Absolute Deviation (MAD)
        median = np.median(history)
        mad = np.median(np.abs(history - median))
        if mad == 0:
            std = np.std(history)
            if std == 0:
                return 0.0
            return float((current_val - np.mean(history)) / std)
        robust_std = mad * 1.4826
        return float((current_val - median) / robust_std)

