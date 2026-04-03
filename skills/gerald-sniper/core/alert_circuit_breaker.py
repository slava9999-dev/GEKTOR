import time
from collections import defaultdict

class AlertCircuitBreaker:
    """
    Trips if too many identical alerts appear in a short window.
    """
    def __init__(self, max_hits: int = 5, window_seconds: int = 60):
        self.max_hits = max_hits
        self.window_seconds = window_seconds
        self.events = defaultdict(list)

    def hit(self, fingerprint: str) -> bool:
        """
        Returns True if breaker should block this event.
        """
        now = time.time()
        bucket = self.events[fingerprint]
        bucket.append(now)

        fresh = [ts for ts in bucket if now - ts <= self.window_seconds]
        self.events[fingerprint] = fresh

        return len(fresh) > self.max_hits
