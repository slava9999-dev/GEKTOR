import time


class TriggerDeduplicator:
    """
    Prevent duplicate trigger emissions for the same candle/setup.
    """
    def __init__(self, ttl_seconds: int = 900):
        self.ttl_seconds = ttl_seconds
        self.seen: dict[str, float] = {}

    def _cleanup(self):
        now = time.time()
        expired = [k for k, ts in self.seen.items() if now - ts > self.ttl_seconds]
        for k in expired:
            del self.seen[k]

    def build_fingerprint(
        self,
        symbol: str,
        interval: str,
        candle_start_ts: int,
        trigger_type: str,
        level_price: float
    ) -> str:
        return f"{symbol}:{interval}:{candle_start_ts}:{trigger_type}:{round(level_price, 8)}"

    def is_duplicate(self, fingerprint: str) -> bool:
        self._cleanup()
        return fingerprint in self.seen

    def mark_seen(self, fingerprint: str):
        self._cleanup()
        self.seen[fingerprint] = time.time()
