import time

class StateWatchdog:
    def __init__(self, stale_after_seconds: int = 900):
        self.stale_after_seconds = stale_after_seconds
        self.last_update = {}

    def mark_updated(self, symbol: str):
        self.last_update[symbol] = time.time()

    def is_stale(self, symbol: str) -> bool:
        ts = self.last_update.get(symbol)
        if ts is None:
            return False
        return (time.time() - ts) > self.stale_after_seconds
