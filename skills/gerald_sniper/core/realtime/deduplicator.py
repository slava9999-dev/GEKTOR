import collections
import zlib
from typing import Set, Deque

class TickDeduplicator:
    """
    [GEKTOR v21.60] HFT-Optimized Idempotency Guard.
    Uses Adler32 hashing on normalized integer payloads for O(1) performance and low Jitter.
    """
    __slots__ = ('capacity', '_seen', '_queue')

    def __init__(self, capacity: int = 50_000):
        self.capacity = capacity
        self._seen: Set[int] = set()
        self._queue: Deque[int] = collections.deque()

    def is_duplicate(self, tick: dict) -> bool:
        """
        [GEKTOR v21.60] Hash-based Composite check.
        Normalizes float price/qty to fixed-point integers to avoid precision drift.
        """
        trade_id = tick.get('i', '')
        if not trade_id: return False
        
        # 1. Normalize and pack
        # Bybit price/qty are usually at most 8-10 decimals. Multiply by 10^8 for safety.
        try:
            p_int = int(tick.get('p', 0) * 1_000_000_00)
            q_int = int(tick.get('q', 0) * 1_000_000_00)
            ts = int(tick.get('E', 0))
            
            # 2. Hash composite key (Adler32 is extremely fast for HFT payloads)
            payload = f"{trade_id}{p_int}{q_int}{ts}".encode()
            h = zlib.adler32(payload)
                
            if h in self._seen:
                return True
                
            self._seen.add(h)
            self._queue.append(h)
            
            if len(self._queue) > self.capacity:
                self._seen.discard(self._queue.popleft())
                
            return False
        except (ValueError, TypeError):
            return False
