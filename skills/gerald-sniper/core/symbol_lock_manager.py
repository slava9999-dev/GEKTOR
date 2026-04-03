import asyncio
from collections import defaultdict


class SymbolLockManager:
    def __init__(self):
        self._locks = defaultdict(asyncio.Lock)

    def get_lock(self, symbol: str) -> asyncio.Lock:
        return self._locks[symbol]
