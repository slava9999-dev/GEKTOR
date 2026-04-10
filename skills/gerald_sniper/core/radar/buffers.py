import numpy as np
from loguru import logger

class ZeroGCRingBuffer:
    """
    [GEKTOR v14.7.3] High-Performance Zero-GC Ring Buffer.
    Ensures zero heap allocations during high-frequency data ingestion.
    """
    __slots__ = ['size', 'data', 'index', 'count']
    
    def __init__(self, size: int, dtype=np.float64):
        self.size = size
        self.data = np.empty(size, dtype=dtype)
        self.data.fill(np.nan)
        self.index = 0
        self.count = 0

    def append(self, value: float) -> None:
        """O(1) insertion bypassing Python's GC tracking for data."""
        self.data[self.index] = value
        self.index = (self.index + 1) % self.size
        if self.count < self.size:
            self.count += 1

    def hard_reset(self) -> None:
        """Logical reset at C-level speed. Memory reuse over reallocation."""
        self.index = 0
        self.count = 0
        # Fast C-level fill
        self.data.fill(np.nan)

    def get_window(self) -> np.ndarray:
        """Returns the ordered view of the current buffer. Minimal overhead."""
        if self.count < self.size:
            return self.data[:self.count]
        return np.concatenate((self.data[self.index:], self.data[:self.index]))
