import numpy as np
from multiprocessing import shared_memory
import ctypes
import os
from loguru import logger

class DoubleBufferedSharedOrderbook:
    """
    [GEKTOR v14.4.2] Lock-Free IPC for L2 Data.
    Provides atomic, double-buffered Top-50 snapshot sharing between I/O and Math processes.
    Zero-Copy access via numpy views.
    """
    BOOK_LEVELS = 50
    # Shape: [Side(2-bid/ask), Levels(50), Metrics(2-price/qty)]
    SIDE_SHAPE = (2, BOOK_LEVELS, 2)
    
    def __init__(self, symbol: str, is_writer: bool = True):
        self.symbol = symbol
        self.name = f"shm_l2_{symbol.lower()}"
        self.idx_name = f"{self.name}_idx"
        
        # Total size: 2 buffers * (2 sides * 50 levels * 2 metrics) * 8 bytes (float64)
        self.bytes_size = 2 * (2 * self.BOOK_LEVELS * 2) * 8
        self.is_writer = is_writer
        
        try:
            if is_writer:
                self.shm = shared_memory.SharedMemory(create=True, name=self.name, size=self.bytes_size)
                self.idx_shm = shared_memory.SharedMemory(create=True, name=self.idx_name, size=1)
                self.idx_shm.buf[0] = 0
            else:
                self.shm = shared_memory.SharedMemory(name=self.name)
                self.idx_shm = shared_memory.SharedMemory(name=self.idx_name)
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(name=self.name)
            self.idx_shm = shared_memory.SharedMemory(name=self.idx_name)
        except Exception as e:
            logger.error(f"❌ [SHM] Failed to init {self.name}: {e}")
            raise

        # Map SHM to a 4D numpy array: [Buffer_Index(2), Side(2), Level(50), Metric(2)]
        self.full_buffer = np.ndarray((2,) + self.SIDE_SHAPE, dtype=np.float64, buffer=self.shm.buf)
        
    def write(self, bids: np.ndarray, asks: np.ndarray):
        """Zero-latency write to the shadow buffer followed by an atomic index flip."""
        curr_idx = self.idx_shm.buf[0]
        next_idx = 1 - curr_idx
        
        # Direct view copy (No allocation)
        self.full_buffer[next_idx, 0, :, :] = bids[:self.BOOK_LEVELS, :]
        self.full_buffer[next_idx, 1, :, :] = asks[:self.BOOK_LEVELS, :]
        
        # Atomic Flip
        self.idx_shm.buf[0] = next_idx

    def read_into(self, out_array: np.ndarray):
        """Read the active buffer into a pre-allocated worker array (No allocation)."""
        curr_idx = self.idx_shm.buf[0]
        np.copyto(out_array, self.full_buffer[curr_idx])

    def cleanup(self):
        self.shm.close()
        self.idx_shm.close()
        if self.is_writer:
            try:
                self.shm.unlink()
                self.idx_shm.unlink()
            except:
                pass
