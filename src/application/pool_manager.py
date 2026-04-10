# src/application/pool_manager.py
import asyncio
from concurrent.futures import ProcessPoolExecutor
from loguru import logger

class WorkerPoolManager:
    """
    [GEKTOR v2.0] Orchestrates ProcessPool lifecycle.
    Prevents Memory Bloat via seamless Soft Recycle protocol.
    """
    def __init__(self, max_workers: int = 4, recycle_threshold_batches: int = 50_000):
        self.max_workers = max_workers
        self.recycle_threshold = recycle_threshold_batches
        self.batch_count = 0
        
        self.current_pool = ProcessPoolExecutor(max_workers=self.max_workers)
        self._lock = asyncio.Lock()

    async def execute_batch(self, target_function, *args):
        """Dispatches batch and monitors rotation threshold."""
        async with self._lock:
            self.batch_count += 1
            if self.batch_count >= self.recycle_threshold:
                await self._trigger_soft_recycle()
            
            pool = self.current_pool

        # Execute outside lock to maintain concurrency
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(pool, target_function, *args)

    async def _trigger_soft_recycle(self):
        """Seamlessly replaces worker pool."""
        logger.warning(f"🔄 [PoolManager] Memory cycle limit reached ({self.batch_count}). Initiating Soft Recycle...")
        
        old_pool = self.current_pool
        self.current_pool = ProcessPoolExecutor(max_workers=self.max_workers)
        self.batch_count = 0
        
        # Shutdown old pool in background to avoid blocking Event Loop
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, self._shutdown_old_pool, old_pool)
        
        logger.success("✅ [PoolManager] Process rotation complete. Fresh state established.")

    def _shutdown_old_pool(self, pool: ProcessPoolExecutor):
        pool.shutdown(wait=True)
        logger.debug("🧹 [PoolManager] Old processes terminated gracefully.")

    def shutdown_all(self):
        self.current_pool.shutdown(wait=True)
