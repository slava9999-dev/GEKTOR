import asyncio
import os
import sys
import psutil
import gc
import signal
from typing import List, Any, Optional
from loguru import logger # Standardizing on Loguru v12.0

def set_high_priority():
    """Sets the current process to High Priority (Windows/Unix)."""
    try:
        p = psutil.Process(os.getpid())
        if sys.platform == 'win32':
            p.nice(psutil.HIGH_PRIORITY_CLASS)
            logger.info("⚡ [Matrix] Process Priority: Set to HIGH (Windows).")
        else:
            p.nice(-10) # High priority on Unix
            logger.info("⚡ [Matrix] Process Priority: Set to HIGH (Unix -10).")
    except Exception as e:
        logger.warning(f"⚠️ [Matrix] Failed to set priority: {e}")

def set_cpu_affinity(cores: List[int]):
    """Pins the process to specific CPU cores for consistency."""
    try:
        p = psutil.Process(os.getpid())
        p.cpu_affinity(cores)
        logger.info(f"🎯 [Matrix] CPU_AFFINITY: Pinned to cores {cores}.")
    except Exception as e:
        logger.warning(f"⚠️ [Matrix] CPU_AFFINITY Fail: {e}")

class ResourceMonitor:
    """Generic resource utilization tracker."""
    def __init__(self):
        self._process = psutil.Process(os.getpid())

    def get_stats(self) -> dict:
        mem = self._process.memory_info()
        return {
            "ram_rss_mb": mem.rss / (1024 * 1024),
            "cpu_pct": self._process.cpu_percent(),
            "threads": self._process.num_threads()
        }

class GracefulKiller:
    """Handles SIGTERM/SIGINT for clean disposal."""
    def __init__(self, stop_event: asyncio.Event):
        self.stop_event = stop_event
        if sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                asyncio.get_event_loop().add_signal_handler(sig, self.exit_gracefully)

    def exit_gracefully(self):
        logger.warning("🛑 [Interrupt] Signal received. Initiating shutdown...")
        self.stop_event.set()

class VitalsWatchdog:
    """
    [GEKTOR v12.0] Systemic Health Sentinel.
    Monitors RAM usage and triggers recovery/shutdown on resource depletion.
    """
    def __init__(self, memory_limit_mb: int = 2048):
        self._limit_mb = memory_limit_mb
        self._process = psutil.Process(os.getpid())
        self._running = False
        
    async def run_forever(self, stop_event: asyncio.Event):
        self._running = True
        logger.info(f"🦷 [Vitals] Watchdog active. RAM Limit: {self._limit_mb}MB")
        
        while self._running:
            try:
                mem_info = self._process.memory_info()
                rss_mb = mem_info.rss / (1024 * 1024)
                
                if rss_mb > self._limit_mb * 0.75:
                    logger.warning(f"💊 [Vitals] Soft Memory Threshold (75%): {rss_mb:.1f}MB. Triggering GC...")
                    gc.collect()
                
                if rss_mb > self._limit_mb * 0.90:
                    logger.critical(f"💀 [Vitals] CRITICAL MEMORY: {rss_mb:.1f}MB > {self._limit_mb}MB limit. PANIC SHUTDOWN.")
                    stop_event.set()
                    break
                
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [Vitals] Monitoring error: {e}")
                await asyncio.sleep(5)

    def stop(self):
        self._running = False

async def shutdown_system(tasks: List[asyncio.Task], db: Any = None, rest: Any = None, ws: Any = None, cp_manager: Any = None, components_to_save: dict = None):
    """
    [GEKTOR v21.1] Orchestrated Shutdown with Kleppman Atomic Checkpointing.
    Ensures state is flushed and artifacts are persisted before process exit.
    """
    logger.info("⚡ [Shutdown] Initiating APEX cleanup...")
    
    # 0. Atomic Checkpointing (The Prado Core)
    if cp_manager and components_to_save:
        logger.info("💾 [Checkpoint] Persisting system artifacts...")
        checkpoint_tasks = []
        for name, component in components_to_save.items():
            if hasattr(component, 'get_state'):
                checkpoint_tasks.append(
                    cp_manager.save_checkpoint_async(name, component.get_state())
                )
        if checkpoint_tasks:
            await asyncio.gather(*checkpoint_tasks, return_exceptions=True)
            logger.success("💾 [Checkpoint] All artifacts persisted to disk.")

    # 1. Infrastructure Disconnect
    if ws:
        try: 
            if hasattr(ws, 'stop'): await ws.stop()
            elif hasattr(ws, 'close'): await ws.close()
            elif hasattr(ws, 'disconnect'): await ws.disconnect()
        except Exception as e: logger.error(f"❌ [Shutdown] WS Close Fail: {e}")
    
    # 2. Database Disconnect
    if db:
        try:
            if hasattr(db, 'dispose'): await db.dispose()
            elif hasattr(db, 'close'): await db.close()
            elif hasattr(db, 'stop'): await db.stop()
        except Exception as e: logger.error(f"❌ [Shutdown] DB Disconnect Fail: {e}")

    # 3. Task Cancellation
    valid_tasks = [t for t in tasks if t is not None]
    logger.info(f"🛑 [Shutdown] Cancelling {len(valid_tasks)} active tasks...")
    for t in valid_tasks: t.cancel()
    
    try:
        if valid_tasks:
            done, pending = await asyncio.wait(valid_tasks, timeout=5.0)
            if pending:
                logger.warning(f"⚠️ [Shutdown] {len(pending)} tasks FAILED TO CANCEL within 5s (Zombie state detected).")
    except Exception as e:
        logger.error(f"❌ [Shutdown] Cleanup failure: {e}")

    logger.success("🏁 [Shutdown] GEKTOR v12.0 CLEAN EXIT.")
