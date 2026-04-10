# skills/gerald-sniper/core/sentinel/tamper_guard.py
import asyncio
import hashlib
import os
import logging
from pathlib import Path
from loguru import logger

class SecurityCompromisedException(Exception):
    """
    [GEKTOR v21.29] Tamper Protection triggered.
    Thrown when production source code is modified in-flight, 
    bypassing the CI/CD pipeline.
    """
    pass

class TamperGuard:
    """
    [GEKTOR v21.29] Immutable Infrastructure Sentinel.
    Periodic integrity check of the Python codebase to prevent 'Hotfixes' by Operator.
    - Purpose: Protects Architectural Consistency from human panic/greed.
    """
    def __init__(self, src_directory: str):
        self.src_dir = Path(src_directory)
        logger.info(f"🛡️ [TAMPER] Initializing Source Integrity Scan: {self.src_dir}")
        self.baseline_hashes = self._compute_hashes()
        logger.success(f"🛡️ [TAMPER] Baseline Secured. {len(self.baseline_hashes)} files locked.")

    def _compute_hashes(self) -> dict:
        """Sequential SHA256 Scan of all .py modules."""
        hashes = {}
        for filepath in self.src_dir.rglob('*.py'):
            try:
                with open(filepath, 'rb') as f:
                    file_hash = hashlib.sha256(f.read()).hexdigest()
                    hashes[str(filepath)] = file_hash
            except Exception as e:
                logger.error(f"⚠️ [TAMPER] Failed to hash {filepath}: {e}")
        return hashes

    async def watch_loop(self, interval_seconds: int = 60):
        """Infinite Watchdog. Triggers 'Halt & Catch Fire' on any code deviation."""
        logger.info(f"⚙️ [TAMPER] Watchdog active (Interval: {interval_seconds}s).")
        while True:
            await asyncio.sleep(interval_seconds)
            current_hashes = self._compute_hashes()
            
            for filepath, original_hash in self.baseline_hashes.items():
                current_hash = current_hashes.get(filepath)
                if current_hash != original_hash:
                    logger.critical(
                        f"🛑 [BREAD-IN DETECTED] Production Vandalism at: {filepath}. "
                        f"Immutability Violation!"
                    )
                    await self._execute_lockdown()
                    
    async def _execute_lockdown(self):
        """Ritual Suicide Protocol: Shutdown ensuring zero corrupted state updates."""
        logger.critical("🚨 [SECURITY] Execution context compromised. Terminating event loop.")
        # Atomic Cancellation of all running market tasks
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        
        # Raise fatal error to trigger systemd Restart=always (which will fail if code is corrupted)
        raise SecurityCompromisedException("Production codebase tampered by human operator.")
