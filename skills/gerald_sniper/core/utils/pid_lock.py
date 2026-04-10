"""
Gerald Sniper v6.27.4 — Hardened OS-Level Locking (Context Manager).
Ensures kernel-level enforcement of single-instance shards.
Automatically releases resources on exit/crash.
"""
import os
import sys
import msvcrt
from loguru import logger

class PIDLock:
    def __init__(self, name: str):
        self.name = name
        # Ensure 'data' dir exists for local state
        os.makedirs("data", exist_ok=True)
        self.lock_file = f"data/{name}.lock"
        self.fh = None

    def __enter__(self):
        """Acquires lock before entering the shard's main logic."""
        try:
            # We use 'w' to refresh the lock file content
            self.fh = open(self.lock_file, 'w')
            
            # Windows Kernel-level exclusive lock (Mandatory)
            # LK_NBLCK: Non-blocking attempt. Raises IOError if file is locked.
            msvcrt.locking(self.fh.fileno(), msvcrt.LK_NBLCK, 1024) # Lock first 1KB
            
            # Write diagnostic info (PID and Path)
            self.fh.write(f"PID:{os.getpid()}\nCWD:{os.getcwd()}")
            self.fh.flush()
            
            logger.info(f"🔒 [LOCK] Kernel-level ownership confirmed for '{self.name}' (PID: {os.getpid()})")
            return self
            
        except (IOError, PermissionError):
            logger.critical(f"🚫 [LOCK_CONFLICT] Another instance of '{self.name}' is holding the kernel handle. Exiting to prevent Split-Brain.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"⚠️ [LOCK_ERROR] Failed to establish '{self.name}' context: {e}")
            sys.exit(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release the file handle. Kernel releases the lock automatically on close."""
        if self.fh:
            try:
                # First unlock (just to be pedantic)
                self.fh.seek(0)
                msvcrt.locking(self.fh.fileno(), msvcrt.LK_UNLCK, 1024)
                self.fh.close()
                if os.path.exists(self.lock_file):
                    os.remove(self.lock_file)
                logger.info(f"🔓 [LOCK] Released ownership of '{self.name}'.")
            except:
                pass
