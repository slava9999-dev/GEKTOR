import asyncio
import time
from typing import Dict, Optional, Any
from loguru import logger

class AtomicDirectionalGuard:
    """
    [GEKTOR v12.0] Persistent Directional Mutex (Redis-backed).
    Prevents capital fragmentation and wash-trading artifacts.
    Standardized for APEX v12.0 core.
    """
    def __init__(self, redis: Any, lock_ttl_sec: int = 14400):
        self.redis = redis
        self.ttl = lock_ttl_sec
        self._local_lock = asyncio.Lock()
        
    async def can_proceed(self, symbol: str, direction: str) -> bool:
        """
        Check if we already have an active/pending signal in the opposite direction.
        Uses Redis for multi-process persistence.
        """
        key = f"macro:guard:{symbol}"
        async with self._local_lock:
            try:
                existing = await self.redis.get(key)
                if existing:
                    existing = existing.decode() if isinstance(existing, bytes) else str(existing)
                    if existing != direction:
                        logger.warning(f"🚫 [AtomicGuard] Blocked {direction} for {symbol} (Opposite {existing} active in Redis).")
                        return False
                
                # Record current attempt with TTL
                await self.redis.set(key, direction, ex=self.ttl)
                return True
            except Exception as e:
                logger.error(f"❌ [AtomicGuard] Redis failure: {e}")
                return True # Fail-open to avoid paralyzing the radar
            
    async def release(self, symbol: str):
        """Clears the lock for a symbol."""
        key = f"macro:guard:{symbol}"
        async with self._local_lock:
            await self.redis.delete(key)
            logger.debug(f"🔓 [AtomicGuard] Released {symbol}")

# For backward compatibility with simpler imports
directional_guard = None # Will be initialized by main.py if needed as singleton
