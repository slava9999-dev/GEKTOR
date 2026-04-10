# core/db/leader.py
"""
[GEKTOR v21.15.6] Distributed Sentinel with Fencing Tokens.

Ensures 'Leader Election' and 'Split-Brain' protection via Redis leases
and Monotonic Epochs in the Database (PostgreSQL/SQLite).
"""
import asyncio
import uuid
import time
from typing import Optional
from loguru import logger

class DistributedSentinel:
    """
    Coordinates leadership across multiple replicas.
    Implements Fencing Tokens to prevent 'Zombie Leader' writes.
    """
    def __init__(self, db_manager, redis_client=None, lease_ttl: float = 5.0):
        self.db = db_manager
        self.redis = redis_client
        self._replica_id = f"Jaguar-{str(uuid.uuid4())[:8]}"
        self._is_leader = False
        self._current_epoch = 0
        self._lease_ttl = lease_ttl
        self._heartbeat_interval = lease_ttl / 3.0
        self._watchdog_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Starts the Leadership Heartbeat Task."""
        if not self._watchdog_task:
            self._watchdog_task = asyncio.create_task(self._election_loop())

    async def _election_loop(self) -> None:
        """Continuous battle for dominance."""
        while True:
            try:
                if not self._is_leader:
                    await self._attempt_takeover()
                else:
                    await self._renew_lease()
                
                await asyncio.sleep(self._heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"⚠️ [Sentinel] Coordination failure: {e}. Stepping down.")
                await self._step_down()
                await asyncio.sleep(2)

    async def _attempt_takeover(self) -> None:
        """
        [GEKTOR v21.15.6] Atomic Takeover.
        Increments the 'Fencing Token' (Epoch) in the Database.
        """
        # In a real setup, we'd use SET NX in Redis here, then increment DB Epoch.
        success, new_epoch = await self.db.acquire_leadership_lease(self._replica_id, self._lease_ttl)
        if success:
            self._is_leader = True
            self._current_epoch = new_epoch
            logger.success(f"👑 [Sentinel] Node {self._replica_id} is LEADER. Epoch: {new_epoch}")
        else:
            self._is_leader = False

    async def _renew_lease(self) -> None:
        """Heartbeat: keeps the lease alive in the DB."""
        success = await self.db.renew_leadership_lease(self._replica_id, self._lease_ttl)
        if not success:
            logger.critical(f"💀 [Sentinel] LEASE EXPIRED! Another node took over. NODE_SELF_RECALL.")
            await self._step_down()

    async def _step_down(self) -> None:
        self._is_leader = False
        self._current_epoch = 0
        # No explicit DB change needed; let the lease expire naturally for others to pick up

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def epoch(self) -> int:
        return self._current_epoch


# Global Sentinel Singleton (Initialized in Composition Root)
sentinel: Optional[DistributedSentinel] = None
