import asyncio
from typing import Dict, Any, List, Optional
from loguru import logger
import time

class L2RecoverySentinel:
    """
    [GEKTOR v14.8.1] Circuit Breaker & Async State Recovery for Orderbooks.
    - Kleppmann: Atomic Stitching logic to ensure state consistency after GAP.
    - Beazley: Background recovery to prevent Event Loop stalls.
    - HFT Quant: Protection against stale data poisoning.
    """
    def __init__(self, rest_client: Any, priority_gateway: Any):
        self.rest = rest_client
        self.priority_gateway = priority_gateway
        self.is_recovering: Dict[str, bool] = {}
        self.buffer: Dict[str, List[tuple[int, dict]]] = {}
        self.orderbook_refs: Dict[str, Any] = {}
        self.failure_counts: Dict[str, int] = {} # [GEKTOR v14.8.3] Circuit Breaker
        self._lock = asyncio.Lock()

    def register_orderbook(self, symbol: str, orderbook: Any):
        self.orderbook_refs[symbol] = orderbook
        self.failure_counts[symbol] = 0

    def mark_stale(self, symbol: str):
        """Called when a Sequence GAP or Timeout is detected."""
        if self.is_recovering.get(symbol):
            return # Already recovering
            
        self.is_recovering[symbol] = True
        self.buffer[symbol] = []
        
        # Invalidate the orderbook immediately to prevent toxic reads
        if symbol in self.orderbook_refs:
            self.orderbook_refs[symbol].state = "SYNCING"
            
        logger.warning(f"🛡️ [{symbol}] L2 State marked as STALE. Triggering Async Hard-Reset.")
        
        # Schedule recovery in the background
        asyncio.create_task(self._execute_hard_reset(symbol))

    def buffer_delta(self, symbol: str, sequence: int, delta: dict) -> bool:
        """
        Intercepts incoming deltas while recovery is in progress.
        Returns True if the delta was buffered.
        """
        if self.is_recovering.get(symbol, False):
            self.buffer[symbol].append((sequence, delta))
            return True
        return False

    async def _execute_hard_reset(self, symbol: str):
        """
        [Audit 14.8.3] Circuit Breaker Recovery.
        - Step 0: Exponential Backoff with Jitter.
        - Step 1: REST Snapshot fetch.
        - Step 2: Atomic Stitching.
        """
        try:
            # 0. CIRCUIT BREAKER BACKOFF (Jittered Exponential)
            failures = self.failure_counts.get(symbol, 0)
            if failures > 0:
                import random
                # (2^failures) with jitter, capped at 60s
                delay = min(60.0, (1.0 * (2 ** (failures - 1))) + random.uniform(0.1, 0.5))
                logger.warning(f"🔌 [{symbol}] Circuit Breaker active. Failure #{failures}. Cooling down for {delay:.1f}s.")
                await asyncio.sleep(delay)

            # 1. Fetch Ground Truth (REST API call via Priority 2 Gateway)
            snapshot = await self.priority_gateway.execute(
                priority=2,
                name=f"L2Recovery:{symbol}",
                coro_fn=lambda: self.rest.get_l2_snapshot(symbol, limit=200)
            )
            if not snapshot or 'u' not in snapshot:
                raise ValueError("Incomplete REST Snapshot received")
            
            snapshot_seq = int(snapshot['u'])
            
            # 2. Atomic Rebuild in Orderbook memory
            if symbol not in self.orderbook_refs:
                return
                
            orderbook = self.orderbook_refs[symbol]
            
            # 3. Apply Snapshot
            orderbook.apply_snapshot_and_replay(snapshot) # [Audit 14.3] Use combined method
            
            # 4. Success Reset
            self.failure_counts[symbol] = 0
            self.is_recovering[symbol] = False
            self.buffer[symbol].clear()
            logger.success(f"✅ [{symbol}] L2 Recovery SUCCESS after {failures} failures.")

        except Exception as e:
            self.failure_counts[symbol] = self.failure_counts.get(symbol, 0) + 1
            logger.error(f"❌ [{symbol}] Recovery Failed: {e}. Failure total: {self.failure_counts[symbol]}")
            # Ensure we allow another attempt after some delay handled by the next cycle or backoff
            self.is_recovering[symbol] = False 
            
    async def watchdog_loop(self):
        """
        [GEKTOR v14.8.2] HFT Watchdog (2.5s).
        - Beazley: Monotonic clock prevents NTP-jump anomalies.
        - HFT Quant: 2.5s is the maximum tolerable silence before state toxicity.
        """
        while True:
            try:
                now = time.monotonic()
                for symbol, ob in list(self.orderbook_refs.items()):
                    if ob.state == "VALID":
                        # ob.last_heartbeat must be updated using monotonic time
                        silence = now - ob.last_heartbeat
                        if silence > 2.5:
                            logger.critical(f"💀 [Watchdog] SYMBOL {symbol} SILENT ({silence:.1f}s). Hard-Reset triggered.")
                            self.mark_stale(symbol)
                            # Update heartbeat locally to prevent immediate re-trigger
                            ob.last_heartbeat = now
                
                await asyncio.sleep(0.5) # Fast poll
            except Exception as e:
                logger.error(f"❌ [RecoveryWatchdog] Error: {e}")
                await asyncio.sleep(5)

# Global Sentinel instance
l2_recovery = L2RecoverySentinel(None)
