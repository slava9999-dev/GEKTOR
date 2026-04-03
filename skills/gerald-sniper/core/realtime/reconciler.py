import asyncio
import time
from loguru import logger
from typing import Dict, Optional
from core.events.events import RawWSEvent
from .market_state import market_state

class OrderbookReconciler:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.last_update_id: int = 0
        self.last_cross_seq: int = 0
        self.is_dirty: bool = True  # Start dirty, wait for snapshot
        self._sync_lock = asyncio.Lock()

    async def apply_update(self, payload: dict) -> bool:
        """
        [Audit v7.5] Reconciler Entry. Returns False if state is broken.
        """
        msg_type = payload.get("type")
        data = payload.get("data", {})
        
        current_u = data.get("u") or 0
        cross_seq = data.get("seq") or 0
        
        if msg_type == "snapshot":
            self.last_update_id = current_u
            self.last_cross_seq = cross_seq
            self.is_dirty = False
            logger.info(f"📐 [{self.symbol}] L2 Snapshot Validated (u: {current_u}, seq: {cross_seq})")
            return True
            
        elif msg_type == "delta":
            if self.is_dirty:
                return False
                
            # 1. Monotonicity check (u should increase)
            if current_u <= self.last_update_id:
                # Potential re-transmission or out-of-order within a batch. 
                # If equal, it's redundant but not 'broken'.
                return True 
                
            # 2. Strong Sequence Guard (seq: Cross-Sequence)
            # Bybit V5: seq is strictly monotonic +1 for the same topic.
            if self.last_cross_seq > 0 and cross_seq != self.last_cross_seq + 1:
                logger.critical(f"🚨 [{self.symbol}] SEQUENCE GAP! Expected seq {self.last_cross_seq+1}, got {cross_seq}. DETECTED BLIND WINDOW.")
                await self.trigger_rest_resync("Sequence Gap")
                return False
                
            self.last_update_id = current_u
            self.last_cross_seq = cross_seq
            return True
        
        return True

    async def trigger_rest_resync(self, reason: str):
        """
        [Audit 28.1] Non-blocking REST-based resynchronization.
        Uses background task to keep the WS reader alive.
        """
        if self.is_dirty: return
        self.is_dirty = True
        
        # [P0] Immediate Signal Freeze
        state = market_state.get_state(self.symbol)
        if state:
            state.orderbook.is_valid = False
            state.mark_cold(f"Reconciler trigger: {reason}")
            
        # Non-blocking background sync
        asyncio.create_task(self._execute_async_sync())

    async def _execute_async_sync(self):
        async with self._sync_lock:
            logger.warning(f"🚑 [{self.symbol}] Starting REST Snapshot Recovery...")
            try:
                # Trigger the actual REST fetch and book rebuild in market_state
                # This should handle the Snapshot -> Delta replay internally
                await market_state.trigger_resync(self.symbol)
                logger.success(f"✅ [{self.symbol}] State Reconciled via REST.")
            except Exception as e:
                logger.error(f"❌ [{self.symbol}] Resync failed: {e}. Retrying in 5s.")
                self.is_dirty = True
                await asyncio.sleep(5)
                asyncio.create_task(self._execute_async_sync())

class GlobalStateReconciler:
    """Manages reconcilers for all symbols."""
    def __init__(self):
        self.reconcilers: Dict[str, OrderbookReconciler] = {}

    async def reconcile(self, event: RawWSEvent):
        topic = event.topic
        if not topic.startswith("orderbook"): return
        
        symbol = topic.split('.')[-1]
        if symbol not in self.reconcilers:
            self.reconcilers[symbol] = OrderbookReconciler(symbol)
        
        # Prepare payload for reconciler logic
        payload = {
            "type": event.type,
            "data": {
                "u": event.u,
                # Bybit puts seq inside data or at top level? docs say at top level of message.
                # But RawWSEvent might not have it yet.
                "seq": getattr(event, 'seq', 0) 
            }
        }
        await self.reconcilers[symbol].apply_update(payload)

# Global Instance
reconciler = GlobalStateReconciler()
