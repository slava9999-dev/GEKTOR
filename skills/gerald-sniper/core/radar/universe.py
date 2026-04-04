# core/radar/universe.py
import asyncio
import time
from typing import List, Set, Optional, Any
from loguru import logger

from data.bybit_rest import BybitREST
from core.events.nerve_center import bus
from core.events.events import UniverseChangeEvent
from utils.math_utils import safe_float

class DynamicUniverseShaker:
    """
    [GEKTOR v11.9] Adaptive Universe Controller.
    
    Dynamically rebalances the set of active symbols for WS streaming and scanning.
    Constraints: 
    - Turnover 24h > $50,000,000 (Institutional interest)
    - Spread < 15bps (Execution efficiency)
    - Rebalance every 15 minutes.
    """
    
    def __init__(
        self, 
        rest: BybitREST, 
        redis: Any = None,
        min_turnover_usd: float = 50_000_000,
        max_spread_bps: float = 15.0,
        rebalance_interval_sec: int = 900
    ):
        self.rest = rest
        self.redis = redis
        self.min_turnover = min_turnover_usd
        self.max_spread = max_spread_bps / 10000.0 # bps to decimal
        self.interval = rebalance_interval_sec
        
        self.active_universe: Set[str] = set()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if self._running: return
        self._running = True
        
        # Immediate first scan
        await self._rebalance_cycle()
        
        # Start background loop
        self._task = asyncio.create_task(self._rebalance_loop())
        logger.info(f"📊 [UniverseShaker] ONLINE — Rebalance every {self.interval/60:.0f}m | T-Floor: ${self.min_turnover/1e6:.0f}M | Spread-Cap: {self.max_spread*10000:.0f}bps")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("🛑 [UniverseShaker] Stopped.")

    async def _rebalance_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.interval)
                await self._rebalance_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [UniverseShaker] Rebalance loop error: {e}")
                await asyncio.sleep(60)

    async def _rebalance_cycle(self):
        """Fetches tickers, filters by liquidity/spread, and updates universe."""
        t0 = time.monotonic()
        try:
            tickers = await self.rest.get_tickers()
            if not tickers:
                logger.warning("⚠️ [UniverseShaker] Empty tickers. Skipping rebalance.")
                return

            new_universe = set()
            for t in tickers:
                symbol = t.get("symbol", "")
                if not symbol.endswith("USDT"): continue
                
                turnover = safe_float(t.get("turnover24h", 0))
                bid = safe_float(t.get("bid1Price", 0))
                ask = safe_float(t.get("ask1Price", 0))
                
                if bid <= 0 or ask <= 0: continue
                
                spread = (ask - bid) / bid
                
                if turnover >= self.min_turnover and spread <= self.max_spread:
                    new_universe.add(symbol)

            # Compare and Apply
            added = list(new_universe - self.active_universe)
            removed = list(self.active_universe - new_universe)
            
            if added or removed:
                logger.success(
                    f"🔄 [UniverseShaker] Rebalance DONE | "
                    f"New: {len(new_universe)} symbols | "
                    f"+{len(added)}, -{len(removed)} | {time.monotonic()-t0:.2f}s"
                )
                
                # Update Redis Persistent State
                if self.redis:
                    await self.redis.set("macro:active_universe", ",".join(sorted(list(new_universe))))
                    
                # Store locally
                old_universe = self.active_universe
                self.active_universe = new_universe
                
                # Broadcast Event
                event = UniverseChangeEvent(
                    new_universe=list(new_universe),
                    added=added,
                    removed=removed,
                    reason="SCHEDULED_REBALANCE"
                )
                await bus.publish(event)
            else:
                logger.debug(f"📊 [UniverseShaker] No changes | Universe: {len(new_universe)} symbols")

        except Exception as e:
            logger.error(f"❌ [UniverseShaker] Rebalance cycle failed: {e}")

    def get_current_universe(self) -> List[str]:
        return list(self.active_universe)
