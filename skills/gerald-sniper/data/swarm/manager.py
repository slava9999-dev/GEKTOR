import asyncio
from typing import List, Dict, Set, Any
from loguru import logger
from .shard_worker import ShardWorkerProcess
from core.events.nerve_center import bus
from core.events.events import UniverseChangeEvent

class SwarmManager:
    """
    HFT Swarm Orchestrator (v2.7 Dynamic Tier Edition).
    """
    def __init__(self, stream_url: str = "wss://stream.bybit.com/v5/public/linear"):
        self.stream_url = stream_url
        self.shards: Dict[int, ShardWorkerProcess] = {}
        self.base_port = 5550
        self.current_watchlist: List[str] = []
        
        # [GEKTOR v11.9] Adaptive Universe Integration
        bus.subscribe(UniverseChangeEvent, self.on_universe_change)
        
    def _get_static_tiers(self, watchlist: List[str]) -> Dict[int, List[str]]:
        """
        Static Sharding Logic (Rule 16.48):
        - Shard 0 (Titan): Leading Assets (BTC, ETH, SOL).
        - Shard 1-3 (Majors): High-Vol Assets.
        - Shard 4-7 (Scouts): Low-Vol / Memes.
        """
        titan_symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"}
        
        tiers = {i: [] for i in range(8)}
        remaining = []
        
        for s in watchlist:
            if s in titan_symbols:
                tiers[0].append(s)
            else:
                remaining.append(s)
        
        # Split remaining equally among shards 1-7
        # In production, this would use historic trade count to balance.
        chunk_size = (len(remaining) // 7) + 1
        for i in range(1, 8):
            start = (i - 1) * chunk_size
            end = start + chunk_size
            tiers[i] = remaining[start:end]
            
        return tiers

    async def update_subscriptions(self, watchlist: List[str]):
        """
        Manages high-performance Shard lifecycle. 
        Uses static tiers to prevent state migration artifacts (Gap Risk).
        """
        if set(watchlist) == set(self.current_watchlist):
            return
            
        self.current_watchlist = watchlist
        tiers = self._get_static_tiers(watchlist)
        
        # For GEKTOR v11.9: We currently RESTART shards if universe changes.
        # This is a safe baseline. Future: ZMQ control pipe for hot-swap.
        await self.disconnect()
        
        for shard_id, symbols in tiers.items():
            if not symbols: continue
            
            port = self.base_port + shard_id
            logger.info(f"⚡ [Swarm] Launching Shard {shard_id} ({len(symbols)} coins) on port {port}")
            p = ShardWorkerProcess(shard_id, symbols, self.stream_url, port)
            p.start()
            self.shards[shard_id] = p

    async def on_universe_change(self, event: UniverseChangeEvent):
        """[GEKTOR v11.9] Event Handler for background rebalancing."""
        logger.info(f"🔄 [SwarmManager] Detected Universe Change (+{len(event.added)}, -{len(event.removed)}). Rebalancing shards...")
        await self.update_subscriptions(event.new_universe)

    async def disconnect(self):
        for p in self.shards.values():
            try:
                p.terminate()
                p.join(timeout=1)
            except: pass
        self.shards.clear()
