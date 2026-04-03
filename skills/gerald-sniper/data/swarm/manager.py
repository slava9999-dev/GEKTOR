import asyncio
from typing import List, Dict, Set, Any
from loguru import logger
from .shard_worker import ShardWorkerProcess

class SwarmManager:
    """
    HFT Swarm Orchestrator (v2.7 Static Tier Edition).
    """
    def __init__(self, stream_url: str = "wss://stream.bybit.com/v5/public/linear"):
        self.stream_url = stream_url
        self.shards: Dict[int, ShardWorkerProcess] = {}
        self.base_port = 5550
        
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

    async def update_subscriptions(self, watchlist: List[str], topic_types: List[str] = None):
        """
        Manages high-performance Shard lifecycle. 
        Uses static tiers to prevent state migration artifacts (Gap Risk).
        """
        # HFT Architecture: Only restart shards if symbols list changed significantly.
        # For simplicity in this demo, we assume static tiers are updated on radar scan.
        tiers = self._get_static_tiers(watchlist)
        
        for shard_id, symbols in tiers.items():
            if not symbols: continue
            
            if shard_id in self.shards:
                # Potential update logic here
                continue
                
            port = self.base_port + shard_id
            logger.info(f"⚡ [Swarm] Launching Shard {shard_id} ({len(symbols)} coins) on port {port}")
            p = ShardWorkerProcess(shard_id, symbols, self.stream_url, port)
            p.start()
            self.shards[shard_id] = p

    async def disconnect(self):
        for p in self.shards.values():
            p.terminate()
        self.shards.clear()
