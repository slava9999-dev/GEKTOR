# core/radar/priority_watchlist.py

import time
from typing import Dict, Optional, List
from loguru import logger

class PriorityWatchlist:
    """Manages escalated symbols with TTL and activity-based eviction (Rule 2.3)"""
    MAX_SIZE = 12
    
    def __init__(self):
        self._entries: Dict[str, dict] = {} # symbol -> entry

    def add(self, symbol: str, reason: str, score: float) -> bool:
        now = time.time()
        
        if symbol in self._entries:
            # Already exists, just update/refresh
            self._entries[symbol]["ttl_until"] = now + 300
            self._entries[symbol]["score"] = score
            return True
            
        if len(self._entries) >= self.MAX_SIZE:
            candidate = self._find_eviction_candidate()
            if candidate:
                logger.info(f"🔄 [PriorityWL] Evicted {candidate} to make room for {symbol}")
                del self._entries[candidate]
            else:
                logger.warning(f"⚠️ [PriorityWL] List full and no candidates for eviction. Skipping {symbol}.")
                return False
                
        self._entries[symbol] = {
            "symbol": symbol,
            "added_at": now,
            "ttl_until": now + 300,
            "score": score,
            "reason": reason,
            "activity_count": 0
        }
        logger.info(f"⚡ [PriorityWL] Added {symbol} (Reason: {reason})")
        return True

    def _find_eviction_candidate(self) -> Optional[str]:
        now = time.time()
        
        # 1. First check for expired TTLs
        expired = [s for s, e in self._entries.items() if e["ttl_until"] < now]
        if expired:
            # Return lowest score among expired
            return min(expired, key=lambda s: self._entries[s]["score"])
            
        # 2. Then find lowest activity
        # Sort by activity (asc), then by age (desc)
        sorted_entries = sorted(
            self._entries.items(),
            key=lambda x: (x[1]["activity_count"], -x[1]["added_at"])
        )
        return sorted_entries[0][0] if sorted_entries else None

    def tick_activity(self, symbol: str):
        """Update activity and extend TTL (max 10m from start)"""
        if symbol in self._entries:
            entry = self._entries[symbol]
            entry["activity_count"] += 1
            now = time.time()
            cap = entry["added_at"] + 600
            entry["ttl_until"] = min(now + 180, cap)

    def cleanup_expired(self):
        now = time.time()
        to_del = [s for s, e in self._entries.items() if e["ttl_until"] < now]
        for s in to_del:
            logger.info(f"🕒 [PriorityWL] TTL Expired: {s}")
            del self._entries[s]

    def get_added_at(self, symbol: str) -> Optional[float]:
        """Returns the timestamp when symbol was added to priority watchlist."""
        entry = self._entries.get(symbol)
        return entry["added_at"] if entry else None

    @property
    def symbols(self) -> List[str]:
        return list(self._entries.keys())

# Global Instance
priority_watchlist = PriorityWatchlist()
