import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from decimal import Decimal
from loguru import logger

from core.level_detector import detect_all_levels
from utils.config import config as global_config

class HTFLevelEngine:
    """
    Manages High-Timeframe (1d, 4h, 1h) levels.
    Implements caching and confluence scoring.
    """
    def __init__(self):
        self._store: Dict[str, Dict[str, List[dict]]] = {} # symbol -> {tf -> levels}
        self._last_update: Dict[str, Dict[str, datetime]] = {} # symbol -> {tf -> timestamp}
        
    def get_update_intervals(self) -> Dict[str, timedelta]:
        return {
            "D": timedelta(hours=4),
            "240": timedelta(hours=1),
            "60": timedelta(minutes=15)
        }

    async def update_needed(self, symbol: str, timeframe: str) -> bool:
        intervals = self.get_update_intervals()
        if symbol not in self._last_update or timeframe not in self._last_update[symbol]:
            return True
        elapsed = datetime.utcnow() - self._last_update[symbol][timeframe]
        return elapsed > intervals.get(timeframe, timedelta(hours=1))

    def set_levels(self, symbol: str, timeframe: str, levels: List[dict]):
        if symbol not in self._store:
            self._store[symbol] = {}
            self._last_update[symbol] = {}
        self._store[symbol][timeframe] = levels
        self._last_update[symbol][timeframe] = datetime.utcnow()
        logger.debug(f"📊 HTF Level Registry Updated: {symbol} {timeframe} ({len(levels)} levels)")

    def get_confluence(self, symbol: str, ltf_price: float, tolerance_pct: float = 0.5) -> dict:
        """
        Checks how many HTF levels align with a given LTF price.
        Returns: { 'score': 0..1, 'matches': [HTFLevel], 'tf_tags': '1d,4h' }
        """
        if symbol not in self._store:
            return {'score': 0.0, 'matches': [], 'tf_tags': ''}
            
        matches = []
        tfs_hit = set()
        score = 0.0
        
        weight_map = {"D": 0.45, "240": 0.35, "60": 0.20}
        
        for tf, levels in self._store[symbol].items():
            for lvl in levels:
                price = lvl['price']
                dist = abs(ltf_price - price) / ltf_price * 100.0
                if dist <= tolerance_pct:
                    matches.append(lvl)
                    tfs_hit.add(tf)
                    score += weight_map.get(tf, 0.1)
                    break # Take only one best match per TF to avoid double counting
                    
        return {
            'score': min(1.0, score),
            'matches': matches,
            'tf_tags': ",".join(sorted(list(tfs_hit)))
        }

# Global Instance
htf_engine = HTFLevelEngine()
