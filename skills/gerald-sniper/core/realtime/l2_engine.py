import time
import heapq
from typing import Dict, List, Optional, Tuple
from collections import deque
from loguru import logger

# Global Integer Ticks Multiplier
TICK_MULTIPLIER = 100_000 # 5 decimal places precision (standard for most USDT pairs)

class TrackedLevel:
    """Zero-GC Level Structure. Optimized for memory density."""
    __slots__ = ['price_tick', 'size', 'first_seen_ts', 'last_trade_ts']
    def __init__(self, price_tick: int, size: float, ts: float):
        self.price_tick = price_tick # Integer
        self.size = size
        self.first_seen_ts = ts
        self.last_trade_ts = 0.0

class PendingCancel:
    """O(1) Cancellation Marker."""
    __slots__ = ['price_tick', 'volume', 'cancel_ts']
    def __init__(self, price_tick: int, volume: float, ts: float):
        self.price_tick = price_tick
        self.volume = volume
        self.cancel_ts = ts

class L2LiquidityEngine:
    """
    HFT Liquidity Radar v2.5 (Hardened Edition).
    - Integer Ticks for dictionary keys (No Float Inaccuracy).
    - O(N + K log K) Top-of-Book search via heapq.
    - Zero-GC via __slots__.
    """
    def __init__(self, symbol: str, maturity_sec: float = 1.5):
        self.symbol = symbol
        self.bids: Dict[int, TrackedLevel] = {} # Key: Price in Ticks
        self.asks: Dict[int, TrackedLevel] = {}
        
        # Confirmation Logic
        self.pending_cancels: Dict[int, PendingCancel] = {}
        self.cancel_ttl_queue = deque() # (timestamp, price_tick)
        self.toxic_zones: Dict[int, float] = {} # price_tick -> expiry_ts
        
        # Config
        self.maturity_sec = maturity_sec
        self.ttl_sec = 0.150 # 150ms window
        self.toxicity_duration = 30.0 # 30s cooldown
        
    def _to_tick(self, price: float) -> int:
        return int(price * TICK_MULTIPLIER + 0.5)

    def apply_delta(self, bids_delta: List[Tuple[float, float]], asks_delta: List[Tuple[float, float]]):
        now = time.time()
        
        # Process Bids
        for p_f, s in bids_delta:
            p = self._to_tick(p_f)
            if s == 0.0:
                if p in self.bids:
                    old_vol = self.bids[p].size
                    self.pending_cancels[p] = PendingCancel(p, old_vol, now)
                    self.cancel_ttl_queue.append((now, p))
                    self.bids.pop(p, None)
            else:
                if p not in self.bids:
                    self.bids[p] = TrackedLevel(p, s, now)
                else:
                    self.bids[p].size = s
                    
        # Process Asks
        for p_f, s in asks_delta:
            p = self._to_tick(p_f)
            if s == 0.0:
                if p in self.asks:
                    old_vol = self.asks[p].size
                    self.pending_cancels[p] = PendingCancel(p, old_vol, now)
                    self.cancel_ttl_queue.append((now, p))
                    self.asks.pop(p, None)
            else:
                if p not in self.asks:
                    self.asks[p] = TrackedLevel(p, s, now)
                else:
                    self.asks[p].size = s
        
        self._cleanup_ttl(now)

    def apply_trade(self, trade_price_f: float, trade_vol: float):
        p = self._to_tick(trade_price_f)
        if p in self.pending_cancels:
            self.pending_cancels.pop(p, None)
            
        if p in self.bids:
            self.bids[p].last_trade_ts = time.time()
        elif p in self.asks:
            self.asks[p].last_trade_ts = time.time()

    def _cleanup_ttl(self, now: float):
        while self.cancel_ttl_queue and (now - self.cancel_ttl_queue[0][0]) > self.ttl_sec:
            _, p = self.cancel_ttl_queue.popleft()
            if p in self.pending_cancels:
                self.pending_cancels.pop(p, None)
                self.toxic_zones[p] = now + self.toxicity_duration

    def calculate_micro_imbalance(self, depth: int = 10) -> float:
        now = time.time()
        
        def score_side(levels: Dict[int, TrackedLevel], is_bid: bool):
            if not levels: return 0.0
            
            # O(N + K log K) instead of O(N log N)
            if is_bid:
                top_prices = heapq.nlargest(depth, levels.keys())
            else:
                top_prices = heapq.nsmallest(depth, levels.keys())
            
            total_score = 0.0
            for i, p in enumerate(top_prices):
                if p in self.toxic_zones:
                    if now < self.toxic_zones[p]: continue
                    else: self.toxic_zones.pop(p, None)
                
                lvl = levels[p]
                age = now - lvl.first_seen_ts
                age_weight = 1.0 if (now - lvl.last_trade_ts < 2.0) else min(1.0, age / self.maturity_sec)
                dist_weight = 1.0 / (i + 1)
                
                total_score += lvl.size * age_weight * dist_weight
            return total_score

        bid_score = score_side(self.bids, True)
        ask_score = score_side(self.asks, False)
        
        denom = bid_score + ask_score
        return (bid_score - ask_score) / (denom + 1e-10)
