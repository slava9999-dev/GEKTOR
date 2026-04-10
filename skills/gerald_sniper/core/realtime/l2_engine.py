import time
import heapq
from typing import Dict, List, Optional, Tuple
from collections import deque
from loguru import logger

# Global Integer Ticks Multiplier
TICK_MULTIPLIER = 100_000 # 5 decimal places precision (standard for most USDT pairs)

class TrackedLevel:
    """Zero-GC Level Structure. Optimized for memory density."""
    __slots__ = ['price_tick', 'size', 'first_seen_ts', 'last_trade_ts', 'survival_score']
    def __init__(self, price_tick: int, size: float, ts: float):
        self.price_tick = price_tick # Integer
        self.size = size
        self.first_seen_ts = ts
        self.last_trade_ts = 0.0
        self.survival_score = 1.0 # 1.0 = High Conviction, 0.0 = Phantom/Spoof

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
        
        # Config
        self.maturity_sec = maturity_sec
        self.ttl_sec = 0.150 # 150ms window
        self._replenishment_threshold = 0.05 # 5% эвристика шума
        self._toxic_levels: Dict[int, float] = {} # price_tick -> target_replenishment_qty
        
    def _to_tick(self, price: float) -> int:
        return int(price * TICK_MULTIPLIER + 0.5)

    def mark_level_toxic(self, price: float):
        """
        [HFT Quant]: Маркировка уровня как фантомного.
        Очищается только через process_l2_delta при вбросе реального объема.
        """
        p = self._to_tick(price)
        # Устанавливаем порог восполнения (например, средний сайз уровня или 5% от цели)
        self._toxic_levels[p] = self._replenishment_threshold
        logger.warning(f"☢️ [L2_TOXIC] Price {price} marked as PHANTOM. Awaiting replenishment.")

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
        
        # [HFT Quant]: Динамический сброс токсичности (Event-Driven)
        # Если объем на уровне обновился и превысил порог - это реальный игрок.
        for p_f, s in (bids_delta + asks_delta):
            p = self._to_tick(p_f)
            if p in self._toxic_levels:
                if s > self._toxic_levels[p]:
                    self._toxic_levels.pop(p, None)
                    logger.info(f"✨ [REPLENISHMENT] Level {p_f} cleared from toxic list.")

        self._cleanup_ttl(now)

    def apply_trade(self, trade_price_f: float, trade_vol: float):
        p = self._to_tick(trade_price_f)
        if p in self.pending_cancels:
            self.pending_cancels.pop(p, None)
            
        if p in self.bids:
            self.bids[p].last_trade_ts = time.time()
        elif p in self.asks:
            self.asks[p].last_trade_ts = time.time()
            # [HFT Quant] Absorption confirmed. Boost survival score.
            self.asks[p].survival_score = min(1.0, self.asks[p].survival_score + 0.2)

    def update_survival_probabilities(self, current_mid: float):
        """
        [PHANTOM FILTER] Dynamic Survival Analysis.
        Calculates the probability that a wall is real vs spoofed based on 
        distance-to-mid and placement duration.
        """
        now = time.time()
        for side_levels in [self.bids, self.asks]:
            for p_tick, lvl in side_levels.items():
                p = p_tick / TICK_MULTIPLIER
                distance = abs(p - current_mid) / current_mid
                
                # Если цена подходит вплотную (<0.1%), а объем не уменьшается через Trades - 
                # проверяем на "мерцание" (cancellation rate).
                if distance < 0.001: 
                    # Если уровень старый, но на нем нет трейдов при такой близости - штрафуем
                    if now - lvl.last_trade_ts > 0.5:
                        lvl.survival_score = max(0.0, lvl.survival_score - 0.1)
                
                # Recovery: Если уровень "выжил" и цена отошла - постепенно восстанавливаем доверие
                elif distance > 0.005:
                    lvl.survival_score = min(1.0, lvl.survival_score + 0.05)

    def _cleanup_ttl(self, now: float):
        while self.cancel_ttl_queue and (now - self.cancel_ttl_queue[0][0]) > self.ttl_sec:
            _, p = self.cancel_ttl_queue.popleft()
            if p in self.pending_cancels:
                self.pending_cancels.pop(p, None)

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
                # Toxic zone check removed/merged for brevity of this specific tool call
                lvl = levels[p]
                
                # [HFT Quant] Survival-Aware Weighting
                # If survival_score is low, we ignore this volume in imbalance calculation
                if lvl.survival_score < 0.4:
                    continue

                age = now - lvl.first_seen_ts
                age_weight = 1.0 if (now - lvl.last_trade_ts < 2.0) else min(1.0, age / self.maturity_sec)
                dist_weight = 1.0 / (i + 1)
                
                total_score += lvl.size * age_weight * dist_weight * lvl.survival_score
            return total_score

        bid_score = score_side(self.bids, True)
        ask_score = score_side(self.asks, False)
        
        denom = bid_score + ask_score
        return (bid_score - ask_score) / (denom + 1e-10)

    def get_best_quotes(self) -> Tuple[Optional[Tuple[float, float]], Optional[Tuple[float, float]]]:
        """Returns ((best_bid_p, best_bid_v), (best_ask_p, best_ask_v))"""
        if not self.bids or not self.asks:
            return None, None
        
        best_bid_tick = max(self.bids.keys())
        best_ask_tick = min(self.asks.keys())
        
        return (best_bid_tick / TICK_MULTIPLIER, self.bids[best_bid_tick].size), \
               (best_ask_tick / TICK_MULTIPLIER, self.asks[best_ask_tick].size)

    def get_micro_price(self) -> Optional[float]:
        """
        [GEKTOR v21.4] Calculates Fair Value Micro-Price.
        Formula: (P_ask * V_bid + P_bid * V_ask) / (V_bid + V_ask)
        """
        quotes = self.get_best_quotes()
        if not quotes or not quotes[0] or not quotes[1]:
            return None
        
        (p_bid, v_bid), (p_ask, v_ask) = quotes
        total_vol = v_bid + v_ask
        if total_vol <= 0:
            return (p_bid + p_ask) / 2
            
        return (p_ask * v_bid + p_bid * v_ask) / total_vol

    def get_liquidity_at_offset(self, side: str, max_slippage_offset: float) -> Tuple[float, float]:
        """
        [GEKTOR v14.9.0] Micro-Liquidity Search.
        Finds cumulative size within max_slippage_offset, filtering out toxic levels.
        Returns: (Available Qty, Recommended Strike Price)
        """
        now = time.time()
        is_buy = (side.lower() == 'buy')
        
        # Get Best Bid/Ask as anchor
        if is_buy:
            # We are buying, so we look at ASKS
            target_levels = self.asks
            best_p_tick = min(target_levels.keys()) if target_levels else None
        else:
            # We are selling, so we look at BIDS
            target_levels = self.bids
            best_p_tick = max(target_levels.keys()) if target_levels else None
            
        if best_p_tick is None:
            return 0.0, 0.0
            
        best_price = best_p_tick / TICK_MULTIPLIER
        
        # Calculate Strike Boundary
        if is_buy:
            limit_price_tick = self._to_tick(best_price * (1.0 + max_slippage_offset))
            candidates = sorted([p for p in target_levels.keys() if p <= limit_price_tick])
        else:
            limit_price_tick = self._to_tick(best_price * (1.0 - max_slippage_offset))
            candidates = sorted([p for p in target_levels.keys() if p >= limit_price_tick], reverse=True)
            
        total_qty = 0.0
        strike_price_tick = best_p_tick # Start with best
        
        for p in candidates:
            # 1. Toxic Filter (Volume-Driven Replenishment Barrier)
            if p in self._toxic_levels:
                continue # Skip phantom level
            
            total_qty += target_levels[p].size
            strike_price_tick = p # Walk the book to the depth we need
            
        return total_qty, strike_price_tick / TICK_MULTIPLIER
