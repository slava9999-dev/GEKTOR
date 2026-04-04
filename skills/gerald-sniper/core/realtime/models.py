import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from collections import deque
from utils.math_utils import EPS, safe_float
import numpy as np
import heapq
from loguru import logger

class SyncState:
    READY = 1
    SYNCING = 2

class ZeroGCOrderbook:
    """
    HFT Orderbook Engine (Audit 19.x).
    Uses Binary Heaps (heapq) for O(log N) updates and O(1) best price access.
    Implements Lazy Deletion to avoid O(N) list-shuffling.
    """
    def __init__(self, symbol: str = "UNKNOWN"):
        self.symbol = symbol
        self.sync_state = SyncState.READY
        self._price_map = {"b": {}, "a": {}}
        # Heaps store prices. Bids are negative for max-heap behavior.
        self._heaps = {"b": [], "a": []}
        self.last_update_id = 0
        self.is_valid = False
        self._cross_tolerance_counter = 0
        
        # In-Flight Buffer with strict limit (Audit 19.1)
        self._delta_buffer = deque(maxlen=1000) 
        self.last_heartbeat = time.time()
        self.last_l2_ts = 0.0 # [GEKTOR v10.3] High-res L2 freshness

    def apply_snapshot(self, data: dict):
        self._apply_snapshot_internal(data)
        self.is_valid = True
        self.sync_state = SyncState.READY
        self.last_heartbeat = self.last_l2_ts = time.time()

    def _apply_snapshot_internal(self, data: dict):
        self._price_map["b"].clear()
        self._price_map["a"].clear()
        self._heaps["b"] = []
        self._heaps["a"] = []
        
        self.last_update_id = int(data.get('u', 0))
        self._process_levels(data.get('b', []), "b")
        self._process_levels(data.get('a', []), "a")

    def apply_delta(self, data: dict):
        self.last_heartbeat = self.last_l2_ts = time.time()
        
        if self.sync_state == SyncState.SYNCING:
            if len(self._delta_buffer) == self._delta_buffer.maxlen:
                logger.critical(f"🛑 [{self.symbol}] REST Sync Buffer Full (1000)! Connection Dead.")
                self.is_valid = False
                raise OverflowError("Seamless sync buffer capacity breached")
            self._delta_buffer.append(data)
            return

        if not self.is_valid: return

        current_u = int(data.get('u', 0))
        if current_u <= self.last_update_id: return
            
        self.last_update_id = current_u
        self._process_levels(data.get('b', []), "b")
        self._process_levels(data.get('a', []), "a")
        self._validate_book()

    def _process_levels(self, levels: list, side: str):
        pmap = self._price_map[side]
        pheap = self._heaps[side]
        is_bid = (side == "b")
        
        for p_str, v_str in levels:
            price, vol = float(p_str), float(v_str)
            is_new_to_heap = (pmap.get(price, 0.0) == 0.0)
            pmap[price] = vol
            if vol > 0.0 and is_new_to_heap:
                heap_val = -price if is_bid else price
                heapq.heappush(pheap, heap_val)

    def _validate_book(self):
        bb = self.best_bid
        ba = self.best_ask
        if bb and ba and bb >= ba:
            self._cross_tolerance_counter += 1
            if self._cross_tolerance_counter >= 3:
                logger.critical(f"🛑 [CROSSED BOOK] {self.symbol}: {bb} >= {ba}")
                self.is_valid = False
        else:
            self._cross_tolerance_counter = 0

    @property
    def best_bid(self) -> float | None:
        heap = self._heaps["b"]
        pmap = self._price_map["b"]
        while heap:
            p = -heap[0]
            if pmap.get(p, 0.0) > 0: return p
            heapq.heappop(heap)
        return None

    @property
    def best_ask(self) -> float | None:
        heap = self._heaps["a"]
        pmap = self._price_map["a"]
        while heap:
            p = heap[0]
            if pmap.get(p, 0.0) > 0: return p
            heapq.heappop(heap)
        return None

@dataclass
class SymbolLiveState:
    symbol: str
    current_price: float = 0.0
    bid_vol_usd: float = 0.0
    ask_vol_usd: float = 0.0
    micro_imbalance: float = 0.0
    
    # GEKTOR v10.0: Blindness / Red Alert
    is_overheated: bool = False
    
    # [GEKTOR v10.3] Microstructural Reality Tracking
    last_l2_ts: float = 0.0
    last_trade_ts: float = 0.0
    
    # Zero-Allocation Storage (Rule 2.3)
    last_seq: int = 0
    seq_derivative: float = 0.0 # Delta seq per second
    
    # HFT Micro-Structure
    bba_bid: float = 0.0
    bba_ask: float = 0.0
    exchange_ts: int = 0 
    update_id: int = 0   
    
    # CVD Buffer
    cvd_window_ms: int = 500
    cvd_history_size: int = 1000
    cvd_history: np.ndarray = field(default_factory=lambda: np.zeros((1000, 2), dtype=np.float64))
    cvd_ptr: int = 0
    current_cvd: float = 0.0 # Asset units (sum of qty * side)
    last_cvd_checkpoint: float = 0.0
    
    # [GEKTOR v11.3] Tox-Vol Tracking (Metadata, not raw counter)
    toxic_volume_total: float = 0.0 
    pending_liq_events: deque = field(default_factory=lambda: deque(maxlen=50))

    orderbook: ZeroGCOrderbook = field(default_factory=ZeroGCOrderbook)
    buckets: dict[int, dict] = field(default_factory=dict)
    history_ts: deque = field(default_factory=lambda: deque(maxlen=300))
    liq_history: np.ndarray = field(default_factory=lambda: np.zeros((500, 4), dtype=np.float64))
    liq_ptr: int = 0
    spread_buffer: deque = field(default_factory=lambda: deque(maxlen=60))

    first_tick_ts: float | None = None
    required_warmup_sec: int = 300

    def update_spread(self, bid: float, ask: float):
        if bid > 0 and ask > 0 and bid < ask:
            spread_bps = (ask - bid) / bid * 10000
            self.spread_buffer.append(spread_bps)
            
    def get_spread_std(self) -> float:
        if len(self.spread_buffer) < 5: return 0.0
        return float(np.std(self.spread_buffer))

    def is_sane_price(self, trade_price: float, best_bid: float, best_ask: float) -> bool:
        if best_bid <= 0.0 or best_ask <= 0.0: return True
        max_dev = 0.005 # Default 0.5%
        lower_bound = best_bid * (1.0 - max_dev)
        upper_bound = best_ask * (1.0 + max_dev)
        return lower_bound <= trade_price <= upper_bound

    @property
    def is_ready(self) -> bool:
        if self.first_tick_ts is None: return False
        return (time.time() - self.first_tick_ts) >= self.required_warmup_sec

    def mark_cold(self, reason: str = "Manual Reset"):
        if self.first_tick_ts:
            logger.warning(f"🧊 [{self.symbol}] State marked COLD: {reason}")
        self.first_tick_ts = None
        self.buckets.clear()
        self.history_ts.clear()
        self.current_cvd = 0.0
        self.last_cvd_checkpoint = 0.0
        self.toxic_volume_total = 0.0
        self.pending_liq_events.clear()

    def tag_toxic_volume(self, liq_qty: float, side: str):
        """
        [GEKTOR v11.3] De-duper: Tags existing volume as toxic.
        Does NOT add new volume to CVD (volume already ingested via TradeStream).
        """
        self.toxic_volume_total += liq_qty
        # Labels the last minute bucket as containing 'toxic' components
        now = int(time.time())
        if now in self.buckets:
            bucket = self.buckets[now]
            if 'toxic_vol' not in bucket: bucket['toxic_vol'] = 0.0
            bucket['toxic_vol'] += liq_qty

    def get_cvd_alignment(self, direction: int) -> bool:
        """[GEKTOR v11.1] Checks if impulse direction matches recent CVD trend."""
        delta = self.current_cvd - self.last_cvd_checkpoint
        if direction == 1: # LONG Impulse
            return delta > 0
        else: # SHORT Impulse
            return delta < 0

        # [GEKTOR v11.1] CVD Persistence
        qty_delta = qty if side.lower() == 'buy' else -qty
        self.current_cvd += qty_delta

        b = self.buckets[now]
        b['price'] = price
        b['vol'] += price * qty
        if side.lower() == 'buy': b['buy_vol'] += price * qty
        b['trades'] += 1

    def get_price_change_pct(self, seconds: int) -> float:
        now = int(time.time())
        target_ts = now - seconds
        start_price = None
        for ts in self.history_ts:
            if ts >= target_ts:
                start_price = self.buckets[ts]['price']
                break
        if not start_price or start_price < EPS: return 0.0
        return ((self.current_price - start_price) / max(start_price, EPS)) * 100
