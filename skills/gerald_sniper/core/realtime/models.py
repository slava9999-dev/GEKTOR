import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from collections import deque
from enum import Enum
from utils.math_utils import EPS, safe_float
import numpy as np
import heapq
from loguru import logger
from core.realtime.reconciler import l2_recovery
from core.realtime.l2_engine import L2LiquidityEngine
from core.realtime.vpin import MacroVPINTracker, RawTick


class SequenceGapError(Exception):
    """Raised when an L2 sequence break is detected."""
    pass

class SyncState(Enum):
    READY = 1
    SYNCING = 2

@dataclass(slots=True)
class L1Tick:
    timestamp: float
    price: float
    bid_p: float
    bid_v: float
    ask_p: float
    ask_v: float

class OrderFlowImbalance:
    """
    [GEKTOR v14.9] Microstructural State Machine (OFI).
    Identifies toxic order flow by analyzing BBO dynamics.
    """
    __slots__ = ['prev_bid_p', 'prev_bid_v', 'prev_ask_p', 'prev_ask_v', 'accumulator']

    def __init__(self):
        self.prev_bid_p: float = 0.0
        self.prev_bid_v: float = 0.0
        self.prev_ask_p: float = 0.0
        self.prev_ask_v: float = 0.0
        self.accumulator: float = 0.0

    def update_and_get(self, tick: L1Tick) -> float:
        """Evaluates OFI for a single tick."""
        if self.prev_bid_p == 0.0:
            self._save_state(tick)
            return 0.0

        # Bid Side (Aggressive Buying Intent)
        if tick.bid_p > self.prev_bid_p:
            e_t = tick.bid_v
        elif tick.bid_p == self.prev_bid_p:
            e_t = tick.bid_v - self.prev_bid_v
        else:
            e_t = -self.prev_bid_v

        # Ask Side (Aggressive Selling Intent)
        if tick.ask_p < self.prev_ask_p:
            f_t = tick.ask_v
        elif tick.ask_p == self.prev_ask_p:
            f_t = tick.ask_v - self.prev_ask_v
        else:
            f_t = -self.prev_ask_v

        ofi = e_t - f_t
        self.accumulator += ofi
        self._save_state(tick)
        return ofi

    def _save_state(self, tick: L1Tick) -> None:
        self.prev_bid_p = tick.bid_p
        self.prev_bid_v = tick.bid_v
        self.prev_ask_p = tick.ask_p
        self.prev_ask_v = tick.ask_v

    def get_and_reset_accumulator(self) -> float:
        """Idempotent reset for Dollar Bar hand-off."""
        val = self.accumulator
        self.accumulator = 0.0
        return val


class ZeroGCOrderbook:
    """
    HFT Orderbook Engine (Audit 19.x).
    Uses Binary Heaps (heapq) for O(log N) updates and O(1) best price access.
    Implements Lazy Deletion to avoid O(N) list-shuffling.
    """
    def __init__(self, symbol: str = "UNKNOWN", buffer_maxlen: int = 2000):
        # [Audit 22.7] Orderbook State Machine
        self.state = "VALID" # VALID, SYNCING, CORRUPTED
        self.symbol = symbol
        self.sync_state = SyncState.READY
        self._price_map = {"b": {}, "a": {}}
        # Heaps store prices. Bids are negative for max-heap behavior.
        self._heaps = {"b": [], "a": []}
        self.last_update_id = 0
        self.last_heartbeat = time.monotonic()
        self.is_valid = False
        self._cross_ts = 0.0 # [GEKTOR v14.3] Transient Cross Timestamp
        
        # In-Flight Buffer with strict limit (Audit 19.1)
        self._delta_buffer = deque(maxlen=buffer_maxlen) 
        self.last_heartbeat = time.time()
        self.last_l2_ts = 0.0 # [GEKTOR v10.3] High-res L2 freshness

    def apply_snapshot_and_replay(self, snapshot: dict):
        """[Audit 22.8] Atomic Stitching (REST Snapshot + WS Delta Replay)."""
        try:
            # 1. Apply Base Layer
            self._apply_snapshot_internal(snapshot)
            current_u = self.last_update_id
            
            # 2. Sequential Replay with Continuity Check
            while self._delta_buffer:
                delta = self._delta_buffer.popleft()
                u = int(delta.get('u', 0))
                
                if u <= current_u:
                    continue # Skip stale
                    
                if u != current_u + 1:
                    raise SequenceGapError(f"Stitching Gap: expected {current_u + 1}, got {u}")
                
                self._apply_delta_internal(delta)
                current_u = u
            
            # 3. Final Validation
            self.last_update_id = current_u
            self.state = "VALID"
            logger.success(f"🧩 [{self.symbol}] Orderbook stitched & verified at u={current_u}")
            
        except SequenceGapError as e:
            logger.error(f"❌ [{self.symbol}] Replay Failed: {e}")
            self.state = "CORRUPTED"
            self._delta_buffer.clear()
            raise
        except Exception as e:
            logger.exception(f"🔥 [{self.symbol}] Critical Replay Error: {e}")
            self.state = "CORRUPTED"
            raise

    def _apply_delta_internal(self, data: dict):
        current_u = int(data.get('u', 0))
        self.last_update_id = current_u
        self._process_levels(data.get('b', []), "b")
        self._process_levels(data.get('a', []), "a")
        self._validate_book()

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
        
        if self.state == "SYNCING":
            # [Audit 22.1] Seamless Sync Buffer: Buffer updates while wait for REST Snapshot
            self._delta_buffer.append(data)
            return

        if self.state != "VALID": return

        current_u = int(data.get('u', 0))
        self.last_heartbeat = time.monotonic()
        self.last_l2_ts = time.time()
        
        # [Audit 22.4] Cold Start: Anchor on first received sequence
        if self.last_update_id == 0:
            self.last_update_id = current_u - 1 # Prep for continuity check
            
        if current_u <= self.last_update_id: return
            
        # [GEKTOR v14.8.1] Proactive Gap Detection
        if current_u != self.last_update_id + 1:
            logger.critical(f"🚨 [{self.symbol}] Sequence GAP: expected {self.last_update_id + 1}, got {current_u}")
            self.state = "CORRUPTED"
            l2_recovery.mark_stale(self.symbol)
            return

        # [GEKTOR v14.8.1] Intercept if recovering
        if l2_recovery.buffer_delta(self.symbol, current_u, data):
            return

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
            # [Audit 22.7] Lazy Nulling: Preserve dict hash-table topology
            # Instead of pop(), we keep the key with 0.0 volume (Lazy Deletion)
            # This minimizes resizing of price_map when GC IS FROZEN.
            is_new_to_heap = (pmap.get(price, 0.0) == 0.0)
            pmap[price] = vol
            
            if vol > 0.0 and is_new_to_heap:
                heap_val = -price if is_bid else price
                heapq.heappush(pheap, heap_val)

    def prune(self, tolerance: float = 0.5):
        """
        [GEKTOR v14.8.2] Orderbook Pruner (v14.8.2).
        Clears Lazy-Nulled price levels that are too far from Mid-Price.
        tolerance: 0.5 (50%)
        """
        bb = self.best_bid
        ba = self.best_ask
        if not bb or not ba: return
        mid = (bb + ba) / 2
        
        pruned = 0
        for side in ["b", "a"]:
            pmap = self._price_map[side]
            # Identify levels to delete
            to_delete = []
            for p, v in pmap.items():
                if v == 0.0:
                    dist = abs(p - mid) / mid
                    if dist > tolerance:
                        to_delete.append(p)
            
            # David Beazley: Batch deletion to minimize hash-table churn
            for p in to_delete:
                del pmap[p]
            pruned += len(to_delete)
        
        if pruned > 100:
            logger.info(f"🧹 [{self.symbol}] Pruned {pruned} dead price levels from OOB memory.")

    def _validate_book(self):
        bb = self.best_bid
        ba = self.best_ask
        if bb and ba and bb >= ba:
            # 1. Start grace period timer using monotonic clock (Safety Guard)
            if self._cross_ts == 0.0:
                self._cross_ts = time.monotonic()
            
            # 2. Check if we exceeded the "Transient" window
            # Monotonic time prevents drift/adjustment jumps from NTP
            elapsed_ms = (time.monotonic() - self._cross_ts) * 1000
            if elapsed_ms > 200: 
                logger.critical(f"🛑 [CROSSED BOOK] {self.symbol}: {bb} >= {ba} (Duration: {elapsed_ms:.1f}ms)")
                self.is_valid = False
        else:
            # Book is healthy, reset timer
            self._cross_ts = 0.0

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
    current_ofi: float = 0.0
    ofi_engine: OrderFlowImbalance = field(default_factory=OrderFlowImbalance)
    
    # [GEKTOR v21.5] Macro-Accumulation Engine (VPIN)
    vpin_engine: Optional[MacroVPINTracker] = None
    last_vpin: float = 0.0
    
    def __post_init__(self):
        # [GEKTOR v14.8.1] Symbol Propagation
        if self.orderbook.symbol == "UNKNOWN":
            self.orderbook.symbol = self.symbol
        self.l2_radar = L2LiquidityEngine(symbol=self.symbol)
    
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
    
    # [GEKTOR v11.7-11.9] State Integrity & Lifecycle
    is_reconciling: bool = False
    is_removing: bool = False # [GEKTOR v11.9] Tombstone Flag
    tombstone_expiration: float = 0.0
    parking_buffer: deque = field(default_factory=lambda: deque(maxlen=2000))
    last_seq_id: int = 0
    
    # [GEKTOR v11.3] Tox-Vol Tracking (Metadata, not raw counter)
    toxic_volume_total: float = 0.0 
    pending_liq_events: deque = field(default_factory=lambda: deque(maxlen=50))

    orderbook: ZeroGCOrderbook = field(default_factory=ZeroGCOrderbook)
    l2_radar: L2LiquidityEngine = field(init=False)
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

    def update(self, price_or_event: Any, qty: Optional[float] = None, side: Optional[str] = None):
        """
        [GEKTOR v12.0] Unified Entry Point.
        Accepts raw events or processed primitives.
        """
        # Case A: Primitive call (price, qty, side)
        if isinstance(price_or_event, (int, float)) and qty is not None:
            self.current_price = float(price_or_event)
            self.apply_trade(self.current_price, qty, side or 'Buy')
            return

        # Case B: Event Object (Trade / Sweep)
        event = price_or_event
        if hasattr(event, 'price') and hasattr(event, 'qty'):
            self.current_price = event.price
            self.apply_trade(event.price, event.qty, getattr(event, 'side', 'Buy'))
        
        # Case C: L2 Object (Snapshot / Delta)
        elif hasattr(event, 'bids') or hasattr(event, 'asks'):
            # L2 updates are usually processed via orderbook.apply_delta
            # but we can record the heartbeat here
            self.last_l2_ts = time.time()

    def apply_trade(self, price: float, qty: float, side: str):
        """Updates internal CVD and OHLCV buckets."""
        now = int(time.time())
        side_val = 1.0 if side.lower() == 'buy' else -1.0
        
        # 1. Update CVD
        self.current_cvd += qty * side_val
        self.last_trade_ts = time.time()
        
        # 2. Update Buckets
        if now not in self.buckets:
            if len(self.history_ts) >= self.history_ts.maxlen:
                old_ts = self.history_ts.popleft()
                self.buckets.pop(old_ts, None)
            self.history_ts.append(now)
            self.buckets[now] = {
                'price': price,
                'vol': 0.0,
                'buy_vol': 0.0,
                'trades': 0,
                'high': price,
                'low': price,
                'open': price
            }
        
        b = self.buckets[now]
        b['price'] = price
        b['vol'] += qty
        if side_val > 0: b['buy_vol'] += qty
        b['trades'] += 1
        b['high'] = max(b['high'], price)
        b['low'] = min(b['low'], price)

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
        self.is_reconciling = False
        self.parking_buffer.clear()

    def tag_toxic_volume(self, liq_qty: float, side: str):
        """[GEKTOR v11.3] De-duper: Tags existing volume as toxic."""
        self.toxic_volume_total += liq_qty
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
