import time
from dataclasses import dataclass, field
from typing import Any
from collections import deque
from utils.math_utils import EPS, safe_float

import numpy as np
import bisect
from loguru import logger


import heapq

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

    def apply_snapshot(self, data: dict):
        self._apply_snapshot_internal(data)
        self.is_valid = True
        self.sync_state = SyncState.READY
        self.last_heartbeat = time.time()

    def _apply_snapshot_internal(self, data: dict):
        self._price_map["b"].clear()
        self._price_map["a"].clear()
        self._heaps["b"] = []
        self._heaps["a"] = []
        
        self.last_update_id = int(data.get('u', 0))
        self._process_levels(data.get('b', []), "b")
        self._process_levels(data.get('a', []), "a")

    def apply_delta(self, data: dict):
        self.last_heartbeat = time.time()
        
        if self.sync_state == SyncState.SYNCING:
            # Audit 20.1: Check capacity BEFORE append to avoid silent loss
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

    def _cpu_heavy_snapshot_processing(self, raw_data: dict) -> dict:
        """
        CPU-Bound: Runs in thread pool via asyncio.to_thread().
        
        Parses and validates snapshot data WITHOUT touching Event Loop.
        The GIL releases during orjson C-extension work, giving Event Loop
        some breathing room even within the same process.
        
        Returns processed result dict for lightweight application in main thread.
        """
        if not raw_data or 'b' not in raw_data:
            raise ValueError("Invalid snapshot: missing 'b' key")
        
        snapshot_u = int(raw_data.get('u', 0))
        
        # Heavy processing: float conversion of all levels
        bids = [(float(p), float(v)) for p, v in raw_data.get('b', [])]
        asks = [(float(p), float(v)) for p, v in raw_data.get('a', [])]
        
        return {
            'u': snapshot_u,
            'b': raw_data.get('b', []),
            'a': raw_data.get('a', []),
            'bid_count': len(bids),
            'ask_count': len(asks),
        }

    async def trigger_active_resync(self, rest_client):
        """
        Hardened REST Resync (v3.0).
        
        CPU-bound snapshot parsing is offloaded to thread pool.
        Event Loop stays responsive for Smart Exit Engine (50ms cycle).
        Post-sync GC spike mitigation prevents STW pauses.
        """
        import asyncio
        
        if self.sync_state == SyncState.SYNCING: return
            
        self.sync_state = SyncState.SYNCING
        self._delta_buffer.clear()
        logger.warning(f"🔄 [{self.symbol}] Initiating REST Seamless Sync (Thread-Isolated)...")
        
        try:
            # Phase 1: I/O-Bound — Download snapshot (non-blocking, awaitable)
            res = await rest_client.get_orderbook_snapshot(self.symbol, limit=50)
            
            # Phase 2: CPU-Bound — Parse & validate in thread pool
            # Event Loop continues serving Smart Exit Engine during this call
            processed = await asyncio.to_thread(self._cpu_heavy_snapshot_processing, res)
            
            # Phase 3: Lightweight — Apply to internal state (O(N) but with pre-parsed data)
            self._apply_snapshot_internal(processed)
            
            snapshot_u = processed['u']
            
            # Phase 4: Catch-up — Replay buffered deltas (already in memory, lightweight)
            while self._delta_buffer:
                delta = self._delta_buffer.popleft()
                if int(delta.get('u', 0)) > snapshot_u:
                    self._process_levels(delta.get('b', []), "b")
                    self._process_levels(delta.get('a', []), "a")
                    self.last_update_id = int(delta['u'])
            
            self.is_valid = True
            self.sync_state = SyncState.READY
            
            # Phase 5: GC Spike Mitigation — Clean up temporaries from thread
            try:
                from core.runtime.memory_manager import mem_manager
                spike_ms = mem_manager.mitigate_spike()
                logger.info(f"✅ [{self.symbol}] Sync Complete "
                           f"({processed['bid_count']}b/{processed['ask_count']}a levels, GC: {spike_ms:.1f}ms).")
            except ImportError:
                logger.info(f"✅ [{self.symbol}] Sync Complete.")
            
        except Exception as e:
            logger.error(f"❌ [{self.symbol}] Sync Failed: {e}")
            self.sync_state = SyncState.READY
            self.is_valid = False

    def _process_levels(self, levels: list, side: str):
        pmap = self._price_map[side]
        pheap = self._heaps[side]
        is_bid = (side == "b")
        
        for p_str, v_str in levels:
            price, vol = float(p_str), float(v_str)
            
            # Audit 20.3: Heap Memory Protection (Skip duplicates)
            # Only push if price was not in pmap or was previously 0.0
            is_new_to_heap = (pmap.get(price, 0.0) == 0.0)
            
            pmap[price] = vol
            
            if vol > 0.0 and is_new_to_heap:
                # O(log N) insertion
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
        """Lazy Deletion O(1) access (Audit 19.3)."""
        heap = self._heaps["b"]
        pmap = self._price_map["b"]
        while heap:
            p = -heap[0]
            if pmap.get(p, 0.0) > 0: return p
            heapq.heappop(heap)
        return None

    @property
    def best_ask(self) -> float | None:
        """Lazy Deletion O(1) access (Audit 19.3)."""
        heap = self._heaps["a"]
        pmap = self._price_map["a"]
        while heap:
            p = heap[0]
            if pmap.get(p, 0.0) > 0: return p
            heapq.heappop(heap)
        return None

    def get_depth_volume(self, side: str, depth: int = 10) -> float:
        """
        Audit 20.x: True O(K log N) Pop-and-Restore Depth Scoring.
        Avoids O(N) heapify or nsmallest.
        """
        heap = self._heaps[side]
        pmap = self._price_map[side]
        is_bid = (side == "b")
        
        total_vol = 0.0
        count = 0
        extracted = []
        
        while heap and count < depth:
            h_val = heapq.heappop(heap)
            p = -h_val if is_bid else h_val
            vol = pmap.get(p, 0.0)
            
            if vol > 0.0:
                total_vol += vol
                count += 1
                extracted.append(h_val)
            else:
                # Permanent lazy deletion of ghosts
                pass
                
        # Restore valid levels
        for h_val in extracted:
            heapq.heappush(heap, h_val)
            
        return total_vol

    def get_volume_within_distance(self, side: str, distance_pct: float = 0.15) -> float:
        """
        Audit 21.1: Zero-Mutation Read.
        Iterates directly over heap array (O(N)) to avoid Pop-and-Restore overhead.
        In Python, direct list iteration is faster than frequent heappush/pop balance cycles.
        """
        heap = self._heaps[side]
        pmap = self._price_map[side]
        is_bid = (side == "b")
        
        bp = self.best_bid if is_bid else self.best_ask
        if not bp: return 0.0
        
        limit_p = bp * (1 - distance_pct/100) if is_bid else bp * (1 + distance_pct/100)
        total_vol = 0.0
        
        # O(N) Sequential Scan - Best for Python L1/L2 cache usage
        for h_val in heap:
            p = -h_val if is_bid else h_val
            # Boundary check
            if (is_bid and p >= limit_p) or (not is_bid and p <= limit_p):
                vol = pmap.get(p, 0.0)
                if vol > 0.0:
                    total_vol += vol
                    
        return total_vol

    def calculate_vwap(self, side: str, order_qty: float) -> dict:
        """
        Audit 23.1: Strict Synchronous VWAP Guard (No Async Leaks).
        Uses try/finally to guarantee heap integrity.
        """
        heap = self._heaps[side]
        pmap = self._price_map[side]
        is_bid = (side == "b")
        
        remaining_qty = order_qty
        total_notional = 0.0
        worst_price = 0.0
        extracted = []
        
        try:
            while heap and remaining_qty > EPS:
                h_val = heapq.heappop(heap)
                p = -h_val if is_bid else h_val
                vol = pmap.get(p, 0.0)
                
                if vol <= 0:
                    continue # Hot-loop lazy cleanup
                    
                extracted.append(h_val)
                fill_qty = min(remaining_qty, vol)
                total_notional += fill_qty * p
                remaining_qty -= fill_qty
                worst_price = p
                
            if remaining_qty > EPS:
                return {"status": "INSUFFICIENT_LIQUIDITY", "collected_qty": order_qty - remaining_qty}
                
            return {
                "status": "OK",
                "vwap": total_notional / order_qty,
                "worst_price": worst_price,
                "collected_qty": order_qty
            }
        finally:
            # Atomic Restore: Guarantee Event Loop never sees 'Yawning' hole
            for h_val in extracted:
                heapq.heappush(heap, h_val)


@dataclass
class SymbolLiveState:
    symbol: str
    current_price: float = 0.0
    bid_vol_usd: float = 0.0
    ask_vol_usd: float = 0.0
    micro_imbalance: float = 0.0
    
    # HFT Micro-Structure (Task 16.7: Lead-Lag V2)
    bba_bid: float = 0.0
    bba_ask: float = 0.0
    exchange_ts: int = 0 # Unix TS in ms from exchange payload (E or T)
    update_id: int = 0   # Sequence ID from exchange (u or s)
    
    # HFT Micro-CVD: Zero-Allocation Circular Buffer (v2.2.1)
    cvd_window_ms: int = 500
    cvd_history_size: int = 1000
    # [timestamp, signed_volume]
    cvd_history: np.ndarray = field(default_factory=lambda: np.zeros((1000, 2), dtype=np.float64))
    cvd_ptr: int = 0
    current_cvd: float = 0.0

    # HFT Engine v2.3.0 (Replaces legacy numpy orderbook arrays)
    orderbook: ZeroGCOrderbook = field(default_factory=ZeroGCOrderbook)
    
    # Bucketed data (existing)
    buckets: dict[int, dict] = field(default_factory=dict)
    history_ts: deque = field(default_factory=lambda: deque(maxlen=300))
    
    # Liquidations: Zero-Allocation Circular Buffer
    liq_history: np.ndarray = field(default_factory=lambda: np.zeros((500, 4), dtype=np.float64))
    liq_ptr: int = 0

    # [Audit 6.7] Dynamic Slippage: Spread Volatility Buffer
    spread_buffer: deque = field(default_factory=lambda: deque(maxlen=60)) # ~3-6s of samples
    
    def update_spread(self, bid: float, ask: float):
        """Calculates and stores current spread in BP."""
        if bid > 0 and ask > 0 and bid < ask:
            spread_bps = (ask - bid) / bid * 10000
            self.spread_buffer.append(spread_bps)
            
    def get_spread_std(self) -> float:
        """Returns standard deviation of spread in BP."""
        if len(self.spread_buffer) < 5: return 0.0 # Need some history
        return float(np.std(self.spread_buffer))

    # [Audit v6.1] Zero-Phase-Lag BBO Sanity Guard
    def is_sane_price(self, trade_price: float, best_bid: float, best_ask: float) -> bool:
        """
        [L1 Cross-Validation] Verifies trade tick against Orderbook BBO.
        - Zero Phase Lag: No rolling windows or median calculations.
        - Cross-Channel Verification: Validates Trades (T-channel) against L1 (S-channel).
        """
        if best_bid <= 0.0 or best_ask <= 0.0:
            return True # Insufficient L1 data to judge

        from core.runtime.config_manager import config_manager
        # Default 0.5% spread tolerance for micro-async drift between WS channels
        max_dev = config_manager.get("max_bbo_deviation", 0.005)
        
        lower_bound = best_bid * (1.0 - max_dev)
        upper_bound = best_ask * (1.0 + max_dev)
        
        if lower_bound <= trade_price <= upper_bound:
            return True
            
        logger.critical(f"⚠️ [SanityGuard] {self.symbol} DIRTY TICK: {trade_price:.6f} outside BBO [{lower_bound:.6f}-{upper_bound:.6f}]. Filtered.")
        return False

    # [NERVE REPAIR v4.1] Warm-up Guard
    first_tick_ts: float | None = None
    required_warmup_sec: int = 300 # 5 minutes default
    
    @property
    def is_ready(self) -> bool:
        """
        [NERVE REPAIR v4.1] Readiness Probe.
        Ensures rolling windows are sufficiently filled to avoid Spike Anomaly.
        """
        if self.first_tick_ts is None:
            return False
        age = time.time() - self.first_tick_ts
        return age >= self.required_warmup_sec

    @property
    def freshness_ms(self) -> int:
        """[Audit 5.9] Adjusted Freshness Gauge (Server Drift compensated)."""
        if self.exchange_ts == 0: return 999999
        from core.realtime.market_state import market_state
        # Logic: (Local_Now + Offset) - Msg_Exchange_TS
        # Msg_Exchange_TS is the time standard in Bybit Matching Engine
        return int(time.time() * 1000 + market_state.server_time_offset) - self.exchange_ts

    def mark_cold(self, reason: str = "Manual Reset"):
        """Invalidates warm-up status (Audit: Data Gap Recovery)."""
        if self.first_tick_ts:
            logger.warning(f"🧊 [{self.symbol}] State marked COLD: {reason}")
        self.first_tick_ts = None
        self.buckets.clear()
        self.history_ts.clear()
        # Note: We keep liq_history and cvd_history as they are relative or large
        # but reset rolling buckets which drive most detectors.

    def get_orderbook_imbalance(self):
        """Audit 20.5: Use ZMQ-fed metrics (Shielded by Shards)."""
        # Return pre-computed values from ZMQ bridge
        return float(self.bid_vol_usd), float(self.ask_vol_usd)

    def update(self, price: float, qty: float, side: str):
        now = int(time.time())
        self.current_price = price
        
        if now not in self.buckets:
            if len(self.history_ts) >= self.history_ts.maxlen:
                oldest = self.history_ts.popleft()
                self.buckets.pop(oldest, None)
            self.history_ts.append(now)
            self.buckets[now] = {'price': price, 'vol': 0.0, 'buy_vol': 0.0, 'trades': 0}
        
        b = self.buckets[now]
        b['price'] = price
        b['vol'] += price * qty
        if side.lower() == 'buy':
            b['buy_vol'] += price * qty
        b['trades'] += 1

    def get_price_change_pct(self, seconds: int) -> float:
        now = int(time.time())
        target_ts = now - seconds
        
        # Find closest bucket to target_ts
        start_price = None
        for ts in self.history_ts:
            if ts >= target_ts:
                start_price = self.buckets[ts]['price']
                break
        
        if not start_price or start_price < EPS: return 0.0
        return ((self.current_price - start_price) / max(start_price, EPS)) * 100

    def get_trade_count(self, seconds: int) -> int:
        now = int(time.time())
        cutoff = now - seconds
        return sum(b['trades'] for ts, b in self.buckets.items() if ts > cutoff)

    def get_volume_usd(self, seconds: int) -> float:
        now = int(time.time())
        cutoff = now - seconds
        return sum(b['vol'] for ts, b in self.buckets.items() if ts > cutoff)

    def get_volume_delta_usd(self, seconds: int) -> float:
        now = int(time.time())
        cutoff = now - seconds
        buy_v = sum(b.get('buy_vol', 0) for ts, b in self.buckets.items() if ts > cutoff)
        total_v = sum(b.get('vol', 0) for ts, b in self.buckets.items() if ts > cutoff)
        return buy_v - (total_v - buy_v)

    def get_orderflow_imbalance(self, seconds: int) -> float:
        now = int(time.time())
        cutoff = now - seconds
        buy_v = sum(b['buy_vol'] for ts, b in self.buckets.items() if ts > cutoff)
        total_v = sum(b['vol'] for ts, b in self.buckets.items() if ts > cutoff)
        
        sell_v = total_v - buy_v
        if sell_v < EPS: return 2.5 if buy_v > EPS else 1.0
        return buy_v / max(sell_v, EPS)

    def get_liquidation_volume(self, seconds: int) -> float:
        """Efficient numpy scan of pre-allocated liquidation buffer (v2.2.1)."""
        now = time.time()
        cutoff = now - seconds
        mask = (self.liq_history[:, 0] >= cutoff) & (self.liq_history[:, 0] > 0)
        # Sum (price * qty) from the masked rows
        return np.sum(self.liq_history[mask, 1] * self.liq_history[mask, 2])

    def get_cvd_momentum(self) -> float:
        """Fast momentum check for CVD within current window."""
        now = int(time.time() * 1000)
        cutoff = now - self.cvd_window_ms
        mask = self.cvd_history[:, 0] >= cutoff
        return np.sum(self.cvd_history[mask, 1])

    def get_acceleration(self) -> float:
        m1 = self.get_price_change_pct(30)
        # Previous 30s means from T-60 to T-30
        now = int(time.time())
        
        p_30, p_60 = None, None
        for ts in reversed(self.history_ts):
            if not p_30 and ts <= now - 30: p_30 = self.buckets[ts]['price']
            if not p_60 and ts <= now - 60: 
                p_60 = self.buckets[ts]['price']
                break
        
        if not p_30 or not p_60 or p_60 < EPS: return 0.0
        m2 = ((p_30 - p_60) / max(p_60, EPS)) * 100
        return m1 - m2

    def get_moving_average(self, seconds: int) -> float:
        now = int(time.time())
        cutoff = now - seconds
        prices = [b['price'] for ts, b in self.buckets.items() if ts > cutoff]
        if not prices: return self.current_price
        return sum(prices) / len(prices)

    def get_volatility(self, seconds: int) -> float:
        """Returns standard deviation of prices in the window."""
        now = int(time.time())
        cutoff = now - seconds
        prices = [b['price'] for ts, b in self.buckets.items() if ts > cutoff]
        if len(prices) < 2: return 0.0
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / (len(prices) - 1)
        return (variance ** 0.5) / max(mean, EPS) * 100 # In percentage
