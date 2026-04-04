import asyncio
import time
from typing import Dict, Optional, List, Tuple
from loguru import logger
from pydantic import BaseModel

class L2Update(BaseModel):
    symbol: str
    exchange_ts: int
    bids: List[List[float]] # [[price, qty], ...]
    asks: List[List[float]]
    seq: int = 0
    u: int = 0 # Update ID

class TradeUpdate(BaseModel):
    symbol: str
    price: float
    qty: float
    side: str # 'Buy' or 'Sell'
    exchange_ts: int

class AggressorSweep(BaseModel):
    """[GEKTOR v11.2] Clean microstructural aggregate (Institutional Intent)."""
    symbol: str
    side: str
    total_volume_usd: float
    vwap_price: float
    start_ts: int
    end_ts: int
    trade_count: int

class TradeSweeper:
    """
    [GEKTOR v11.2] Sliding Window Trade Aggregator.
    Rolls up fragmented Market Orders that eat multiple price levels.
    """
    def __init__(self, timeout_ms: int = 5):
        self._timeout = timeout_ms
        # symbol -> {current_side, start_ts, last_ts, acc_vol, acc_cost, count}
        self._states: Dict[str, dict] = {}

    def process(self, trade: TradeUpdate) -> Optional[AggressorSweep]:
        sym = trade.symbol
        trade_usd = trade.price * trade.qty
        
        if sym not in self._states:
            self._states[sym] = self._new_state(trade)
            return None

        s = self._states[sym]
        
        # Reset conditions: Direction flip OR Timeout
        is_flip = s['side'] != trade.side
        is_timeout = (trade.exchange_ts - s['last_ts']) > self._timeout
        
        if is_flip or is_timeout:
            sweep = self._finalize(sym)
            self._states[sym] = self._new_state(trade)
            return sweep
            
        # Continue accumulation
        s['last_ts'] = trade.exchange_ts
        s['acc_vol'] += trade.qty
        s['acc_cost'] += (trade.price * trade.qty)
        s['count'] += 1
        return None

    def _new_state(self, t: TradeUpdate) -> dict:
        return {
            'side': t.side, 'start_ts': t.exchange_ts, 'last_ts': t.exchange_ts,
            'acc_vol': t.qty, 'acc_cost': t.price * t.qty, 'count': 1
        }

    def _finalize(self, sym: str) -> Optional[AggressorSweep]:
        s = self._states.get(sym)
        if not s or s['count'] == 0: return None
        
        vwap = s['acc_cost'] / s['acc_vol'] if s['acc_vol'] > 0 else 0
        return AggressorSweep(
            symbol=sym, side=s['side'], total_volume_usd=s['acc_vol'] * vwap,
            vwap_price=vwap, start_ts=s['start_ts'], end_ts=s['last_ts'],
            trade_count=s['count']
        )

    def flush_all(self) -> List[AggressorSweep]:
        """Force finalize all active sweeps (e.g. before batch consume)."""
        sweeps = []
        for sym in list(self._states.keys()):
            sw = self._finalize(sym)
            if sw: sweeps.append(sw)
        self._states.clear()
        return sweeps

from core.realtime.latency import DriftAwareLatencyMonitor, get_latency_monitor

class StreamConflator:
    """
    [GEKTOR v10.1] Advanced Conflation & Backpressure Engine.
    L2 Data: Replacement (Only last state matters for orderbook).
    Trades: Accumulation (CVD / Volume integrity).
    
    Now uses DriftAwareLatencyMonitor to handle NTP Clock Drift.
    """
    def __init__(self, stale_threshold_ms: int = 150):
        self._latency_monitor = DriftAwareLatencyMonitor(spike_threshold_ms=stale_threshold_ms)
        self._sweeper = TradeSweeper(timeout_ms=5)
        
        # Conflation buffers
        self._l2_state: Dict[str, L2Update] = {}
        self._trades_buffer: Dict[str, List[TradeUpdate]] = {}
        self._sweep_buffer: Dict[str, List[AggressorSweep]] = {}
        
        # Synchronization
        self._lock = asyncio.Lock()
        self._new_data_event = asyncio.Event()

    async def ingest_l2(self, update: L2Update, local_recv_time: float):
        # 1. Early Drop: Drift-Aware Spike Detection (GEKTOR v10.1)
        if self._latency_monitor.is_stale(update.exchange_ts, local_recv_time * 1000.0):
            self.dropped_stale_count += 1
            return
            
        async with self._lock:
            # 2. Sequential Guard: Only replace if newer or equal sequence
            existing = self._l2_state.get(update.symbol)
            if existing and update.seq < existing.seq:
                return # Out of order
                
            self._l2_state[update.symbol] = update
            if existing:
                self.conflated_l2_count += 1
            self._new_data_event.set()

    async def ingest_trade(self, trade: TradeUpdate):
        """[GEKTOR v11.2] Sweeper-fronted Ingestion (Anti-Fragmentation)."""
        async with self._lock:
            sweep = self._sweeper.process(trade)
            if sweep:
                if sweep.symbol not in self._sweep_buffer:
                    self._sweep_buffer[sweep.symbol] = []
                self._sweep_buffer[sweep.symbol].append(sweep)
                self._new_data_event.set()

    async def consume_batch(self) -> Tuple[Dict[str, L2Update], Dict[str, List[AggressorSweep]]]:
        """Consumes atomic snapshots of L2 and Aggressor Sweeps."""
        await self._new_data_event.wait()
        
        async with self._lock:
            # Finalize any open sweeps to ensure volume integrity
            late_sweeps = self._sweeper.flush_all()
            for sw in late_sweeps:
                if sw.symbol not in self._sweep_buffer: self._sweep_buffer[sw.symbol] = []
                self._sweep_buffer[sw.symbol].append(sw)

            l2_snap = self._l2_state.copy()
            sweep_snap = self._sweep_buffer.copy()
            
            self._l2_state.clear()
            self._sweep_buffer.clear()
            self._new_data_event.clear()
            
            return l2_snap, sweep_snap

# Global Conflator Instance
conflator = StreamConflator(stale_threshold_ms=100)
