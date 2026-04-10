import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Deque
from collections import deque
from loguru import logger

@dataclass(frozen=True, slots=True)
class TradeEvent:
    symbol: str
    price: float
    qty: float
    side: str
    exchange_ts: int # Matching Engine Time (ms)
    trade_id: str

from collections import deque, defaultdict

class StreamSynchronizer:
    """
    [GEKTOR v14.4.1] Institutional Stream Joiner (O1).
    Uses Deque for O(1) cleanup. Implements Microstructural OFI (Order Flow Imbalance).
    """
    __slots__ = ('_trade_buffer', '_time_window_ms')

    def __init__(self, time_window_ms: int = 150):
        self._trade_buffer: defaultdict[str, deque[TradeEvent]] = defaultdict(deque)
        self._time_window_ms = time_window_ms

    def push_trade(self, symbol: str, price: float, qty: float, side: str, ts: int, trade_id: str):
        buffer = self._trade_buffer[symbol]
        buffer.append(TradeEvent(
            symbol=symbol, price=price, qty=qty, side=side, exchange_ts=ts, trade_id=trade_id
        ))
        
        # O(1) Garbage Collection: Prune trades outside the window
        cutoff = ts - self._time_window_ms
        while buffer and buffer[0].exchange_ts < cutoff:
            buffer.popleft()

    def analyze_absorption(self, symbol: str, price: float, l2_ts: int) -> dict:
        """
        [Audit 24.1] Order Flow Imbalance (OFI) Analysis.
        Distinguishes institutional sweeps from wash-trading noise.
        """
        buffer = self._trade_buffer.get(symbol)
        if not buffer:
            return {"absorbed_qty": 0.0, "ofi_score": 0.0, "is_toxic": True}

        absorbed_qty = 0.0
        buy_vol = 0.0
        sell_vol = 0.0
        trade_count = 0

        # Sync window scan (Worst case is small N in a 150ms window)
        for trade in reversed(buffer):
            if l2_ts - trade.exchange_ts > self._time_window_ms:
                break
            
            if abs(trade.price - price) < 1e-9:
                absorbed_qty += trade.qty
                trade_count += 1
                if trade.side.lower() == "buy":
                    buy_vol += trade.qty
                else:
                    sell_vol += trade.qty

        # OFI: 1.0 = Pure one-way aggression (Whale Print), 0.0 = Symmetrical noise (Wash/MM)
        ofi = abs(buy_vol - sell_vol) / (absorbed_qty or 1.0)
        
        return {
            "absorbed_qty": absorbed_qty,
            "trade_count": trade_count,
            "ofi_score": ofi,
            "is_toxic": ofi < 0.7 and trade_count > 1 # Too much non-directional churn
        }

# Global Singleton
synchronizer = StreamSynchronizer()
