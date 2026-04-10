# core/radar/liquidation_feed.py
"""
[GEKTOR v21.15] Liquidation Volume Accumulator.

Ingests real-time liquidation events from exchange WebSocket feeds
and maintains a rolling window of liquidation volumes per symbol.

Architecture:
    WebSocket 'liquidation' topic → LiquidationAccumulator → Ring Buffer
    Ring Buffer is consumed by LiquidationDiscriminator in Phase 4.

Bybit V5 Public WebSocket:
    Topic: 'liquidation'
    Format: {"symbol": "BTCUSDT", "side": "Buy", "price": "30000", "size": "0.5", "updatedTime": 1690000000000}
    Note: "side" = side of the LIQUIDATED position. Buy = Long was liquidated, Sell = Short was liquidated.
          When Shorts are liquidated, the engine BUYS (Market Buy) → creates buy volume.
"""
import time
import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple
from loguru import logger


@dataclass(slots=True)
class LiquidationEvent:
    """Atomic liquidation event from exchange feed."""
    symbol: str
    side: str           # "Buy" = Long liquidated, "Sell" = Short liquidated
    price: float
    size: float         # In asset units
    value_usd: float    # price * size
    timestamp_ms: int   # Exchange timestamp


class LiquidationAccumulator:
    """
    [GEKTOR v21.15] Ring-Buffer Liquidation Tracker.
    
    Maintains rolling windows of liquidation events per symbol.
    Provides aggregated liquidation volume for a configurable time window.
    
    Thread Safety: Single-writer (WS handler), multiple-reader (ScoringEngine queries).
    Since Python GIL protects dict/deque mutations, this is safe without locks.
    """
    __slots__ = ('_buffers', '_window_s', '_max_events')

    def __init__(self, window_seconds: float = 300.0, max_events_per_symbol: int = 500):
        """
        Args:
            window_seconds: Rolling window for liquidation aggregation (default 5 min).
            max_events_per_symbol: Max events in ring buffer per symbol.
        """
        self._buffers: Dict[str, deque] = {}
        self._window_s = window_seconds
        self._max_events = max_events_per_symbol

    def ingest(self, raw: dict) -> None:
        """
        Processes raw WebSocket liquidation message.
        
        Bybit V5 format:
        {
            "topic": "liquidation",
            "data": {
                "symbol": "BTCUSDT",
                "side": "Buy",     # Side of liquidated position
                "price": "30000.5",
                "size": "0.5",
                "updatedTime": 1690000000000
            }
        }
        """
        try:
            data = raw.get('data', raw)  # Handle both wrapped and unwrapped formats
            
            symbol = data.get('symbol', '')
            side = data.get('side', '')
            price = float(data.get('price', 0))
            size = float(data.get('size', 0))
            ts_ms = int(data.get('updatedTime', time.time() * 1000))

            if not symbol or price <= 0 or size <= 0:
                return

            event = LiquidationEvent(
                symbol=symbol,
                side=side,
                price=price,
                size=size,
                value_usd=price * size,
                timestamp_ms=ts_ms
            )

            if symbol not in self._buffers:
                self._buffers[symbol] = deque(maxlen=self._max_events)
            
            self._buffers[symbol].append(event)

        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"[LiqFeed] Malformed liquidation event: {e}")

    def get_liquidation_volume(self, symbol: str, window_s: Optional[float] = None) -> Tuple[float, float, int]:
        """
        Returns aggregated liquidation metrics for a symbol over the rolling window.
        
        Returns:
            Tuple of:
            - total_liq_usd: Total USD value of liquidations
            - short_liq_usd: USD value of SHORT liquidations only (= forced Market Buys)
            - event_count: Number of liquidation events
        """
        buf = self._buffers.get(symbol)
        if not buf:
            return (0.0, 0.0, 0)

        window = window_s or self._window_s
        cutoff_ms = (time.time() - window) * 1000.0

        total_usd = 0.0
        short_liq_usd = 0.0  # Shorts liquidated = forced Buy volume
        count = 0

        for evt in buf:
            if evt.timestamp_ms < cutoff_ms:
                continue
            total_usd += evt.value_usd
            count += 1
            # "Sell" side means SHORT position was liquidated → engine BUYS → creates buy pressure
            if evt.side == "Sell":
                short_liq_usd += evt.value_usd

        return (total_usd, short_liq_usd, count)

    def get_short_liq_ratio(self, symbol: str, total_vpin_volume_usd: float, 
                             window_s: Optional[float] = None) -> float:
        """
        Key metric for LiquidationDiscriminator:
        What fraction of observed VPIN buy volume is actually forced liquidation buys?
        
        Returns:
            ratio ∈ [0, 1]. 
            0.0 = No liquidations (pure organic).
            1.0 = 100% of volume is liquidation-driven.
        """
        if total_vpin_volume_usd <= 0:
            return 0.0

        _, short_liq_usd, _ = self.get_liquidation_volume(symbol, window_s)
        
        # Clamp to [0, 1] — liquidation volume can't exceed total in theory,
        # but data races between feeds may cause brief overcount
        return min(1.0, short_liq_usd / total_vpin_volume_usd)

    def purge_stale(self) -> None:
        """Periodic cleanup of events outside the rolling window."""
        cutoff_ms = (time.time() - self._window_s) * 1000.0
        for symbol, buf in self._buffers.items():
            while buf and buf[0].timestamp_ms < cutoff_ms:
                buf.popleft()

    @property
    def tracked_symbols(self) -> int:
        return len(self._buffers)


# Global singleton — initialized once in Composition Root
liquidation_accumulator = LiquidationAccumulator()
