# skills/gerald-sniper/core/realtime/consensus.py
import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple
from loguru import logger

class ExchangeStreamState:
    """
    [GEKTOR v21.20] Per-Exchange Temporal Buffer.
    - Kleppmann: Tracking Event Time vs Processing Time.
    - Beazley: Monitoring Monotonic Heartbeats for Liveness detection.
    """
    def __init__(self, name: str):
        self.name = name
        self.latest_event_ts: int = 0
        self.last_local_arrival: float = 0.0
        self.buffer: List[Tuple[int, Any]] = []
        self.is_stale = False
        self.is_quarantined = True # Start in quarantine until synced

class WatermarkAligner:
    """
    [GEKTOR v21.20] Multi-Exchange Event-Time Aligner.
    Synchronizes streams from Binance/Bybit/OKX to eliminate "Phantom Breakouts".
    Implements Liveness Threshold (Idle Eviction) to prevent "Straggler Freeze".
    """
    def __init__(self, exchanges: List[str], max_drift_ms: int = 150):
        self._streams = {ex: ExchangeStreamState(ex) for ex in exchanges}
        self._global_watermark: int = 0
        self._max_drift = max_drift_ms
        self._liveness_threshold = 2.5 # Silence before eviction (seconds)
        
        self._watermark_advanced = asyncio.Event()
        self._lock = asyncio.Lock()
        
    def on_tick(self, exchange: str, exchange_ts: int, payload: Any):
        """Hot Path (O(1)): Ingests ticks into specific exchange buffers."""
        stream = self._streams.get(exchange)
        if not stream: return
        
        now = time.monotonic()
        
        # 1. Update Heartbeat
        stream.last_local_arrival = now
        if stream.is_stale:
            logger.warning(f"🔄 [{exchange}] Heartbeat RESTORED. Ending eviction.")
            stream.is_stale = False
            # Stream enters Quarantine to avoid "Time Jump" injection
            stream.is_quarantined = True 

        # 2. Sequential Check (Event Time Monotonicity)
        if exchange_ts < stream.latest_event_ts:
            return # Drop late event

        stream.latest_event_ts = exchange_ts
        stream.buffer.append((exchange_ts, payload))
        
        # 3. Synchronous Watermark Update check
        self._check_logic_time()

    def _check_logic_time(self):
        """
        Recalculates the Global Watermark based on 'Alive' streams.
        A stream is 'Alive' if it has recent data and is not stale.
        """
        active_streams = [
            s for s in self._streams.values() 
            if not s.is_stale and s.latest_event_ts > 0
        ]
        
        if not active_streams:
            return

        # Consensus: Watermark = MIN(EventTime) - Drift
        # This prevents one биржу from being 'future-ahead' of another.
        min_ts = min(s.latest_event_ts for s in active_streams)
        new_watermark = min_ts - self._max_drift
        
        if new_watermark > self._global_watermark:
            self._global_watermark = new_watermark
            self._watermark_advanced.set()

    async def liveness_watchdog_loop(self):
        """
        [ArjanCodes] Idle Eviction Guard.
        Periodically checks for silent streams and evicts them to prevent Global Freeze.
        """
        while True:
            try:
                now = time.monotonic()
                async with self._lock:
                    for name, s in self._streams.items():
                        if not s.is_stale and s.last_local_arrival > 0:
                            silence = now - s.last_local_arrival
                            if silence > self._liveness_threshold:
                                logger.critical(f"💀 [{name}] SILENCE ({silence:.1f}s). Evicting from Watermark.")
                                s.is_stale = True
                                # Recalculate watermark immediately to resume consensus for remaining streams
                                self._check_logic_time()
                
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"❌ [Watchdog] Consensus Error: {e}")
                await asyncio.sleep(2)

    async def consensus_loop(self, callback=None):
        """
        [Beazley] Non-blocking Processing Loop.
        Only wakes up when the Global Watermark moves forward.
        """
        logger.info(f"⚡ [Consensus] Multi-Exchange Alignment Active. Max Drift: {self._max_drift}ms")
        while True:
            await self._watermark_advanced.wait()
            self._watermark_advanced.clear()
            
            # Extract data validated by Watermark (Safe for VPIN)
            batch = self._drain_confirmed_data(self._global_watermark)
            if batch:
                await self._process_consensus_batch(batch)
                if callback:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(batch)
                    else:
                        callback(batch)

    def _drain_confirmed_data(self, watermark: int) -> List[Any]:
        """Atomically extracts all data from ALL streams up to the Watermark line."""
        final_batch = []
        for s in self._streams.values():
            if s.is_stale: 
                # Evicted streams cannot contribute to logic time
                continue 
                
            idx = 0
            for i, (ts, data) in enumerate(s.buffer):
                if ts <= watermark:
                    final_batch.append(data)
                    idx = i + 1
                else: break
            
            # Trim the buffer (In production, replace with Zero-Alloc Ring Buffer logic)
            s.buffer = s.buffer[idx:]
            
            # Lifting Quarantine: Stream is now caught up to Global Time
            if s.is_quarantined and s.buffer:
                s.is_quarantined = False 
                
        return final_batch

    async def _process_consensus_batch(self, batch: List[Any]):
        """
        Consolidated Market Signal Generation.
        Data is now Causally Consistent across exchanges.
        """
        # Logic for Cross-Exchange VPIN / Arbitrage / Global Divergence
        # Dispatch to MLMathEngine via ProcessPoolExecutor
        pass

# Global Aligner instance for Binance, Bybit, OKX signals
consensus_aligner = WatermarkAligner(["binance", "bybit", "okx"])
