import asyncio
import time
import struct
from typing import Dict, List, Optional
from multiprocessing import shared_memory
from loguru import logger
from core.events.nerve_center import bus
from core.realtime.market_state import market_state

class MarketHealthSHMBridge:
    """
    [GEKTOR v14.4.6] Institutional-Grade SeqLock SHM Bridge.
    Architecture: Lock-Free Atomic Transfer with Stale Data Awareness.
    
    Structure (40 bytes):
    [0:8]   Unsigned Long Long (Sequence Counter)
    [8:16]  Double (Skew Ratio)
    [16:24] Double (Global Momentum)
    [24:32] Double (Lead Impulse - e.g. ETH velocity)
    [32:40] Double (Lead Timestamp - ts of update from lead asset)
    """
    __slots__ = ('shm', 'is_writer', 'name')

    def __init__(self, is_writer: bool = False):
        self.is_writer = is_writer
        self.name = "health_seqlock_v14_6"
        self.shm = None
        size = 40
        try:
            if self.is_writer:
                self.shm = shared_memory.SharedMemory(create=True, name=self.name, size=size)
                self._write_raw(0, 0.5, 0.0, 0.0, 0.0) # Init
            else:
                try:
                    self.shm = shared_memory.SharedMemory(name=self.name)
                except (FileNotFoundError, Exception):
                    # Reader might start before writer; we'll retry in read_health
                    pass
        except Exception as e:
            if self.is_writer and "FileExistsError" in str(e):
                self.shm = shared_memory.SharedMemory(name=self.name)
            else:
                logger.error(f"❌ [SeqLock] SHM Init Error: {e}")
                raise

    def write_health(self, skew: float, momentum: float, lead_impulse: float = 0.0, lead_ts: float = 0.0):
        """Writer: Atomic update with 40-byte SeqLock footprint."""
        seq = struct.unpack('Q', self.shm.buf[0:8])[0]
        self.shm.buf[0:8] = struct.pack('Q', seq + 1)
        # Update Data: skew, momentum, lead_imp, lead_ts
        self.shm.buf[8:40] = struct.pack('dddd', skew, momentum, lead_impulse, lead_ts)
        self.shm.buf[0:8] = struct.pack('Q', seq + 2)

    def read_health(self) -> tuple[float, float, float, float]:
        """Reader: Spin-loop with 50ms stale data awareness (calculated by consumer)."""
        if self.shm is None:
            try:
                self.shm = shared_memory.SharedMemory(name=self.name)
            except:
                return (0.5, 0.0, 0.0, 0.0) # Default healthy baseline
        
        while True:
            s1 = struct.unpack('Q', self.shm.buf[0:8])[0]
            if s1 % 2 != 0: continue
            
            vals = struct.unpack('dddd', self.shm.buf[8:40])
            
            s2 = struct.unpack('Q', self.shm.buf[0:8])[0]
            if s1 == s2:
                return vals

    def _write_raw(self, seq: int, skew: float, mom: float, imp: float, ts: float):
        self.shm.buf[:40] = struct.pack('Qdddd', seq, skew, mom, imp, ts)

    def cleanup(self):
        self.shm.close()
        if self.is_writer:
            try: self.shm.unlink()
            except: pass

class CrossImpactMonitor:
    """
    [GEKTOR v14.4.2] Institutional Cross-Asset Correlation Engine.
    
    Monitors global market skew (how many assets are trending)
    and updates a shared 'Global Market Health' state in Redis.
    
    Architecture:
    1. Collects 1m price changes for all tracked symbols.
    2. Calculates Skew: (Longs - Shorts) / Total.
    3. Calculates Momentum: Average 1m price change.
    4. Updates 'market_health' key for Zero-GC Correlator consumption.
    """

    UPDATE_INTERVAL = 1.0 # Faster update for HFT awareness
    
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._prices_1m: Dict[str, List[float]] = {s: [] for s in symbols}
        # [GEKTOR v14.4.3] Zero-I/O Bridge
        self.shm_bridge = MarketHealthSHMBridge(is_writer=True)

    async def start(self):
        if self._running: return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"🧬 [CrossImpact] Monitoring {len(self.symbols)} symbols for global correlation.")

    async def _monitor_loop(self):
        """Historical aggregation cycle."""
        while self._running:
            try:
                # 1. Update Snapshot
                snapshot = {}
                trending_long = 0
                trending_short = 0
                avg_momentum = 0.0
                
                for s in self.symbols:
                    p = market_state.get_price(s)
                    if p <= 0: continue
                    
                    # Track simple 1m return
                    self._prices_1m[s].append(p)
                    if len(self._prices_1m[s]) > 20: # ~1 min at 3s intervals
                        self._prices_1m[s].pop(0)
                    
                    if len(self._prices_1m[s]) >= 5:
                        start_p = self._prices_1m[s][0]
                        ret = (p - start_p) / start_p
                        avg_momentum += ret
                        
                        if ret > 0.005: trending_long += 1
                        elif ret < -0.005: trending_short += 1
                
                # 2. Calculate Skew (-1.0 to +1.0)
                total = len(self.symbols)
                skew = (trending_long - trending_short) / total if total > 0 else 0.0
                momentum = avg_momentum / total if total > 0 else 0.0
                
                # 3. Predict Global Impact (Systemic Risk)
                impact_score = abs(skew) # 1.0 = All assets move in sync (High risk)
                
                # 4. Push to SHM (Zero-I/O for AlphaCorrelator)
                self.shm_bridge.write_health(skew, momentum)
                
                # 5. Background logging to Redis (Optional, non-critical)
                health_data = {
                    "skew": round(skew, 3),
                    "momentum_pct": round(momentum * 100, 3),
                    "impact": round(impact_score, 3),
                    "ts": time.time()
                }
                
                import orjson
                await bus.redis.set("market_health:global", orjson.dumps(health_data))
                
                if impact_score > 0.6:
                    logger.warning(f"⚠️ [CrossImpact] SYSTEMIC SKEW DETECTED! Skew: {skew:.2f} | Momentum: {momentum*100:.2f}%")
            except Exception as e:
                logger.error(f"❌ [CrossImpact] Monitor loop error: {e}")
            
            await asyncio.sleep(self.UPDATE_INTERVAL)

    async def stop(self):
        self._running = False
        if self._task: self._task.cancel()
        self.shm_bridge.cleanup()
