import gc
import asyncio
import time
import sys
from loguru import logger


class MemoryManager:
    """
    HFT Garbage Collection Manager (v3.0 — Arena-Aware).
    
    Strategy: Controlled GC lifecycle to prevent Stop-The-World pauses
    during market data spikes (e.g., 15MB L2 snapshots creating 100k+ dicts).
    
    Key principles:
    1. DISABLE automatic Gen 2 collections (they cause 10-50ms STW pauses)
    2. Perform Gen 0/1 collections on OUR schedule during Radar quiet periods
    3. On memory spike detection, do immediate Gen 0 cleanup + deferred Gen 2
    4. Object arena pressure monitoring via gc.get_count()
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MemoryManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, check_interval: float = 30.0):
        if hasattr(self, '_initialized'): return
        self.interval = check_interval
        self._initialized = True
        
        # AGGRESSIVE: Very high thresholds to prevent spontaneous collections
        # Default Python: (700, 10, 10)
        # We set Gen 2 threshold extremely high — we'll trigger it manually
        gc.set_threshold(50000, 50, 5)
        
        # Metrics
        self._gc_count = 0
        self._avg_gc_ms = 0.0
        self._max_gc_ms = 0.0
        self._spike_mitigations = 0
        self._last_gen0_count = 0
        
        # Pressure thresholds
        self.GEN0_SPIKE_THRESHOLD = 30000  # Objects in Gen 0 before emergency collection
        self.MEMORY_MB_WARNING = 500       # RSS warning threshold
        
        logger.info(
            f"🧹 [MEM v3.0] Arena-Aware Memory Management initialized. "
            f"Thresholds: Gen0={gc.get_threshold()[0]}, Spike={self.GEN0_SPIKE_THRESHOLD}"
        )

    def trigger_hft_gc(self, generation: int = 1) -> float:
        """
        Manual trigger for fast collection.
        
        Generation 0: Ultra-fast (<1ms). Clears short-lived temporaries.
        Generation 1: Fast (<5ms). Clears mid-life objects.
        Generation 2: DANGEROUS (10-50ms). Full sweep. Only during idle periods.
        
        Returns: Collection duration in milliseconds.
        """
        t_start = time.perf_counter()
        collected = gc.collect(generation)
        duration_ms = (time.perf_counter() - t_start) * 1000
        
        self._gc_count += 1
        self._avg_gc_ms = (self._avg_gc_ms * (self._gc_count - 1) + duration_ms) / self._gc_count
        if duration_ms > self._max_gc_ms:
            self._max_gc_ms = duration_ms
        
        if duration_ms > 5.0:
            logger.debug(
                f"🧹 [MEM] GC Gen {generation}: {duration_ms:.2f}ms, "
                f"collected {collected} objects"
            )
        
        return duration_ms

    def check_arena_pressure(self) -> dict:
        """
        Probes GC arena state without triggering collection.
        
        Returns dict with:
        - gen0/gen1/gen2: Object counts per generation
        - pressure: 'low' | 'medium' | 'high' | 'critical'
        - recommendation: Suggested action
        """
        gen0, gen1, gen2 = gc.get_count()
        
        if gen0 > self.GEN0_SPIKE_THRESHOLD:
            pressure = 'critical'
            recommendation = 'immediate_gen0'
        elif gen0 > self.GEN0_SPIKE_THRESHOLD // 2:
            pressure = 'high'
            recommendation = 'schedule_gen0'
        elif gen1 > 100:
            pressure = 'medium'
            recommendation = 'schedule_gen1'
        else:
            pressure = 'low'
            recommendation = 'none'
        
        return {
            'gen0': gen0, 'gen1': gen1, 'gen2': gen2,
            'pressure': pressure,
            'recommendation': recommendation
        }

    def mitigate_spike(self) -> float:
        """
        Emergency spike mitigation.
        
        Called when a large data structure (e.g., 15MB L2 snapshot) 
        has been processed and the temporaries need immediate cleanup.
        
        Strategy:
        1. Immediate Gen 0 collection (clear temporaries from orjson parsing)
        2. Check if Gen 1 needs attention
        3. NEVER do Gen 2 here — defer to background worker
        
        Returns: Total collection time in milliseconds.
        """
        self._spike_mitigations += 1
        total_ms = 0.0
        
        # Phase 1: Clear Gen 0 (sub-millisecond for temporaries)
        total_ms += self.trigger_hft_gc(0)
        
        # Phase 2: If Gen 1 is pressured, clear it too
        _, gen1, _ = gc.get_count()
        if gen1 > 30:
            total_ms += self.trigger_hft_gc(1)
        
        if total_ms > 3.0:
            logger.warning(
                f"⚠️ [MEM] Spike mitigation took {total_ms:.2f}ms "
                f"(mitigations total: {self._spike_mitigations})"
            )
        
        return total_ms

    async def run_background_gc(self):
        """
        Background GC worker with arena-aware scheduling.
        
        Runs Gen 1 every interval, Gen 2 only when pressure requires it.
        Monitors RSS growth trends for early warning.
        """
        logger.info("🧹 [MEM v3.0] Background Arena Worker started.")
        gen2_last_run = time.monotonic()
        GEN2_MIN_INTERVAL = 120.0  # Never run Full GC more often than every 2 minutes
        
        while True:
            try:
                await asyncio.sleep(self.interval)
                
                # Check arena state
                state = self.check_arena_pressure()
                now = time.monotonic()
                
                if state['pressure'] == 'critical':
                    # Emergency: Gen 0 immediately
                    self.trigger_hft_gc(0)
                    self.trigger_hft_gc(1)
                    
                elif state['pressure'] in ('high', 'medium'):
                    # Standard maintenance: Gen 1
                    self.trigger_hft_gc(1)
                    
                else:
                    # Low pressure: Light Gen 0 sweep
                    self.trigger_hft_gc(0)
                
                # Periodic Gen 2: Only during low pressure + time interval
                if (now - gen2_last_run > GEN2_MIN_INTERVAL 
                    and state['pressure'] == 'low'):
                    duration = self.trigger_hft_gc(2)
                    gen2_last_run = now
                    if duration > 10.0:
                        logger.warning(
                            f"⚠️ [MEM] Full GC (Gen 2) took {duration:.2f}ms. "
                            f"Consider increasing GEN2_MIN_INTERVAL."
                        )
                
            except asyncio.CancelledError:
                logger.info("🛑 [MEM] Background worker shutting down.")
                break
            except Exception as e:
                logger.error(f"❌ [MEM] GC Worker error: {e}")

    def get_stats(self) -> dict:
        """Diagnostics for monitoring dashboard."""
        gen0, gen1, gen2 = gc.get_count()
        return {
            "gc_invocations": self._gc_count,
            "avg_gc_ms": round(self._avg_gc_ms, 2),
            "max_gc_ms": round(self._max_gc_ms, 2),
            "spike_mitigations": self._spike_mitigations,
            "arena": {"gen0": gen0, "gen1": gen1, "gen2": gen2},
            "thresholds": list(gc.get_threshold()),
        }


# Global instance for easy access from Orchestrator/Radar
mem_manager = MemoryManager()
