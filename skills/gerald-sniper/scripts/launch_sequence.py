import asyncio
import time
from loguru import logger

# LAUNCH SEQUENCE PROTOCOL (v5.23 - LSP)
# Role: Connection Warm-up & JIT Stabilization before LIVE Fire.
# Audit 16.32: TCP/TLS Handshake priming.

class LaunchSequence:
    def __init__(self, bybit_client, binance_client, settings):
        self.clients = [bybit_client, binance_client]
        self.settings = settings
        self.is_ready = False

    async def warm_up(self, cycles: int = 50):
        """
        Audit 16.32: Warm-up TCP/TLS connections and stabilize Python Event Loop.
        Eliminates 'Cold Start' latency for the first order.
        """
        logger.warning(f"🔥 [WARMUP] Initiating Connection Priming ({cycles} cycles)...")
        
        latency_samples = []
        for i in range(cycles):
            try:
                start = time.perf_counter()
                # Fan-out to all exchange endpoints
                await asyncio.gather(*[client.get_server_time() for client in self.clients])
                rtt = (time.perf_counter() - start) * 1000
                latency_samples.append(rtt)
                
                if i % 10 == 0:
                     logger.debug(f"Warmup Cycle {i}: RTT {rtt:.2f}ms")
                
                # Minimum interval to stabilize loop
                await asyncio.sleep(0.01)
            except Exception as e:
                logger.error(f"⚠️ [WARMUP_FAIL] Cycle {i} error: {e}")

        avg_rtt = sum(latency_samples) / len(latency_samples)
        p95_rtt = sorted(latency_samples)[int(len(latency_samples) * 0.95)]
        
        logger.success(f"🚀 [WARMUP_COMPLETE] Average RTT: {avg_rtt:.2f}ms | P95: {p95_rtt:.2f}ms")
        
        if avg_rtt > self.settings.LATENCY_THRESHOLD_MS:
             logger.critical(f"🛑 [NETWORK_HALT] Latency {avg_rtt:.2f}ms exceeds threshold! Check Proxy/Region.")
             return False
        
        self.is_ready = True
        return True

    async def pre_flight_checks(self, reconciler, tracker):
        """Final integrity validation (Audit 16.30/16.31)."""
        logger.info("🛡 [PRE-FLIGHT] Verifying Atomic Integrity (Satoshi Match)...")
        
        # 1. Integrity check
        if not await reconciler.shield.verify_integrity(tracker.get_current_qty()):
             return False
             
        # 2. Reset Epoch to ensure clean slate
        await reconciler.perform_hardened_atomic_reset(new_anchor_id=0) # Reset to latest truth
        
        logger.success("🏁 [LSP] System is Armed and Synchronized.")
        return True
