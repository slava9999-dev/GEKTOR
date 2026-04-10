# skills/gerald_sniper/core/quant/gap_guard.py
import asyncio
from loguru import logger
import time
from typing import Optional, List, Dict
from .bar_engine import AdaptiveDollarBarEngine
from ..realtime.deduplicator import TickDeduplicator
from .quarantine import StateInvalidationError

class VolumeGapGuard:
    """
    [GEKTOR v21.58] Institutional Integrity Guard with Strict DI.
    """
    def __init__(
        self, 
        bar_engine: AdaptiveDollarBarEngine, 
        rest_client, 
        dedup: TickDeduplicator, 
        *, 
        on_integrity_violation: Optional[callable] = None,
        signal_router=None, 
        shadow_tracker=None, 
        max_buffer_size: int = 100_000,
        shutdown_event: Optional[asyncio.Event] = None
    ):
        self.bar_engine = bar_engine
        self.rest = rest_client
        self.dedup = dedup
        self.on_integrity_violation = on_integrity_violation
        self.signal_router = signal_router
        self.shadow_tracker = shadow_tracker
        self.max_buffer_size = max_buffer_size
        self.shutdown_event = shutdown_event
        self.source = None # Will be set to Ingestor
        
        self.last_ts = 0
        self._is_recovering = False
        self._live_buffer = asyncio.Queue()
        self.is_startup = True
        self._compromised = False
        logger.info("🛡️ [GapGuard] Initialized with strict DI.")

    async def on_new_tick(self, tick: dict):
        """
        Hot Path: Causal synchronization.
        """
        if self._compromised:
            return

        current_ts = int(tick.get('E', 0))
        trade_id = tick.get('i', '')
        
        # 1. VALIDATE DE-DUPLICATION
        if self.dedup.is_duplicate(tick):
            return 

        # 2. GAP DETECTION & STARTUP SEED
        if not self._is_recovering:
            if self.last_ts == 0:
                # [GEKTOR v21.56] Startup Seed: Fetch last 60s to bridge the "Ignorance Gap"
                logger.info("🌱 [GapGuard] Startup Seed: Pre-warming engine with 60s history.")
                self._is_recovering = True
                asyncio.create_task(self._run_backfill(current_ts - 60_000, current_ts))
            else:
                gap = current_ts - self.last_ts
                if gap > 1000:
                    logger.warning(f"⚠️ [GapGuard] Sequence Gap Detected: {gap}ms. Initiating recovery...")
                    if self.on_integrity_violation:
                        # Report gap for the INGESTOR's proxy, as it's the one that missed ticks
                        current_proxy = getattr(self.source, 'current_proxy', None)
                        if current_proxy:
                            self.on_integrity_violation(current_proxy)
                    
                    self._is_recovering = True
                    asyncio.create_task(self._run_backfill(self.last_ts, current_ts))

        # 3. DATA ROUTING
        is_quarantined = getattr(self.bar_engine, 'grid', None) and self.bar_engine.grid.is_quarantined
        
        if self._is_recovering or is_quarantined:
            if is_quarantined and not self._is_recovering:
                # If we are quarantined but NOT in recovery, we might need a backfill 
                # to bridge the lag gap or just wait for stability.
                self._is_recovering = True
                logger.warning("🛡️ [GapGuard] Transitioning to RECOVERY.")
            
            if self._live_buffer.qsize() >= self.max_buffer_size:
                await self._handle_capitulation()
                return

            await self._live_buffer.put(tick)
        else:
            try:
                # Standard Flow: Send to math AND routing
                bar = await self.bar_engine.process_tick(
                    symbol=tick.get('s', ''),
                    ts=current_ts / 1000.0,
                    price=tick.get('p', 0.0),
                    volume=tick.get('q', 0.0),
                    is_buy=(tick.get('side') == 'B'),
                    trade_id=trade_id
                )
                if bar:
                    await self.signal_router.on_new_bar(bar)
            except Exception as e:
                logger.error(f"☠️ [GapGuard] Math core failure: {e}")
                self._compromised = True
                
            self.last_ts = current_ts

    async def force_cold_restart(self):
        """[GEKTOR v21.63] Clean slate for post-hibernation wake."""
        self.last_ts = 0
        self.is_startup = True
        self._is_recovering = False
        # Clear buffer
        while not self._live_buffer.empty():
            try: self._live_buffer.get_nowait()
            except asyncio.QueueEmpty: break
        self.bar_engine.reset_math_state()
        logger.warning("❄️ [GapGuard] Cold Restart Force-Triggered.")

    async def _run_backfill(self, start_ts: int, end_ts: int):
        """[GEKTOR v21.60.2] Guaranteed seed loading or math reset."""
        task = asyncio.current_task()
        if task: task.set_name("Backfill")
        
        MAX_BACKFILL_GAP = 60000 
        gap_size = end_ts - start_ts
        
        if gap_size > MAX_BACKFILL_GAP:
            logger.critical(f"🚨 [GapGuard] Gap too large ({gap_size}ms). Forcing COLD RESTART.")
            self.bar_engine.reset_math_state()
            self._is_recovering = False
            return

        try:
            logger.info(f"🧬 [Backfill] Attempting Warm Recovery for gap {gap_size}ms...")
            all_missing_ticks = []
            symbols = list(self.bar_engine.states.keys())
            if not symbols: symbols = ["BTCUSDT"]
            
            for symbol in symbols:
                raw_trades = await self.rest.get_recent_trades(symbol)
                for trade in raw_trades:
                    trade_ts = int(trade["time"])
                    if start_ts < trade_ts < end_ts:
                        all_missing_ticks.append({
                            "E": trade_ts, "p": float(trade["price"]),
                            "q": float(trade.get("size", 0)), 
                            "s": trade.get("symbol", symbol),
                            "i": trade.get("execId", ""),
                            "side": "B" if trade["side"].lower().startswith("b") else "S"
                        })

            if all_missing_ticks:
                all_missing_ticks.sort(key=lambda x: x["E"])
                logger.info(f"📥 [Backfill] Replaying {len(all_missing_ticks)} missing ticks.")
                for tick in all_missing_ticks:
                    if self.dedup.is_duplicate(tick): continue
                    await self.bar_engine.process_tick(
                        symbol=tick["s"], ts=tick["E"] / 1000.0,
                        price=tick["p"], volume=tick["q"],
                        is_buy=(tick["side"] == "B"), trade_id=tick["i"]
                    )
                
                corrected_now = int(self.bar_engine.grid.sync.get_synchronized_time())
                current_lag = abs(corrected_now - end_ts)
                
                # [GEKTOR v21.62] Adaptive Threshold: Stale-Data Exclusion
                # Позволяем прогреву длиться дольше, но ограничиваем его временем самого гэпа
                from .quarantine import LatencyGate
                
                adaptive_limit = (gap_size * 0.5) + 1000 
                if self.is_startup:
                    # В режиме SWING даем еще больше времени на холодный старт
                    adaptive_limit = 5000 if LatencyGate.PROFILE == "HFT" else 15000
                    
                if current_lag > adaptive_limit:
                    logger.warning(f"🐢 [GapGuard] Recovery too expensive ({current_lag}ms vs {adaptive_limit:.0f}ms). State poisoned.")
                    self.bar_engine.reset_math_state()
                    self._is_recovering = False
                    return

                logger.success(f"✅ [Backfill] Warm Recovery complete. Lag: {current_lag}ms.")
                self.is_startup = False # Successfully calibrated
            else:
                logger.warning(f"⚠️ [Backfill] No history found for {gap_size}ms. Forcing Math Reset.")
                self.bar_engine.reset_math_state()

            await self._drain_recovery_buffer()
            self.last_ts = end_ts
        except Exception as e:
            logger.error(f"💥 [Backfill] Failure: {e}. Forcing Cold Restart.")
            self.bar_engine.reset_math_state()
        finally:
            self._is_recovering = False

    async def _drain_recovery_buffer(self):
        """Replay buffered live ticks through the engine (v21.61)."""
        count = self._live_buffer.qsize()
        if count > 0:
            logger.info(f"⚡ [GapGuard] Draining live buffer: {count} ticks.")
            processed = 0
            
            # [GEKTOR v21.64] Prevent state oscillation during burst drain
            if self.bar_engine.grid:
                self.bar_engine.grid.is_draining = True
                
            try:
                while not self._live_buffer.empty():
                    if self.shutdown_event and self.shutdown_event.is_set():
                        logger.warning("🛑 [GapGuard] Drain aborted by shutdown signal.")
                        break
    
                    tick = await self._live_buffer.get()
                    current_ts = int(tick.get('E', 0))
                    trade_id = tick.get('i', '')
                    
                    # [GEKTOR v21.61] Burst/Integrity check inside drain
                    try:
                        self.bar_engine.grid.inspect_tick(current_ts)
                    except Exception as e:
                        logger.error(f"🌊 [GapGuard] Burst/Integrity failure in buffer: {e}. Clearing.")
                        while not self._live_buffer.empty():
                            try: self._live_buffer.get_nowait()
                            except asyncio.QueueEmpty: break
                        break
    
                    try:
                        bar = await self.bar_engine.process_tick(
                            symbol=tick.get('s', ''),
                            ts=current_ts / 1000.0,
                            price=tick.get('p', 0.0),
                            volume=tick.get('q', 0.0),
                            is_buy=(tick.get('side') == 'B'),
                            trade_id=trade_id
                        )
                        
                        is_quarantined = self.bar_engine.grid.is_quarantined
                        if bar and not is_quarantined and self.signal_router:
                            await self.signal_router.on_new_bar(bar)
                            
                    except Exception as e:
                        logger.error(f"Drain error: {e}")
                    
                    self.last_ts = current_ts
                    processed += 1
                    if processed % 50 == 0:
                        await asyncio.sleep(0) # Yield for watchdog/health checks
            finally:
                if self.bar_engine.grid:
                    self.bar_engine.grid.is_draining = False

    async def _handle_capitulation(self):
        """[GEKTOR v21.32] Adaptive Failure Mode."""
        logger.critical("🚨 [GapGuard] BUFFER OVERFLOW. CAPITULATING.")
        self._compromised = True
        self._is_recovering = False
        while not self._live_buffer.empty():
            try: self._live_buffer.get_nowait()
            except asyncio.QueueEmpty: break
        logger.warning("🏳️ [GapGuard] Blind mode ACTIVE. Waiting for Cold Sync.")
        # In a real system, we'd trigger a signal to GektorAPEXDaemon to restart or resync with snapshot
