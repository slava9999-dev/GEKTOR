import os
import sys
import asyncio
import gc
import json
import time
import signal
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
import psutil

# [SCARLETT v10.0] Fundamental Infrustructure Imports
from core.events.nerve_center import bus
from core.events.dispatcher import dispatcher as signal_dispatcher
# Execution Consumers Removed for Radar-Only mode

# [GEKTOR v10.1] Windows Process Elevators
def set_cpu_affinity():
    try:
        proc = psutil.Process(os.getpid())
        count = psutil.cpu_count()
        target = [c for c in [2, 3] if c < count]
        if target:
            proc.cpu_affinity(target)
            logger.info(f"🎯 [CoreMatrix] CPU_AFFINITY: Pinned to cores {target}.")
    except Exception as e:
        logger.warning(f"⚠️ [Runtime] Failed to set CPU affinity: {e}")

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        proc = psutil.Process(os.getpid())
        proc.nice(psutil.HIGH_PRIORITY_CLASS)
        logger.info("⚡ [Matrix] Process Priority: Set to HIGH.")
        set_cpu_affinity()
    except Exception as e:
        logger.warning(f"⚠️ [Runtime] Failed to elevate priority: {e}")

# Load Configuration
PROJECT_ROOT = Path(__file__).resolve().parents[2]
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path, override=True)

from data.bybit_rest import BybitREST
from data.swarm.manager import SwarmManager as BybitWSManager
from data.database import DatabaseManager
from utils.config import config
from core.candle_cache import CandleCache
from data.bybit_ws_private import BybitPrivateWSManager
from core.runtime.tasks import safe_task, TaskRegistry, setup_loop_exception_handler
from services.level_service import LevelService
from core.realtime.detectors import antimiss_system
from core.scenario.heartbeat import detector_heartbeat_loop
from core.realtime.market_state import market_state
from core.realtime.bridge import bridge
from core.metrics.metrics import metrics
from core.events.recorder import recorder
from core.scenario.tactical_orchestrator import tactical_orchestrator
from core.position_tracker import PaperTracker
from data.bybit_instruments import InstrumentsProvider
from data.swarm.sentiment_worker import NeuralSentinel as AnalysisWorker
from core.alerts.bot_listener import start_bot_listener
from core.alerts.outbox import telegram_outbox
from core.radar.macro_radar import MacroRadar
from core.radar.universe import DynamicUniverseShaker
from core.shield.amputation import get_amputation_protocol
from pre_flight import PreflightCommander, PeriodicKeyValidator
from core.events.nerve_center import NerveCenter

# [Watchdog v8.3 Import]
from main import VitalsWatchdog, ResourceMonitor, GracefulKiller, shutdown_system

async def main():
    from core.utils.pid_lock import PIDLock
    with PIDLock("sniper_main"):
        setup_loop_exception_handler()
        stop_event = asyncio.Event()
        killer = GracefulKiller(stop_event)

        # 1. Infrastructure Boot
        db_manager = DatabaseManager()
        await db_manager.initialize()
        
        rest_client = BybitREST(base_url=config.bybit.rest_url)
        ws_manager = BybitWSManager(stream_url=config.bybit.ws_url)
        market_state.set_clients(rest_client, ws_manager)
        
        paper_tracker = PaperTracker(db_manager)
        candle_mgr = CandleCache(rest_client, db=db_manager)
        
        trading_pairs = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"]
        instruments_mgr = InstrumentsProvider(rest_client, whitelist=trading_pairs)
        await instruments_mgr.start_background_sync(interval_seconds=3600)
        
        # 2. Pre-Flight Check (Fail-Fast)
        system_mode = os.getenv("SYSTEM_MODE", "RADAR_ONLY").upper()
        preflight = PreflightCommander(
            rest_client=rest_client,
            mode="ADVISORY" if not os.getenv("BYBIT_API_KEY") else system_mode
        )
        await preflight.execute()
        
        # 3. Memory Optimization
        gc.collect()
        gc.freeze()
        logger.info("❄️ [Runtime] GC Freeze Engaged.")

        # 4. Global Orchestrator Startup (Decision Core)
        # Background tasks like detector_heartbeat_loop depend on this singleton
        await tactical_orchestrator.start(
            db_manager=db_manager,
            paper_tracker=paper_tracker,
            candle_cache=candle_mgr
        )

        from core.runtime.time_sync import TimeSynchronizer
        from core.runtime import time_sync as ts_module
        ts_module.time_sync = TimeSynchronizer(rest_client)
        await ts_module.time_sync.start()

        # 5. Radar-Only Fan-Out initialization
        from core.events.events import SignalEvent, ManualExecutionEvent
        # [GEKTOR v10.0] Direct Bus Routing for Radar signals
        # SignalDispatcher is now strictly for Advisory Fan-out to Notification Hubs
        bus.subscribe(SignalEvent, signal_dispatcher.broadcast)
        bus.subscribe(ManualExecutionEvent, signal_dispatcher.broadcast)

        # Initialize single Radar Context for Advisory signals
        # (TradingContext removed - replaced by direct Radar logic)
        logger.info("📡 [Radar-Only] Contexts: Execution Layer Amputated.")

        # Parallel Start of essential loops
        level_service = LevelService(rest_client)
        await asyncio.gather(
            # Start background infrastructure only
            bus.start_listening(),
        )

        if system_mode == "LIVE":
            from core.events.outbox_relay import outbox_relay
            outbox_relay.configure(db_manager, combat_bus)
            await outbox_relay.start()
            logger.critical("🔥 [MODE] LIVE: Execution Contexts ACTIVE.")
        
        # 6. Background Daemons
        res_monitor = ResourceMonitor(stop_event, ws_manager, candle_mgr)
        from core.health.heartbeat_emitter import heartbeat_emitter
        
        shaker = None
        shaker_task = None
        macro_radar = None
        macro_radar_task = None
        # Load macro_radar if enabled
        try:
            import yaml
            cfg_path = Path(__file__).parent / "config_sniper.yaml"
            with open(cfg_path, 'r', encoding='utf-8') as f:
                full_cfg = yaml.safe_load(f)
            macro_cfg = full_cfg.get("macro_radar", {})
            
            # [GEKTOR v11.9] Universe Shaker Integration
            shaker = DynamicUniverseShaker(
                rest=rest_client,
                redis=bus.redis,
                min_turnover_usd=macro_cfg.get("universe_turnover_floor", 50_000_000),
                max_spread_bps=macro_cfg.get("universe_spread_cap_bps", 15.0)
            )
            shaker_task = safe_task(shaker.start(), name="universe_shaker")
            
            if macro_cfg.get("enabled", True):
                macro_radar = MacroRadar(
                    rest=rest_client,
                    config=macro_cfg,
                    scan_interval_sec=macro_cfg.get("scan_interval_sec", 60),
                    dispatcher=signal_dispatcher
                )
                macro_radar_task = safe_task(macro_radar.start(), name="macro_radar")
        except Exception as e:
            logger.error(f"❌ [Main] Shaker/Radar initialization failed: {e}")

        tasks = [
            safe_task(res_monitor.run(), name="res_monitor"),
            safe_task(heartbeat_emitter.run(stop_event), name="heartbeat_cloud"),
            safe_task(metrics.start(), name="metrics"),
            safe_task(recorder.start(), name="recorder"),
            safe_task(AnalysisWorker().run(), name="ai"),
            # detector_heartbeat_loop uses the singleton tactical_orchestrator
            safe_task(detector_heartbeat_loop(market_state, antimiss_system, tactical_orchestrator), name="heartbeat"),
        ]
        
        # 7. Watchdog initialization
        watchdog = VitalsWatchdog(stop_event)
        for t in tasks:
            if t.get_name() in ("res_monitor", "heartbeat_cloud", "heartbeat"):
                watchdog.register(t.get_name(), t)
        
        if macro_radar_task:
            watchdog.register("macro_radar", macro_radar_task, restart_factory=lambda: safe_task(macro_radar.start(), name="macro_radar"))
            tasks.append(macro_radar_task)
            
        if shaker_task:
            watchdog.register("universe_shaker", shaker_task, restart_factory=lambda: safe_task(shaker.start(), name="universe_shaker"))
            tasks.append(shaker_task)

        watchdog_task = safe_task(watchdog.run(), name="vitals_watchdog")
        tasks.append(watchdog_task)

        # 8. Final Services
        # Amputation Protocol: Minimal Radar footprint
        get_amputation_protocol(rest_client=rest_client, kill_switch=None, bus=bus)
        await telegram_outbox.start()
        bridge.start() # Starts RealtimeStateManager
        
        # Bot listener with global orchestrator
        tasks.append(safe_task(start_bot_listener(
            paper_tracker=paper_tracker, db=db_manager, stop_event=stop_event,
            macro_radar=macro_radar, orchestrator=tactical_orchestrator
        ), name="tg_listener"))

        logger.success(f"🚀 Gerald Sniper fully ARMED in {system_mode} mode.")
        
        await stop_event.wait()
        await shutdown_system(tasks, db_manager, rest_client, ws_manager)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
