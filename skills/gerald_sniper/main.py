# main.py
import os
import sys
import asyncio
import signal
import gc
import logging
from contextlib import AsyncExitStack
from loguru import logger
from dotenv import load_dotenv

# [GEKTOR v21.3] Composition Root & Lifecycle Management
# ArjanCodes/David Beazley Institutional Standard.
# -----------------------------------------------------------------------------

# 1. Environment & Event Loop Setup
load_dotenv()
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Core Imports
from data.database import DatabaseManager
from data.bybit_rest import BybitREST
from data.swarm.manager import SwarmManager as BybitWSManager
# core/radar/monitoring.py
from core.neural.labeler import AsyncMetaLabeler
from core.radar.scoring_engine import SignalScoringEngine
from core.db.outbox import OutboxRepository, OutboxRelay
from core.radar.rehydration import StateRehydrator
from core.radar.monitoring import MonitoringOrchestrator
from utils.config import config
from core.events.nerve_center import bus
from core.system.monitor import set_high_priority, set_cpu_affinity

class GektorApplication:
    """
    [GEKTOR v21.3] Institutional Composition Root.
    Orchestrates the lifecycle of all asynchronous and process-bound dependencies.
    Provides Graceful Shutdown and State Rehydration.
    """
    def __init__(self, db_path: str = "gektor_analytical.db"):
        self.db_path = db_path
        self.shutdown_event = asyncio.Event()
        self.tasks: list[asyncio.Task] = []

    def _setup_signal_handlers(self):
        """SIGINT/SIGTERM handlers (David Beazley Standards)."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._request_shutdown, sig)
            except NotImplementedError:
                # Fallback for Windows (handled by try/except Block in startup)
                pass

    def _request_shutdown(self, sig: signal.Signals):
        logger.critical(f"🛑 [Lifecycle] Signal-{sig.name} detected. Initiating Graceful Shutdown...")
        self.shutdown_event.set()

    async def run(self):
        """
        Main composition sequence.
        Wires all components together using an AsyncExitStack to guarantee 
        cleanup of unmanaged resources (ProcessPools, SQL Connections).
        """
        self._setup_signal_handlers()
        logger.info("🔱 GEKTOR APEX v21.3: Institutional Bootloader active.")

        async with AsyncExitStack() as stack:
            # 1. Resource: Database Manager (PostgreSQL + SQLite)
            db = DatabaseManager()
            await db.initialize()
            stack.callback(db.close) # Ensure Dispose on exit
            
            # 2. Resource: ML Meta-Labeler (ProcessPoolExecutor isolated)
            # Model path pulled from config (Institutional Standard)
            model_path = os.getenv("ML_MODEL_PATH", "models/macro_rf_v2.pkl")
            labeler = AsyncMetaLabeler(
                model_path=model_path, 
                feature_names=['vpin', 'volatility', 'velocity', 'volume_spike', 'ofi', 'rsi']
            )
            # Synchronous load performed before Event Loop fully saturates
            labeler._load_model_sync() 
            stack.callback(labeler.shutdown) # Kills zombie processes

            # 3. Component instantiation (Dependency Injection)
            outbox_repo = OutboxRepository(self.db_path)
            scoring_engine = SignalScoringEngine(
                labeler=labeler, 
                outbox_repo=outbox_repo,
                threshold=config.macro_radar.get("ml_threshold", 0.65)
            )
            
            # 4. State Rehydration & Phase 5 Monitoring
            rehydrator = StateRehydrator(self.db_path)
            # Fetch active contexts from DB; they'll be injected into orchestrator
            active_contexts_list = await db.fetch_active_contexts()
            
            monitoring_orchestrator = MonitoringOrchestrator(
                db_manager=db,
                inversion_threshold=config.macro_radar.get("inversion_threshold", -0.45),
                max_idle_time=config.macro_radar.get("max_idle_time", 14400)
            )
            await monitoring_orchestrator.hydrate_from_db()
            
            # 5. Infrastructure setup (Networking)
            rest_client = BybitREST(base_url=config.bybit.rest_url)
            ws_manager = BybitWSManager(stream_url=config.bybit.ws_url)
            
            # 6. Spawning Background Tasks
            relay = OutboxRelay(self.db_path, alert_handler=None) # Placeholder for TG Handler
            
            self.tasks = [
                asyncio.create_task(relay.run(self.shutdown_event), name="Outbox_Relay"),
                asyncio.create_task(monitoring_orchestrator.run_liquidity_watchdog(self.shutdown_event), name="Phase5_Watchdog"),
                # Additional tasks (e.g. WebSocket, Radar, metrics) would go here
            ]
            
            # Mark system as Armed
            set_high_priority()
            logger.success("🚀 GEKTOR v21.3 APEX: System Armed & Tracking Institutional Anomalies.")

            # Full GC collection and object freezing for HFT burst performance
            gc.collect()
            if hasattr(gc, "freeze"):
                gc.freeze()

            # Wait until Shutdown requested (SIGINT or internal failure)
            await self.shutdown_event.wait()
            
            # 7. Shutdown execution
            logger.warning("📉 [Lifecycle] Shutdown triggered. Finalizing active tasks...")
            for t in self.tasks:
                if not t.done():
                    t.cancel()
            
            # Institutional wait-for-tasks with timeout
            await asyncio.gather(*self.tasks, return_exceptions=True)
            logger.info("🔱 GEKTOR APEX: All tasks aborted. Resource stack unwind successful.")

if __name__ == "__main__":
    # Institutional logging config
    logger.add("logs/gektor_system.log", rotation="100 MB", retention="7 days", compression="zip")
    
    app = GektorApplication()
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass # Handled by SIGINT in run() but caught here as fallback
    except Exception as e:
        logger.opt(exception=True).critical(f"💀 [BOOT CRASH] SYSTEM FAILURE: {e}")
        sys.exit(1)
