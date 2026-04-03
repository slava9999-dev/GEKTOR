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

# [Audit 25.13] CPU Core Affinity (Task 18.1): Pin to Cores 2, 3
# Avoids Core 0/1 (Interrupts/OS overhead) and minimizes Context-Switch Jitter.
def set_cpu_affinity():
    try:
        proc = psutil.Process(os.getpid())
        count = psutil.cpu_count()
        # Pin to cores 2 and 3 if available (Physical Core Matrix)
        # Core 0/1 are usually heavily used by OS and Docker/WSL interrupts.
        target = [c for c in [2, 3] if c < count]
        if target:
            proc.cpu_affinity(target)
            logger.info(f"🎯 [CoreMatrix] CPU_AFFINITY: Pinned to physical cores {target}.")
    except Exception as e:
        logger.warning(f"⚠️ [Runtime] Failed to set CPU affinity: {e}")

# [Audit 25.12] Windows-native HFT Tuning (IOCP Matrix)
if sys.platform == 'win32':
    # 1. IO Completion Ports (IOCP) - Removes the 64-socket limit of Selector
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # 2. Kernel Scheduling: Process Priority (Minimize Jitter)
    try:
        proc = psutil.Process(os.getpid())
        # HIGH_PRIORITY_CLASS ensures OS prioritizes HFT threads over background tasks
        proc.nice(psutil.HIGH_PRIORITY_CLASS)
        logger.info("⚡ [Matrix] Process Priority: Set to HIGH.")
        set_cpu_affinity()
    except Exception as e:
        logger.warning(f"⚠️ [Runtime] Failed to elevate priority: {e}")

# [NERVE REPAIR v4.4] Force configuration loading before any module imports.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
env_path = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=env_path, override=True)

# Imports
from data.bybit_rest import BybitREST
from data.swarm.manager import SwarmManager as BybitWSManager
from data.database import DatabaseManager
from utils.config import config
from core.radar.scanner import RadarV2Scanner
from core.radar.watchlist import select_watchlist
from core.radar.priority_watchlist import priority_watchlist
from core.candle_cache import CandleCache
from core.runtime.time_sync import time_sync, TimeSynchronizer
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
from core.scenario.virtual_adapter import VirtualExchangeAdapter
from core.position_tracker import PaperTracker
from data.bybit_instruments import InstrumentsProvider
from data.swarm.sentiment_worker import NeuralSentinel as AnalysisWorker
from core.alerts.bot_listener import start_bot_listener
from core.alerts.outbox import telegram_outbox
from core.radar.macro_radar import MacroRadar
from core.shield.amputation import get_amputation_protocol
from pre_flight import PreflightCommander, PeriodicKeyValidator


# ═══════════════════════════════════════════════════════════════════════════
# [GEKTOR v8.3] VITALS WATCHDOG — Zero-Latency Event-Driven Zombie Detection
# ═══════════════════════════════════════════════════════════════════════════
# Uses asyncio.wait(FIRST_COMPLETED) instead of polling with sleep().
# When a daemon dies, reaction time is 0.0ms (Event Loop callback),
# not 10 seconds (polling interval).
# ═══════════════════════════════════════════════════════════════════════════

class VitalsWatchdog:
    """
    [GEKTOR v8.3] Zero-Latency Watchdog.
    
    Architecture: asyncio.wait(FIRST_COMPLETED)
    - No polling loop, no sleep(10) waste
    - Event Loop wakes us INSTANTLY when any task completes
    - stop_event sentinel enables clean shutdown
    - Automatic restart with exponential cooldown
    """
    
    def __init__(self, stop_event: asyncio.Event):
        self.stop_event = stop_event
        self._watched_tasks: dict[str, asyncio.Task] = {}
        self._restart_count: dict[str, int] = {}
        self._restart_factories: dict[str, callable] = {}  # name → coroutine factory
        self._max_restarts = 3
    
    def register(self, name: str, task: asyncio.Task, restart_factory=None):
        """
        Register a task for zero-latency monitoring.
        
        Args:
            name: Human-readable daemon name
            task: The asyncio.Task to monitor
            restart_factory: Optional async callable that returns a new task.
                             If None, dead daemon requires manual restart.
        """
        self._watched_tasks[name] = task
        self._restart_count.setdefault(name, 0)
        if restart_factory:
            self._restart_factories[name] = restart_factory
    
    async def run(self):
        """
        Zero-latency watchdog loop.
        Uses asyncio.wait(FIRST_COMPLETED) — wakes ONLY when a task dies.
        """
        logger.info(f"🐕 [Watchdog] Zero-latency monitoring: {list(self._watched_tasks.keys())}")
        
        # Sentinel task for clean shutdown
        stop_sentinel = asyncio.create_task(
            self.stop_event.wait(), name="__stop_sentinel__"
        )
        
        while not self.stop_event.is_set():
            # Build the wait set: all monitored tasks + stop sentinel
            wait_set = set(self._watched_tasks.values()) | {stop_sentinel}
            
            if len(wait_set) <= 1:  # Only sentinel left
                logger.warning("🐕 [Watchdog] No daemons to monitor. Sleeping until stop.")
                await self.stop_event.wait()
                break
            
            # ─── ZERO-LATENCY WAIT ───────────────────────────────────────
            # Event Loop wakes us the INSTANT any task completes.
            # No polling. No wasted CPU cycles. No 10-second blind window.
            done, _pending = await asyncio.wait(
                wait_set,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for dead_task in done:
                task_name = dead_task.get_name()
                
                # Clean shutdown signal
                if dead_task is stop_sentinel:
                    logger.info("🛑 [Watchdog] Stop signal received. Shutting down.")
                    return
                
                # Extract exception from dead daemon
                exc = None
                try:
                    if not dead_task.cancelled():
                        exc = dead_task.exception()
                except (asyncio.InvalidStateError, asyncio.CancelledError):
                    pass
                
                if exc:
                    logger.critical(
                        f"💀 [WATCHDOG] ZOMBIE: '{task_name}' died with: {exc}"
                    )
                elif dead_task.cancelled():
                    logger.warning(f"⚠️ [WATCHDOG] '{task_name}' was cancelled.")
                else:
                    logger.critical(
                        f"💀 [WATCHDOG] ZOMBIE: '{task_name}' exited silently (returned)."
                    )
                
                # Emergency TG alert
                try:
                    telegram_outbox.enqueue(
                        text=(
                            f"💀 <b>DAEMON DEATH: {task_name}</b>\n"
                            f"Ошибка: <code>{str(exc)[:200] if exc else 'Silent exit'}</code>\n"
                            f"Рестарт #{self._restart_count.get(task_name, 0)+1}/{self._max_restarts}..."
                        ),
                        priority=3,
                        disable_notification=False,
                    )
                except Exception:
                    pass
                
                # Attempt restart
                restarts = self._restart_count.get(task_name, 0)
                if restarts < self._max_restarts and task_name in self._restart_factories:
                    self._restart_count[task_name] = restarts + 1
                    
                    # Exponential cooldown: 1s, 2s, 4s before restart
                    cooldown = min(2 ** restarts, 10)
                    logger.warning(
                        f"🔄 [WATCHDOG] Restarting '{task_name}' in {cooldown}s "
                        f"(attempt {restarts+1}/{self._max_restarts})..."
                    )
                    await asyncio.sleep(cooldown)
                    
                    try:
                        factory = self._restart_factories[task_name]
                        new_task = await factory()
                        self._watched_tasks[task_name] = new_task
                        logger.success(f"✅ [WATCHDOG] '{task_name}' restarted successfully.")
                    except Exception as e:
                        logger.error(f"❌ [WATCHDOG] Restart of '{task_name}' failed: {e}")
                        del self._watched_tasks[task_name]
                else:
                    if restarts >= self._max_restarts:
                        logger.critical(
                            f"🛑 [WATCHDOG] '{task_name}' exceeded {self._max_restarts} restarts. "
                            f"Manual intervention required."
                        )
                    # Remove dead task from watch set
                    self._watched_tasks.pop(task_name, None)
async def sync_exchange_state(rest: BybitREST, tracker: PaperTracker):
    try:
        positions = await rest.get_positions(category="linear")
        if positions is None: raise Exception("Exchange unreachable.")
        for pos in positions:
            if float(pos.get('size', 0)) > 0:
                tracker.on_external_position_found(pos)
        balance = await rest.get_account_wallet_balance()
        if balance:
            # kill_switch was refactored and no longer tracks equity natively
            pass
    except Exception as e:
        logger.critical(f"🚨 [Hydration] FAILED: {e}")
        sys.exit(1)

class GracefulKiller:
    def __init__(self, stop_event: asyncio.Event):
        self.stop_event = stop_event
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try: loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError: pass

async def shutdown_system(tasks, db, rest, ws, timeout: float = 5.0):
    async def perform_cleanup():
        from core.events.outbox_relay import outbox_relay
        await outbox_relay.stop()
        
        from core.scenario.tactical_orchestrator import tactical_orchestrator
        if tactical_orchestrator.execution_engine:
            await tactical_orchestrator.execution_engine.stop()
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await TaskRegistry.shutdown(5.0)
        await ws.disconnect()
        await rest.close()
        await db.close()
        await telegram_outbox.stop()

    try:
        await asyncio.wait_for(perform_cleanup(), timeout=timeout)
    except Exception as e:
        logger.error(f"⚠️ [Shutdown] Cleanup failed: {e}")
    finally:
        os._exit(0)

class ResourceMonitor:
    """
    [Audit 25.13] Advanced Resource Guard (Task 18.2).
    Implements Lame Duck Mode (ONLY_CLOSE) and Last Will for Sentinel.
    """
    def __init__(self, stop_event: asyncio.Event, ws_manager, candle_mgr):
        self.stop_event = stop_event
        self.ws_manager = ws_manager
        self.candle_mgr = candle_mgr

    async def enter_lame_duck_mode(self):
        """[Audit 25.14] Sensing-Decline: Stop the data flood to recover RAM."""
        try:
            logger.critical("🦆 [LAME_DUCK] Entering Lame Duck Mode. Shutting down data plane...")
            # 1. Stop heavy Radar/Public streams to stop memory inflation
            if self.ws_manager:
                await self.ws_manager.disconnect() # Kills public tickers/klines
            
            # 2. Flush Caches (Non-critical for closing positions)
            if self.candle_mgr:
                self.candle_mgr.data.clear()
            gc.collect()
            
            # 3. Transition Tactical state
            tactical_orchestrator.execution_mode = "ONLY_CLOSE"
            logger.warning("🛡️ [LAME_DUCK] Data plane severed. ONLY_CLOSE mode active. Focusing on bailouts.")
        except Exception as e:
            logger.error(f"❌ Failed to enter Lame Duck correctly: {e}")

    async def run(self):
        from core.events.nerve_center import bus
        while not self.stop_event.is_set():
            try:
                mem_pct = psutil.virtual_memory().percent
                rss_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                
                # THRESHOLD 1: Lame Duck (ONLY_CLOSE + Stream Shutdown) at 98% RAM
                if mem_pct > 98:
                    if tactical_orchestrator.execution_mode != "ONLY_CLOSE":
                        await self.enter_lame_duck_mode()
                
                # [RE-ARM] Return to AUTO if RAM recovers to 90%
                elif mem_pct < 90:
                    if tactical_orchestrator.execution_mode == "ONLY_CLOSE":
                        logger.success(f"🚀 [HYSTERESIS] RAM recovered to {mem_pct}%. Re-initializing data plane...")
                        tactical_orchestrator.execution_mode = "AUTO"
                        
                        if self.ws_manager:
                            logger.info("📡 [HYSTERESIS] Restarting public WebSocket streams...")
                            asyncio.create_task(self.ws_manager.start()) 
                        
                        gc.collect()
                
                # THRESHOLD 2: Nuclear Nuke at 99.5% RAM
                if mem_pct > 99.5:
                    logger.error("💀 [NUCLEAR_NUKE] RAM > 99.5%. Resource depletion total. Executing Emergency Last Will.")
                    
                    # 1. Write Last Will to Redis (for External Sentinel)
                    # Sentinel will see this and take over if main dies without closing pos.
                    try:
                        will_data = {
                            "ts": time.time(),
                            "reason": "MEM_EXHAUSTION",
                            "pid": os.getpid(),
                            "mode": "EMERGENCY_LIQUIDATION_REQUESTED"
                        }
                        await bus.redis.setex("TEKTON:LAST_WILL", 3600, json.dumps(will_data))
                        logger.warning("📝 [LastWill] Redis entry created for Sentinel escalation.")
                    except Exception as re:
                        logger.error(f"❌ Failed to write Last Will: {re}")

                    # 2. Attempt final liquidation
                    if tactical_orchestrator.execution_engine:
                        try:
                            # Use short timeout, we're dying
                            await asyncio.wait_for(tactical_orchestrator.execution_engine.panic_liquidate_all(), timeout=3.0)
                        except:
                            logger.error("❌ Final Panic Liquidation failed (possible 429/Timeout).")

                    os._exit(1)

                # Standard Telemetry
                logger.debug(f"🏥 [Health] Symbols: {len(market_state.symbols)} | RAM: {mem_pct}% | Mode: {tactical_orchestrator.execution_mode}")
                
            except Exception as e:
                logger.error(f"⚠️ [ResMonitor] error: {e}")
            
            await asyncio.sleep(5)

async def main():
    from core.utils.pid_lock import PIDLock
    with PIDLock("sniper_main"):
        setup_loop_exception_handler()
        stop_event = asyncio.Event()
        killer = GracefulKiller(stop_event)

        db_manager = DatabaseManager()
        await db_manager.initialize()
        rest_client = BybitREST(base_url=config.bybit.rest_url)
        ws_manager = BybitWSManager(stream_url=config.bybit.ws_url)
        market_state.set_clients(rest_client, ws_manager)
        
        paper_tracker = PaperTracker(db_manager)
        candle_mgr = CandleCache(rest_client, db=db_manager)
        # [Audit 25.18] Focused sync: BTC, ETH, SOL, DOGE, XRP (Core Matrix)
        trading_pairs = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"]
        instruments_mgr = InstrumentsProvider(rest_client, whitelist=trading_pairs)
        await instruments_mgr.start_background_sync(interval_seconds=3600)
        
        # ══════════════════════════════════════════════════════════════════
        # [GEKTOR v8.4] PRE-FLIGHT CHECK — Fail-Fast Infrastructure Gate
        # ══════════════════════════════════════════════════════════════════
        preflight = PreflightCommander(
            rest_client=rest_client,
            mode="ADVISORY" if not os.getenv("BYBIT_API_KEY") else config.trading_mode.upper()
        )
        preflight_report = await preflight.execute()
        # If we reach here, all critical checks passed (or sys.exit(1) was called)
        
        # [Audit 25.19] Memory Stabilization: Freeze long-lived objects to optimize GC
        gc.collect() 
        gc.freeze() 
        logger.info("❄️ [Runtime] Multi-generational GC Freeze Engaged. Memory Stable.")

        logger.remove()
        logger.add(sys.stdout, colorize=True, enqueue=True)
        logger.add("logs/sniper.log", rotation="100 MB", enqueue=True)
        
        await candle_mgr.initialize()

        from core.runtime import time_sync as ts_module
        ts_module.time_sync = TimeSynchronizer(rest_client)
        await ts_module.time_sync.start()
        # ══════════════════════════════════════════════════════════════════
        # [GEKTOR v8.4] SYSTEM MODE — Graceful Degradation Router
        # ══════════════════════════════════════════════════════════════════
        system_mode = os.getenv("SYSTEM_MODE", "RADAR_ONLY").upper()
        has_api_keys = bool(os.getenv("BYBIT_API_KEY")) and bool(os.getenv("BYBIT_API_SECRET"))
        
        # Auto-detect: if no keys, force RADAR_ONLY regardless of env var
        if not has_api_keys and system_mode == "LIVE":
            logger.warning(
                "⚠️ [MODE] SYSTEM_MODE=LIVE but BYBIT_API_KEY is empty. "
                "Forcing RADAR_ONLY mode."
            )
            system_mode = "RADAR_ONLY"
        
        # Export for bot_listener to check
        os.environ["_TEKTON_SYSTEM_MODE"] = system_mode
        logger.info(f"🛡️ [MODE] System Mode: {system_mode}")
        
        if system_mode == "LIVE":
            if config.trading_mode.upper() == "LIVE":
                await sync_exchange_state(rest_client, paper_tracker)
        
        from core.events.nerve_center import bus as bus_infra
        from core.shield.risk_engine import init_risk_guard
        from core.shield.execution_guard import ExecutionGuard
        from core.shield.kill_switch import KillSwitch
        from core.scenario.oms_adapter import BybitLiveAdapter

        if system_mode == "LIVE":
            kill_switch = KillSwitch(redis=bus_infra.redis, db_pool=None, publisher=bus_infra)
            risk_armor = init_risk_guard(config.model_dump())
            risk_armor.start()
            execution_gatekeeper = ExecutionGuard(kill_switch)

            # [Kleppmann's Pattern] Transactional Outbox Relay
            from core.events.outbox_relay import outbox_relay
            from core.realtime.bridge import bridge
            outbox_relay.configure(db_manager, bus_infra)
            await outbox_relay.start()
            bridge.start()

            if config.trading_mode.upper() == "LIVE":
                adapter = BybitLiveAdapter(
                    api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"),
                    rest_client=rest_client, instruments_provider=instruments_mgr,
                    db_manager=db_manager
                )
                await adapter.enable_dcp(window_sec=15)
                private_ws = BybitPrivateWSManager(
                    api_key=os.getenv("BYBIT_API_KEY"), api_secret=os.getenv("BYBIT_API_SECRET"),
                    ws_url=config.bybit.ws_url_private
                )
            else:
                adapter = VirtualExchangeAdapter(
                    db_manager=db_manager, candle_cache=candle_mgr, 
                    paper_tracker=paper_tracker, instruments_provider=instruments_mgr,
                    is_ghost=(config.trading_mode.upper() == "GHOST")
                )

            paper_tracker.start()
            level_service = LevelService(rest_client)
            await tactical_orchestrator.start(
                db_manager, paper_tracker, candle_mgr, 
                adapter=adapter, level_service=level_service,
                time_sync=ts_module.time_sync
            )

            from core.ai_analyst import analyst
            if tactical_orchestrator.execution_engine:
                analyst.configure(tactical_orchestrator.execution_engine.sor)
            
            logger.critical("🔥 [MODE] LIVE: Execution Engine + Exoskeleton ACTIVE.")
        else:
            # ══════════════════════════════════════════════════════════════
            # RADAR_ONLY: No adapters, no OMS, no execution, no private WS
            # Only MacroRadar + TG Bot + Redis + PostgreSQL
            # ══════════════════════════════════════════════════════════════
            from core.events.outbox_relay import outbox_relay
            from core.realtime.bridge import bridge
            outbox_relay.configure(db_manager, bus_infra)
            await outbox_relay.start()
            bridge.start()
            
            logger.info(
                "🛡️ [MODE] RADAR_ONLY: MacroRadar + TG Bot active. "
                "Execution Engine DISABLED. Кнопки входа заблокированы."
            )

        # [Task 18.2] Resource Monitor with Lame Duck Strategy
        res_monitor = ResourceMonitor(stop_event, ws_manager, candle_mgr)
        
        # [Task 25.15] Cloud Heartbeat (Dead Man's Switch — Client Side)
        from core.health.heartbeat_emitter import heartbeat_emitter
        
        # ══════════════════════════════════════════════════════════════════
        # [GEKTOR v8.2] MacroRadar — Signal Purity Engine
        # ══════════════════════════════════════════════════════════════════
        macro_cfg = {}
        try:
            import yaml
            cfg_path = Path(__file__).parent / "config_sniper.yaml"
            with open(cfg_path, 'r', encoding='utf-8') as f:
                full_cfg = yaml.safe_load(f)
            macro_cfg = full_cfg.get("macro_radar", {})
        except Exception as e:
            logger.warning(f"⚠️ [MacroRadar] Config load failed: {e}. Using defaults.")
        
        macro_radar = None
        macro_radar_task = None
        if macro_cfg.get("enabled", True):
            macro_radar = MacroRadar(
                rest=rest_client,
                config=macro_cfg,
                scan_interval_sec=macro_cfg.get("scan_interval_sec", 60),
            )
            macro_radar_task = safe_task(macro_radar.start(), name="macro_radar")
            logger.info("📡 [MacroRadar] Registered as background daemon.")
        
        tasks = [
            safe_task(res_monitor.run(), name="res_monitor"),
            safe_task(heartbeat_emitter.run(stop_event), name="heartbeat_cloud"),
            safe_task(metrics.start(), name="metrics"),
            safe_task(recorder.start(), name="recorder"),
            safe_task(AnalysisWorker().run(), name="ai"),
            safe_task(detector_heartbeat_loop(market_state, antimiss_system, tactical_orchestrator), name="heartbeat"),
        ]
        
        # ══════════════════════════════════════════════════════════════════
        # [GEKTOR v8.3] Vitals Watchdog — Zero-Latency Zombie Detection
        # ══════════════════════════════════════════════════════════════════
        watchdog = VitalsWatchdog(stop_event)
        
        if macro_radar_task and macro_radar:
            # Restart factory: creates a new safe_task from existing MacroRadar instance
            _radar = macro_radar  # closure capture
            async def _restart_radar():
                return safe_task(_radar.start(), name="macro_radar")
            watchdog.register("macro_radar", macro_radar_task, restart_factory=_restart_radar)
        
        # Register other critical tasks (no restart factory = manual restart only)
        for t in tasks:
            if t.get_name() in ("res_monitor", "heartbeat_cloud", "heartbeat"):
                watchdog.register(t.get_name(), t)
        
        watchdog_task = safe_task(watchdog.run(), name="vitals_watchdog")
        tasks.append(watchdog_task)
        if macro_radar_task:
            tasks.append(macro_radar_task)
        
        # [GEKTOR v8.3] Initialize Amputation Protocol (singleton)
        get_amputation_protocol(rest_client=rest_client, kill_switch=None, bus=bus)
        logger.info("☢️ [AmputationProtocol] /panic_sell command ARMED.")
        
        await telegram_outbox.start()
        await bus_infra.start_listening()
        bridge.start()
        
        # [GEKTOR v8.4] Send Pre-flight Boot Report to TG
        telegram_outbox.enqueue(
            text=preflight.format_boot_report_telegram(),
            priority=3,
            disable_notification=False
        )
        
        # [GEKTOR v8.4] Periodic API Key Validator (24h cycle)
        key_validator = PeriodicKeyValidator(rest_client)
        key_validator_task = safe_task(key_validator.run(stop_event), name="key_validator")
        tasks.append(key_validator_task)
        
        # Start TG Bot Listener (passes macro_radar for signal cache)
        tg_listener_task = safe_task(
            start_bot_listener(
                paper_tracker=paper_tracker,
                db=db_manager,
                stop_event=stop_event,
                macro_radar=macro_radar
            ),
            name="tg_listener"
        )
        tasks.append(tg_listener_task)
        
        await stop_event.wait()
        await shutdown_system(tasks, db_manager, rest_client, ws_manager)

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except KeyboardInterrupt: pass
