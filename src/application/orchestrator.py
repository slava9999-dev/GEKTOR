# src/application/orchestrator.py
import asyncio
import math
import time
import os
import json
from typing import Dict, List, Optional, Any
from loguru import logger

from src.infrastructure.config import settings
from src.infrastructure.database import DatabaseManager
from src.infrastructure.telegram_notifier import TelegramRadarNotifier
from src.infrastructure.bybit import BybitIngestor
from src.application.pool_manager import WorkerPoolManager
from src.domain.math_core import process_ticks_subroutine
from src.domain.exit_protocol import MarketTick as ExitTick
from src.application.exit_protocol_service import SignalTracker
from src.application.sentinel_watchdog import event_loop_monitor
from src.infrastructure.conflation import DirectionalConflationBuffer
from src.application.vanguard import VanguardScanner
from src.application.microstructure import MicrostructureDefender, L2Snapshot, L2Level
from src.infrastructure.bybit import BybitRestClient
from src.domain.macro_regime import MacroRegimeFilter
from src.domain.friction_guard import ExecutionFrictionGuard
from src.application.sentinel import BlackoutSentinel, FlatlineSentinel
from src.infrastructure.event_bus import EventBus
from src.domain.entities.events import ExecutionEvent, ConflatedEvent, StateInvalidationEvent, EuthanasiaEvent
from src.application.quarantine import QuarantineManager
from src.application.escrow import LiquidationEchoGuard
class TickBatcher:
    """[GEKTOR v2.0] IPC Optimization with Memory Cycle Management."""
    def __init__(self, symbol: str, pool_manager: WorkerPoolManager, on_results_callback):
        self.symbol = symbol
        self.pool_manager = pool_manager
        self.on_results = on_results_callback
        
        # [SLOW CONSUMER SHIELD]
        self.buffer = DirectionalConflationBuffer(critical_size=2000)
        
        self.max_batch_size = 500
        self.last_flush = time.time()
        self._lock = asyncio.Lock()
        self._flush_task = asyncio.create_task(self._timer_flush())

    async def add_tick(self, tick_dict: dict):
        """МГНОВЕННЫЙ ИСТОЧНИК ПИТАНИЯ. Не блокирует луп."""
        self.buffer.ingest_immediate(tick_dict)
        # Если накопилось много, пытаемся сбросить (без ожидания семафора здесь)
        if len(self.buffer._buffer) >= self.max_batch_size:
            # Note: We don't use the event bus for TickBatcher because it has its own 
            # DirectionalConflationBuffer for raw ticks. But we use a protected task.
            task = asyncio.create_task(self._safe_flush())
            # For TickBatcher, we handle task protection locally 
            # or could use a global task repository. 
            # Given current architecture, we'll keep it as create_task but 
            # ideally this should also be managed.

    async def _safe_flush(self):
        """Попытка сброса в пул. Если семафор закрыт — просто не сбрасываем сейчас."""
        # Мы используем Lock только для предотвращения параллельных флошей
        if self._lock.locked():
            return
        async with self._lock:
            await self._flush_buffer()

    async def flush_pending_ticks(self):
        async with self._lock:
            await self._flush_buffer()

    async def _timer_flush(self):
        while True:
            await asyncio.sleep(0.05)
            if time.time() - self.last_flush >= 0.1:
                async with self._lock:
                    await self._flush_buffer()

    async def _flush_buffer(self):
        # 0. Извлекаем данные (со слиянием если нужно)
        batch = self.buffer.flush()
        if not batch:
            return
        self.last_flush = time.time()
        
        # 1. Получаем текущее состояние из оркестратора
        current_state = self.on_results("__GET_STATE__", self.symbol)
        
        # [INTRADAY v4.1] Адаптивный объем ведра в зависимости от символа
        bucket_vol = settings.VOLUME_BUCKETS.get(self.symbol, settings.VOLUME_BUCKETS["DEFAULT"])
        
        try:
            # [TRIAGE] 0 For CORE, 2 for Noise
            priority = 0 if self.symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"] else 2
            
            # 2. Передаем состояние в пул
            result_pkg = await self.pool_manager.execute_batch(
                process_ticks_subroutine, 
                self.symbol, batch, bucket_vol, current_state, priority=priority
            )
            
            if result_pkg:
                # 3. Сохраняем обновленное состояние и обрабатываем результаты
                self.on_results(result_pkg["results"], result_pkg["new_state"])
        except RuntimeError as rt:
            logger.error(f"☠️ [TickBatcher] Euthanized {self.symbol}: {rt}")
            # If euthanized due to starvation, we should ideally trigger State Corruption, 
            # but usually it's already corrupted or will be dropped.
        except Exception as e:
            logger.error(f"💥 [Orchestrator] Worker Process Crash for {self.symbol}: {e}")

class GektorOrchestrator:
    """[GEKTOR APEX] CHIEF ARCHITECT Monolith v2.0."""
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        # [ФАЗА 0] Независимые ресурсы (Layer 0)
        from src.infrastructure.database import DatabaseManager
        self.db = DatabaseManager()
        
        # [ФАЗА 1] Репозитории и Шины (Layer 1)
        self.event_bus = EventBus()
        self.quarantine = QuarantineManager(event_bus=self.event_bus)
        self.escrow_guard = LiquidationEchoGuard(escrow_window_ms=150)
        
        # [ФАЗА 2] Внешние адаптеры (Layer 2)
        total_cores = os.cpu_count() or 4
        core_workers = min(2, total_cores)
        noise_workers = max(2, total_cores - core_workers)
        self.pool_manager = WorkerPoolManager(core_workers=core_workers, noise_workers=noise_workers)

        
        # ИСТИННЫЙ ИСТОЧНИК ПАМЯТИ (State Ownership)
        self.macro_states: Dict[str, dict] = {}
        # [SUPERVISION TREE] Control over Daemons
        self._daemon_tasks: Set[asyncio.Task] = set()
        
        self.tg = TelegramRadarNotifier(
            db_manager=self.db,
            bot_token=settings.bot_token,
            chat_id=settings.chat_id,
            event_bus=self.event_bus,
            proxy_url=os.getenv("PROXY_URL")
        )
        self.sentinel = BlackoutSentinel(self.db, self.tg._live_allowed)
        self.flatline_sentinel = FlatlineSentinel(threshold_sec=65)
        
        # Subscriptions
        self.event_bus.subscribe("ExecutionEvent", self.tg.handle_execution_event)
        self.event_bus.subscribe("ConflatedEvent", self.tg.handle_conflated_event)
        self.event_bus.subscribe("StateInvalidationEvent", self.handle_state_invalidation)
        self.event_bus.subscribe("EuthanasiaEvent", self.handle_euthanasia)
        
        self.batchers: Dict[str, TickBatcher] = {}
        self.micro_defenders: Dict[str, MicrostructureDefender] = {}
        self.latest_bbo: Dict[str, Tuple[float, float]] = {}
        
        # [SIGNAL COOLDOWN v4.1] Защита от спама на запилах (3600 сек / 60 мин)
        self._last_alert_time: Dict[str, float] = {}
        
        self.rest_client = BybitRestClient(proxy_url=os.getenv("PROXY_URL"))
        
        self.ingestor = BybitIngestor(
            symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            on_tick_callback=self.ingest_tick,
            on_snapshot_callback=self.ingest_snapshot,
            alert_callback=self.send_critical_alert
        )
        
        self.vanguard = VanguardScanner(self.rest_client, self)
        # --- PHASE 2 & 5: MACRO REGIME & EXIT PROTOCOL ---
        self.signal_tracker = SignalTracker(self.db, self.tg)
        self.macro_filter = MacroRegimeFilter()
        self.friction_guard = ExecutionFrictionGuard(taker_fee_bps=6.0, min_alpha_bps=5.0)
        self._macro_alert_sent = False

    def _launch_daemon(self, name: str, coro) -> None:
        """[SUPERVISION] Изолированный запуск бесконечных циклов."""
        task = asyncio.create_task(coro, name=name)
        self._daemon_tasks.add(task)
        task.add_done_callback(self._handle_daemon_death)
        logger.info(f"⚙️ [SUPERVISOR] Daemon '{name}' launched.")

    def _handle_daemon_death(self, task: asyncio.Task):
        """[FAIL-FAST] Erlang 'Let it Crash' Pattern via OS SIGTERM."""
        self._daemon_tasks.discard(task)
        try:
            exc = task.exception()
            if exc:
                death_reason = f"Daemon '{task.get_name()}' crashed: {type(exc).__name__} - {str(exc)}"
                logger.critical(f"☠️ [FATAL] {death_reason}")
                
                # 1. Инъекция предсмертной записки в Черный Ящик ДО выстрела
                if hasattr(self, 'dead_mans_switch'):
                    self.dead_mans_switch.set_fatal_context(reason=death_reason)
                
                # 2. Хардкорный C-level interrupt
                import os, signal
                logger.critical("☠️ [FAIL-FAST] Sending OS SIGTERM to self to guarantee atomic loop destruction.")
                os.kill(os.getpid(), signal.SIGTERM)
        except asyncio.CancelledError:
            pass # Штатная остановка

    async def _flatline_monitor_loop(self):
        """Background loop to check for exchange freezes."""
        while not self._shutdown_event.is_set():
            await asyncio.sleep(10)
            active_symbols = self.vanguard.active_universe
            blind_symbols = self.flatline_sentinel.check_for_flatlines(active_symbols)
            if blind_symbols:
                for symbol in blind_symbols:
                    asyncio.create_task(self.tg.notify_manual(f"🛑 <b>[ЧАСТИЧНАЯ СЛЕПОТА]</b> {symbol} flatlined. Данные застыли."))

    def send_critical_alert(self, message: str):
        """Callback for ingestor to report critical network/API failures."""
        logger.error(f"🚨 [INGESTOR] {message}")
        asyncio.create_task(self.tg.notify_manual(f"🚨 <b>[СБОЙ ИНФРАСТРУКТУРЫ]</b>\n{message}"))

    async def _persist_symbol_state(self, symbol: str, state: dict):
        """Сохранение стейта в Redis для выживания при рестарте."""
        try:
            key = f"gektor:state:{symbol}"
            await self.db.buffer.redis.set(key, json.dumps(state, default=str))
        except Exception as e:
            logger.error(f"❌ [Orchestrator] Persistence failure ({symbol}): {e}")

    async def _hydrate_states(self, symbols: List[str]):
        """Загрузка стейта из Redis при старте."""
        hydrated = 0
        for symbol in symbols:
            try:
                key = f"gektor:state:{symbol}"
                data = await self.db.buffer.redis.get(key)
                if data:
                    self.macro_states[symbol] = json.loads(data)
                    hydrated += 1
            except Exception as e:
                logger.error(f"⚠️ [Orchestrator] Hydration failure ({symbol}): {e}")
        if hydrated > 0:
            logger.success(f"📟 [Orchestrator] Hydrated {hydrated} symbols from Redis.")

    async def start(self):
        logger.info("🔥 [CORE] Gektor APEX v2.1 Monolith starting...")
        # 0. Start Event Bus Consumer
        await self.event_bus.start()
        
        await self.db.initialize()
        
        # [EXORCISM] Жесткая зачистка старых алертов при старте
        try:
            async with self.db.engine.begin() as conn:
                from sqlalchemy import text
                result = await conn.execute(text("DELETE FROM outbox WHERE status = 'PENDING';"))
                if result.rowcount > 0:
                    logger.warning(f"🧹 [EXORCISM] Banished {result.rowcount} ghost alerts from Outbox.")
        except Exception as e:
            logger.error(f"❌ [EXORCISM] Failed to clear outbox: {e}")

        await self.tg.start()
        
        # [PERSISTENCE] Живительная инъекция стейта
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        await self._hydrate_states(symbols)
        await self.signal_tracker.hydrate_signals()
        
        # [SUPERVISOR] Дерево процессов
        self._launch_daemon("MemoryWatchdog", self.pool_manager.memory_watchdog_loop())
        self._launch_daemon("VanguardScanner", self.vanguard.scan_market_cycle())
        self._launch_daemon("BlackoutSentinel", self.sentinel.watch())
        self._launch_daemon("WebSocketIngestor", self.ingestor.run())
        
        # [HEARTBEAT] Run flatline check
        self._launch_daemon("FlatlineMonitor", self._flatline_monitor_loop())
        
        await self.tg.notify_manual(
            "🚀 <b>[СИСТЕМА ЗАПУЩЕНА]</b>\n"
            "GEKTOR APEX v4.1: РАДАР ОНЛАЙН\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📡 <b>Мониторинг:</b> BTC, ETH, SOL\n"
            "🛡️ <b>Защита:</b> Активна (PSR 4.1)\n"
            "⚡ <b>Режим:</b> TACTICAL INTRADAY"
        )
        logger.success("🚀 [CORE] System ARMED. Daemons and Supervisor running.")

    async def handle_state_invalidation(self, event: StateInvalidationEvent):
        """[RESILIENCE] Emergency handler for causality breaks."""
        symbol = event.symbol
        logger.critical(f"☠️ [Orchestrator] STATE CORRUPTED for {symbol}. Reason: {event.reason}")
        
        # 1. Immediate Causality Lock
        self.quarantine.mark_corrupted(symbol)
        
        # [MICROSTRUCTURE AMNESIA] Erase micro-state completely to avoid trading on disjointed ticks
        if symbol in self.micro_defenders:
            self.micro_defenders[symbol].reset_state()
            
        if symbol in self.batchers:
            await self.batchers[symbol].flush_pending_ticks()
            
        # [ESCROW PURGE] Kill any pending signals waiting in quarantine
        self.escrow_guard.purge_stale_escrow(symbol)
            
        asyncio.create_task(self.tg.notify_manual(
            f"☠️ <b>[STATE CORRUPTED: {symbol}]</b>\n"
            f"Причинно-следственная связь разорвана.\n"
            f"Торговля заблокирована. Запуск протокола RE-HYDRATION."
        ))
        
        # 2. Trigger VoIP if available
        # if hasattr(self, 'voip'): await self.voip.trigger_alarm(...)

        # 3. Initiate Non-Blocking Pit-Stop
        self.event_bus.publish_fire_and_forget(self._recovery_pit_stop(symbol))

    async def _recovery_pit_stop(self, symbol: str):
        """[GEKTOR v2.2] Asynchronous Pit-Stop for State Reconstruction."""
        try:
            logger.warning(f"🏎️ [Pit-Stop] Reconstructing baseline for {symbol}...")
            
            # Phase 1: Background REST Fetch (Non-blocking I/O)
            recent_trades = await self.rest_client.get_recent_trades(symbol, limit=1000)
            
            # Phase 2: Math Offloading (CPU-intensive task to Worker Pool)
            # We use the existing start_monitoring logic but specifically for recovery
            success = await self.start_monitoring(symbol, seed_data=recent_trades)
            
            if success:
                # Phase 3: Catch-up Replay
                stashed_ticks = self.quarantine.extract_stash(symbol)
                if stashed_ticks:
                    logger.info(f"🔄 [Pit-Stop] Replaying {len(stashed_ticks)} stashed ticks for {symbol}.")
                    for tick_data in stashed_ticks:
                        self.event_bus.publish_fire_and_forget(self.batchers[symbol].add_tick(tick_data))
                
                # Phase 4: Unlock causality
                self.quarantine.mark_recovered(symbol)
                self.event_bus.mark_recovered(symbol)
                self.tg.notify_manual(f"🔄 <b>[STATE RECOVERED: {symbol}]</b>\nМатематика восстановлена. Радар снова в строю.")
                logger.success(f"🏁 [Pit-Stop] {symbol} is back online.")
            else:
                logger.error(f"❌ [Pit-Stop] Recovery failed for {symbol}.")
                
        except Exception as e:
            logger.opt(exception=True).error(f"🚨 [Pit-Stop] Fatal failure during recovery of {symbol}: {e}")

    async def handle_euthanasia(self, event: EuthanasiaEvent):
        """[EUTHANASIA PROTOCOL] Full cascaded teardown of a deceased asset."""
        logger.critical(f"☠️ [EUTHANASIA] Executing order 66 on {event.symbol}. Reason: {event.reason}")
        
        # 1. Помечаем монету как мертвую (Tombstone)
        self.quarantine.bury(event.symbol, event.cooldown_seconds)
        
        # 2. РУБИМ СЕТЕВОЙ ПОТОК (Критическое исправление)
        if event.symbol in self.ingestor.symbols:
            self.ingestor.symbols.remove(event.symbol)
            await self.ingestor.unsubscribe([event.symbol])
            logger.info(f"🔌 [EUTHANASIA] WebSocket Unsubscribed: {event.symbol}")
        
        # 3. Физическая очистка внутреннего стейта
        await self.stop_monitoring(event.symbol)
        
        self.tg.notify_manual(
            f"☠️ <b>[АСЕТ ЛИКВИДИРОВАН: {event.symbol}]</b>\n"
            f"Система эвтаназировала монету для спасения Ядра.\n"
            f"Причина: {event.reason}\n"
            f"Карантин: {event.cooldown_seconds}s"
        )

    def on_math_results(self, results: Any, new_state: Optional[dict] = None):
        """Callback для обработки результатов и управления стейтом."""
        # Специальный вызов для получения стейта батчером
        if results == "__GET_STATE__":
            return self.macro_states.get(new_state) # Здесь new_state используется как символ
            
        if new_state and isinstance(new_state, dict):
            symbol = new_state.get("symbol", "UNKNOWN")
            
            # Логирование прогресса Warmup (раз в 1 млн)
            if new_state.get("state") == "WARMUP":
                vol = new_state.get("warmup_vol", 0)
                step = 1_000_000.0
                if int(vol / step) > int((self.macro_states.get(symbol, {}).get("warmup_vol", 0)) / step):
                    logger.info(f"⏳ [Math] Warmup Progress ({symbol}): ${vol/1_000_000:.1f}M / $5.0M")
            
            self.macro_states[symbol] = new_state
            # Асинхронное сохранение в Redis (через буфер БД)
            self.event_bus.publish_fire_and_forget(self._persist_symbol_state(symbol, new_state))

        # 2. Рассылаем алерты
        for res in results:
            symbol = res.get("symbol", "BTCUSDT")
            vpin = res["vpin"]
            price = res["price"]
            ts = res["timestamp"]

            # Обновление Поводыря (Macro Baseline)
            if symbol == "BTCUSDT":
                self.macro_filter.update_baseline(symbol, vpin, price)
                if self.macro_filter.current_health.is_panic and not self._macro_alert_sent:
                    self.tg.notify_manual(
                        f"🚨 <b>[MACRO SHIFT: PANIC]</b>\n"
                        f"Рынок в состоянии паники: {self.macro_filter.current_health.reason}.\n"
                        f"Сигналы по альткоинам ПРИОСТАНОВЛЕНЫ."
                    )
                    self._macro_alert_sent = True
                elif not self.macro_filter.current_health.is_panic and self._macro_alert_sent:
                    self.tg.notify_manual("🟢 <b>[MACRO]</b> Рынок стабилизировался. Радар по альткоинам возобновлен.")
                    self._macro_alert_sent = False

            # Синхронизация VPIN/State в трекере для реактивных правил
            bid, ask = self.latest_bbo.get(symbol, (price, price))
            self.event_bus.publish_fire_and_forget(self.signal_tracker.update_math_state(symbol, vpin, price, ts, bid, ask))

            if res.get("is_anomaly"):
                # [COOLDOWN CHECK] Проверяем таймаут 60 минут
                last_time = self._last_alert_time.get(symbol, 0)
                if time.time() - last_time < 3600:
                    logger.info(f"🤐 [COOLDOWN] Signal suppressed for {symbol} (VPIN: {vpin:.2f})")
                    continue

                # Проверка Макро-барьера
                if self.macro_filter.should_mute(symbol):
                    logger.info(f"🤐 [Mute] Signal for {symbol} suppressed due to Macro Panic.")
                    continue

                # [FRICTION GUARD] Peter Brown's Razor: Alpha must exceed transaction costs
                # Retrieve dynamic_threshold from state for friction calculation
                sym_state = self.macro_states.get(symbol, {})
                vpin_k = sym_state.get("vpin_k", 0)
                vpin_m = sym_state.get("vpin_m", 0.0)
                vpin_s = sym_state.get("vpin_s", 0.0)
                vpin_var = vpin_s / (vpin_k - 1) if vpin_k > 1 else 0.0
                vpin_std = math.sqrt(max(0.0, vpin_var))
                dyn_thr = max(settings.VPIN_THRESHOLD, vpin_m + 3.0 * vpin_std) if vpin_k > 50 else settings.VPIN_THRESHOLD

                if not self.friction_guard.is_tradable(symbol, vpin, dyn_thr):
                    logger.info(f"🛡️ [FRICTION] Signal for {symbol} killed by transaction costs.")
                    continue

                # Авто-мониторинг новой аномалии с учетом L1 спреда
                self.event_bus.publish_fire_and_forget(self.signal_tracker.register_signal(
                    symbol=symbol,
                    price=price,
                    vpin=vpin,
                    direction=1,
                    timestamp=ts,
                    entry_bid=bid,
                    entry_ask=ask
                ))
                
                self._last_alert_time[symbol] = time.time()
                # Create and publish ExecutionEvent to the bus
                event = ExecutionEvent(
                    symbol=symbol,
                    price=price,
                    volume=0.0, # Will be filled by execution/conflation if needed
                    side="BUY", # Advisory assumption
                    metadata={"vpin": vpin, "anomaly": True}
                )
                self.event_bus.publish_fire_and_forget(event)
                
                self.tg.notify_event({
                    "symbol": symbol,
                    "price": price,
                    "vpin": vpin,
                    "timestamp": ts
                })
            
            if res.get("abort_mission"):
                # Аборты не мутятся — это сигналы защиты капитала
                self.tg.notify_event({
                    "symbol": symbol,
                    "price": price,
                    "vpin": vpin,
                    "timestamp": ts,
                    "abort_mission": True,
                    "abort_reason": res.get("abort_reason", "")
                })

    async def ingest_tick(self, symbol: str, tick_data: dict):
        # [GATEKEEPER] Drop Ghost Events for dead coins
        if self.quarantine.is_dead(symbol):
            return
            
        # 0. Causality Checks (Intercept into quarantine if repairing)
        if not self.quarantine.intercept_ws_tick(symbol, tick_data):
            return
            
        # 1. Update pulse and Microstructure Defender
        self.flatline_sentinel.update_pulse(symbol)
        
        if symbol in self.micro_defenders:
            self.micro_defenders[symbol].update_execution(tick_data["volume"], tick_data["side"])

        # 1. Сначала реактивная проверка мониторинга (Beazley)
        # Оптимизация: проверяем наличие активных сигналов ПРЕЖДЕ чем создавать объект тика
        if self.signal_tracker.active_signals:
            current_vpin = self.macro_states.get(symbol, {}).get("vpin_m", 0.0)
            tick = ExitTick(
                symbol=symbol,
                price=tick_data["price"],
                volume=tick_data["volume"],
                side=tick_data["side"],
                exchange_ts=tick_data["timestamp"]
            )
            self.event_bus.publish_fire_and_forget(self.signal_tracker.process_tick(tick, current_vpin))

        # 2. Затем батчинг для тяжелой математики
        if symbol not in self.batchers:
            self.batchers[symbol] = TickBatcher(symbol, self.pool_manager, self.on_math_results)
        await self.batchers[symbol].add_tick(tick_data)

    async def ingest_snapshot(self, symbol: str, snapshot_data: dict):
        """Обработка срезов стакана для детекции манипуляций."""
        # [GATEKEEPER] Drop Ghost Events for dead coins
        if self.quarantine.is_dead(symbol):
            return
            
        if symbol not in self.micro_defenders:
            self.micro_defenders[symbol] = MicrostructureDefender(symbol=symbol)
        
        snapshot = L2Snapshot(
            symbol=symbol,
            best_bid=L2Level(snapshot_data["bid_p"], snapshot_data["bid_v"]),
            best_ask=L2Level(snapshot_data["ask_p"], snapshot_data["ask_v"]),
            exchange_ts=int(snapshot_data["ts"])
        )
        self.latest_bbo[symbol] = (snapshot.best_bid.price, snapshot.best_ask.price)
        # [FRICTION GUARD] Feed L1 BBO to FrictionGuard for spread tracking
        self.friction_guard.update_quote(symbol, snapshot_data["bid_p"], snapshot_data["ask_p"])

        # [MICROSTRUCTURE TRIGGER] Получаем результат с учетом гистерезиса
        result = await self.micro_defenders[symbol].ingest_snapshot(snapshot)
        
        # [КРИТИЧЕСКИЙ БАРЬЕР] Посылаем сигнал в ТГ только в момент СТАРТА импульса (фронт)
        if result.get("is_new_impulse"):
            # Асинхронное сожжение: запускаем проверку эскроу
            asyncio.create_task(self._process_micro_impulse(symbol, result, snapshot))

    async def _process_micro_impulse(self, symbol: str, result: dict, snapshot: L2Snapshot):
        # [ESCROW ВАЛИДАЦИЯ] Спим 150мс и проверяем стрим ликвидаций
        ofi_usd = abs(result["ofi"] * snapshot.best_bid.price)
        is_clean = await self.escrow_guard.escort_signal(symbol, result["state"], ofi_usd)
        
        if not is_clean:
            return  # Сгорел в Liquidation Echo Guard

        # [COOLDOWN CHECK] Проверяем таймаут для микроструктурных импульсов
        last_time = self._last_alert_time.get(symbol, 0)
        if time.time() - last_time < 3600:
            logger.debug(f"🤐 [COOLDOWN] Micro-impulse suppressed for {symbol}")
            return

        self._last_alert_time[symbol] = time.time()
        
        # [SHADOW REGISTRATION] Оповещаем трекер о новом импульсе
        direction = 1 if result["state"] == "BUY_IMPULSE" else -1
        self.event_bus.publish_fire_and_forget(self.signal_tracker.register_signal(
            symbol=symbol,
            price=snapshot.best_bid.price,
            vpin=0.0,
            direction=direction,
            timestamp=snapshot.exchange_ts,
            entry_bid=snapshot.best_bid.price,
            entry_ask=snapshot.best_ask.price
        ))

        self.tg.notify_event({
            "type": "MICRO_IMPULSE",
            "symbol": symbol,
            "side": result["state"],
            "price": snapshot.best_bid.price, 
            "ofi": result["ofi"],
            "timestamp": snapshot.exchange_ts
        })

    def ingest_liquidation(self, symbol: str, side: str, usd_volume: float):
        """Инжектится из вебсокета биржи (stream.liquidation)."""
        self.escrow_guard.register_liquidation_event(symbol, side, usd_volume)
    async def start_monitoring(self, symbol: str, seed_data: List[dict] = None) -> bool:
        """Динамическое подключение нового актива с гидратацией стейта."""
        # [GATEKEEPER] Prevent resurrecting a buried coin
        if self.quarantine.is_dead(symbol):
            logger.debug(f"🪦 [Vanguard] Refused to hydrate {symbol}. It rests in peace.")
            return False
            
        try:
            # 1. Инициализируем батчер и дефендер
            if symbol not in self.batchers:
                self.batchers[symbol] = TickBatcher(symbol, self.pool_manager, self.on_math_results)
            
            if symbol not in self.micro_defenders:
                self.micro_defenders[symbol] = MicrostructureDefender(symbol=symbol)
            
            # 2. Гидратация (Cold Start Solution)
            if seed_data:
                logger.info(f"⏳ [Orchestrator] Hydrating {symbol} with {len(seed_data)} ticks...")
                for i, t in enumerate(seed_data):
                    asyncio.create_task(self.batchers[symbol].add_tick({
                        "symbol": symbol,
                        "price": float(t["price"]), 
                        "volume": float(t["size"]), 
                        "side": t["side"],
                        "timestamp": int(t["time"])
                    }))
                    if i % 100 == 0:
                        await asyncio.sleep(0)
                logger.success(f"✅ [Orchestrator] {symbol} hydrated. Mathematical continuity established.")
            
            # 3. Подписываемся на стрим
            if symbol not in self.ingestor.symbols:
                self.ingestor.symbols.append(symbol)
                await self.ingestor.subscribe([symbol])
            
            return True
        except Exception as e:
            logger.error(f"❌ [Orchestrator] Failed to start monitoring {symbol}: {e}")
            return False

    async def stop_monitoring(self, symbol: str):
        """Отключение актива и очистка памяти."""
        if symbol in self.ingestor.symbols and symbol not in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
            self.ingestor.symbols.remove(symbol)
            await self.ingestor.unsubscribe([symbol])
        
        if symbol in self.batchers:
            del self.batchers[symbol]
        if symbol in self.micro_defenders:
            del self.micro_defenders[symbol]
        if symbol in self.macro_states:
            del self.macro_states[symbol]
        
        # [PHANTOM FLATLINE FIX] Decouple watchdog from decommissioned symbols
        self.flatline_sentinel.remove_symbol(symbol)

    async def stop(self):
        logger.warning("🔌 [CORE] Initiating shutdown...")
        await self.tg.notify_manual("🔌 <b>[ОФФЛАЙН]</b> Система завершает работу. Все задачи будут остановлены корректно.")
        self._shutdown_event.set()
        await self.ingestor.stop()
        tasks = [b.flush_pending_ticks() for b in self.batchers.values()]
        if tasks:
            await asyncio.gather(*tasks)
        # [ИДЕМПОТЕНТНЫЙ ASYNC SHUTDOWN] 
        # Блокирующее ожидание пула вынесено в отдельный поток, чтобы Event Loop жил
        loop = asyncio.get_running_loop()
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, self.pool_manager.shutdown_all),
                timeout=10.0
            )
            logger.info("✅ [SHUTDOWN] Воркеры успешно завершили Liquidity Harvest.")
        except asyncio.TimeoutError:
            logger.error("🚨 [SHUTDOWN] Таймаут пула (10s). Принудительный аборт (ОС убьет пул).")
        await self.db.close()
        await self.tg.stop()
        logger.info("💤 [CORE] Offline.")
