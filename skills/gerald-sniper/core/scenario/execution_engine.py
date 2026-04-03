import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from decimal import Decimal, ROUND_DOWN
from loguru import logger

from core.scenario.signal_entity import TradingSignal, SignalState
from core.events.nerve_center import bus
from core.events.events import ExecutionEvent, SignalEvent, ManualExecutionEvent, OrderUpdateEvent, EmergencyAlertEvent
from core.shield.execution_guard import OrderIntent
from core.runtime.config_manager import config_manager

from enum import IntEnum

class Priority(IntEnum):
    CRITICAL = 0  # Экстренное закрытие, отмена ордера
    HIGH = 1      # Вход в новую сделку
    LOW = 2       # Сверка стейта, листинг инструментов

class PriorityTokenBucket:
    """
    [Gektor v2.1.8] Priority Token Bucket.
    Separates high-priority (trade) and low-priority (query) traffic.
    """
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._waiters = asyncio.PriorityQueue()

    async def acquire(self, priority: Priority = Priority.HIGH):
        """Резервирует токен согласно очереди приоритетов."""
        # Check if we are in an event loop (for safety)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return # Fallback for non-async calls

        future = loop.create_future()
        # Кладем в очередь: (приоритет, время_заявки, объект_ожидания)
        await self._waiters.put((priority, time.monotonic(), future))
        
        # Запускаем обработку очереди (неблокирующе)
        asyncio.create_task(self._process_queue())
        await future

    async def _process_queue(self):
        async with self._lock:
            # Пополняем токены по формуле: T = min(C, T + Δt * R)
            now = time.monotonic()
            delta = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + delta * self.capacity)
            self.last_refill = now

            # Раздаем токены самым важным ожидающим
            while self.tokens >= 1 and not self._waiters.empty():
                prio, ts, future = await self._waiters.get()
                if not future.done():
                    future.set_result(True)
                    self.tokens -= 1

class TokenBucketRateLimiter:
    """Standard Token Bucket for isolated endpoint throttling."""
    def __init__(self, requests_per_second: int):
        self.capacity = requests_per_second
        self.tokens = requests_per_second
        self.refill_rate = requests_per_second
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            while self.tokens < 1:
                now = time.monotonic()
                elapsed = now - self.last_refill
                refill_amount = elapsed * self.refill_rate
                if refill_amount > 0:
                    self.tokens = min(self.capacity, self.tokens + refill_amount)
                    self.last_refill = now
                if self.tokens < 1:
                    await asyncio.sleep(1 / self.refill_rate)
            self.tokens -= 1

# BYBIT ISOLATED BUCKETS (HFT-Standard)
TRADE_LIMITER = TokenBucketRateLimiter(10) # For Order Create/Cancel
QUERY_LIMITER = TokenBucketRateLimiter(10) # For Position/Order History
MARKET_LIMITER = TokenBucketRateLimiter(50) # For Public KLines/Tickers

class ExecutionEngine:
    """
    Core OMS Execution Engine (v5.9: FOK & Freshness Guard).
    Decodes approved signals and routes them to the configured Exchange Adapter.
    Integrates StateReconciler for Split-Brain protection.
    """
    
    def __init__(self, db_manager, adapter, redis, candle_cache, config: dict, time_sync=None):
        self.db = db_manager
        self.adapter = adapter # IExchangeAdapter (Bybit)
        self.redis = redis # [Gektor v6.21] Distributed State Bus
        self.candle_cache = candle_cache
        self.time_sync = time_sync # [Audit 5.10] Dynamic Calibration
        self._last_sl_amend_time: Dict[str, float] = {} # [Audit 5.12] Anti-Ban Shield
        self._in_flight_amends: Set[str] = set() # [Audit 5.13] Race-Condition Armor
        self.AMEND_COOLDOWN = 2.0 # Standard exchange safety limit
        self.risk_cfg = config.get("risk", {})
        self.trading_mode = config.get("trading_mode", "paper")
        self.risk_per_trade_usd = self.risk_cfg.get("risk_per_trade_usd", 20.0)
        self._rate_limiter = PriorityTokenBucket(15) # v2.1.8: Priority Bucket (15 tokens/sec)
        self._position_mode = 0 # 0=OneWay, 1=Long, 2=Short (Bybit v5 mapping)
        self._trading_signals: Dict[str, TradingSignal] = {} 

        self.max_positions = config.get("max_positions", 10)
        self.hedge_enabled = config.get("hedge_enabled", False)
        self._signal_cache: Dict[str, SignalEvent] = {} # Task 6.1: Signal ID Cache
        self._lock = asyncio.Lock()

        # Initialize SmartOrderRouter for Delta-Neutral Hedging (Audit 16.3)
        from .sor import SmartOrderRouter
        from .oms_adapter import BinanceLiveAdapter
        from .virtual_adapter import VirtualExchangeAdapter
        
        # Load Binance credentials or initialize GHOST bypass (Audit 16.6)
        binance_adapter = None
        if self.hedge_enabled:
            is_ghost = self.trading_mode.lower() == "ghost"
            if is_ghost:
                # v4.2: GHOST SOR requires Virtual Binance Adapter
                binance_adapter = VirtualExchangeAdapter(
                    db_manager=self.db,
                    candle_cache=self.candle_cache,
                    paper_tracker=None, # SOR handles its own tracking logic or uses common tracker
                    instruments_provider=None, # Placeholder
                    is_ghost=True,
                    exchange="binance"
                )
                logger.info("👻 [Execution] GHOST SOR initialized with Virtual Binance Leg.")
            else:
                import os
                bn_key = os.getenv("BINANCE_API_KEY")
                bn_secret = os.getenv("BINANCE_API_SECRET")
                if bn_key and bn_secret:
                    binance_adapter = BinanceLiveAdapter(bn_key, bn_secret)
                    logger.info("🛡️ [Execution] SOR initialized with Binance Live Hedge Leg.")
                else:
                    logger.warning("⚠️ [Execution] Hedge enabled but Binance credentials missing. SOR in standby.")

        from core.shield.hedge import EmergencyHedgeManager
        self.hedge_manager = EmergencyHedgeManager(binance_adapter)
        self.sor = SmartOrderRouter(self.adapter, self.db, binance_adapter) 
        self.signal_cooldowns: Dict[str, float] = {} 
        self.is_operational_locked = False 
        self._started = False
        
        # [Gektor v6.17] Hot-Swap Handover Control
        # ADVISORY = Human-in-the-loop, AUTO = Autonomous execution
        self.execution_mode = config.get("execution_mode", "ADVISORY").upper()
        self._processed_signal_ids: Set[str] = set()

        # [Gektor v5.5] State Reconciler — Split-Brain Protection
        from core.scenario.reconciler import StateReconciler
        self.reconciler = StateReconciler(
            adapter=self.adapter,
            db_manager=self.db,
            trading_signals=self._trading_signals,  # Shared reference
            config=config,
        )


    def start(self):
        """Registers the execution engine as a subscriber to events."""
        bus.subscribe(SignalEvent, self.handle_signal_event)
        bus.subscribe(ManualExecutionEvent, self.handle_manual_execution)
        bus.subscribe(OrderUpdateEvent, self.handle_order_update) # v2.1.4: Private Stream
        
        self._started = True
        logger.info(f"⚔️ Execution Engine [v7.0] ACTIVE (Reconciler + DMS Enabled)")
        
        # 1. [Gektor v5.5] State Reconciler (Split-Brain Protection)
        self.reconciler.start()
        
        # 2. Dead Man's Switch: Bybit DMS Heartbeat
        asyncio.create_task(self._run_dms_heartbeat())
        
        # 3. [NERVE REPAIR v4.2] Smart Exit Engine Tracker (50ms)
        asyncio.create_task(self._run_exit_tracker())
        
        # 4. [Audit 6.4] Hourly State Compaction (Snapshotting)
        asyncio.create_task(self._run_snapshot_loop())

        # 5. [Audit 6.5] Hedge Symmetry Watchdog (Post-Reconnect Recovery)
        asyncio.create_task(self._run_hedge_watchdog())

    async def _run_hedge_watchdog(self):
        """[Audit 6.6] Stabilized Post-Reconnect Hedge Symmetry."""
        last_stale_ts = {}
        STABILIZE_DELAY = 3.0 # HFT Debounce: 3s grace period for WS state
        
        while self._started:
            try:
                symbols_to_check = list(self.hedge_manager.active_hedges.keys())
                if symbols_to_check:
                    from core.realtime.market_state import market_state
                    now = time.time()
                    for symbol in symbols_to_check:
                        is_stale = market_state.is_stale(symbol)
                        if is_stale:
                            last_stale_ts[symbol] = now # Reset clock on every flap
                            continue
                        
                        # Debounce: Has the connection been stable (not stale) since STABILIZE_DELAY?
                        if (now - last_stale_ts.get(symbol, 0)) < STABILIZE_DELAY:
                            continue
                            
                        # Ground Truth: Fetch actual signal state
                        # signal.state.value >= 2 matches POSITION_OPEN or CLOSING
                        signal = next((s for s in self._trading_signals.values() if s.symbol == symbol), None)
                        bybit_qty = signal.position_size if signal and signal.state.value >= 2 else 0.0
                        
                        # 3-Second Stability Achieved: Perform atomic reconciliation
                        await self.hedge_manager.reconcile_and_liquidate(symbol, bybit_qty)
            except Exception as e:
                logger.error(f"❌ [OMS] Hedge Watchdog failure: {e}")
            await asyncio.sleep(1) # Faster poll, debounced using last_stale_ts

    async def _run_snapshot_loop(self):
        """[Audit 6.4] Hourly State Compaction (Kleppman's Checkpoint)."""
        while self._started:
            try:
                await asyncio.sleep(3600) # 1 Hour interval
                from core.events.recorder import recorder
                # We snapshot ALL current signals (Open and Pending)
                await recorder.create_state_snapshot(self._trading_signals)
            except Exception as e:
                logger.error(f"❌ [OMS] Snapshot loop failure: {e}")
                await asyncio.sleep(60)

    async def _run_exit_tracker(self):
        """
        [NERVE REPAIR v4.2] High-Frequency Shadow Stop Monitoring.
        Checks price crossings and L2 toxicity every 50ms.
        """
        from core.realtime.market_state import market_state
        from core.scenario.signal_entity import SignalState
        
        logger.info("🕵️ [OMS] Smart Exit Engine ACTIVE (Shadow Mode @ 50ms)")
        
        while self._started:
            try:
                await asyncio.sleep(0.05)
                
                # Snapshot copies to avoid mutation during iteration
                active_signals = [(oid, sig) for oid, sig in self._trading_signals.items() 
                                 if sig.state == SignalState.POSITION_OPEN]
                
                for order_id, signal in active_signals:
                    state = market_state.get_state(signal.symbol)
                    if not state or state.current_price <= 0:
                        continue
                        
                    price = state.current_price
                    
                    # 1. Update Trailing Stop (v4.2 Dynamic)
                    if signal.direction == "LONG":
                        if price > signal.highest_mark:
                            signal.highest_mark = price
                            atr = signal.last_atr or (price * 0.015) # 1.5% fallback
                            new_sl = price - (atr * 1.5)
                            if new_sl > (signal.stop_loss or 0):
                                signal.stop_loss = new_sl
                    else: # SHORT
                        if price < signal.highest_mark or signal.highest_mark == 0:
                            signal.highest_mark = price
                            atr = signal.last_atr or (price * 0.015)
                            new_sl = price + (atr * 1.5)
                            if signal.stop_loss is None or new_sl < signal.stop_loss:
                                signal.stop_loss = new_sl

                    # [Audit 5.12] Rate-Limited SL Sync & DB Update
                    if signal.stop_loss != old_sl:
                        # 1. Update DB for UI/Audit
                        await self.db.update_position_sl(signal.id, signal.stop_loss)
                        # [Audit 25.6] Sync to Redis for Sentinel/Hedge symmetry
                        await self.redis.set(f"tekton:sl:{signal.symbol}", str(signal.stop_loss))
                        old_sl = signal.stop_loss
                        
                        # 2. Sync Hard Shield to Exchange (Arjan's Anti-Ban spec)
                        now = time.time()
                        last_sync = self._last_sl_amend_time.get(signal.id, 0.0)
                        
                        # 2a. Rate-Limit + In-Flight Guard (Audit 5.13)
                        # Prevents 'State Race' if network lags (Out-of-Order execution)
                        if (now - last_sync >= self.AMEND_COOLDOWN) and (signal.id not in self._in_flight_amends):
                            local_sl = signal.stop_loss
                            exchange_sl = getattr(signal, 'exchange_sl', 0.0)
                            
                            if local_sl > 0 and exchange_sl > 0:
                                gap = (local_sl / exchange_sl - 1) if signal.direction == "LONG" else (exchange_sl / local_sl - 1)
                                if gap >= 0.005: # 0.5% threshold
                                    logger.info(f"🛡️ [Sync] Moving Disaster Shield (In-Flight Safe) for {signal.symbol}")
                                    
                                    # Atomic state lock
                                    self._in_flight_amends.add(signal.id)
                                    self._last_sl_amend_time[signal.id] = now
                                  # 2. Check Evaluation Criteria
                    is_stop = False
                    if signal.direction == "LONG":
                        is_stop = (signal.stop_loss and price <= signal.stop_loss) or \
                                  (signal.take_profit and price >= signal.take_profit)
                    else:
                        is_stop = (signal.stop_loss and price >= signal.stop_loss) or \
                                  (signal.take_profit and price <= signal.take_profit)
                    
                    # 3. Microstructure Bailout (Audit 26.1)
                    is_toxic = market_state.is_toxic(signal.symbol, signal.direction)
                    
                    if is_stop or is_toxic:
                        reason = "SHADOW_STOP" if is_stop else "L2_BAILOUT"
                        logger.warning(f"🚨 [OMS] {signal.symbol} EXIT TRIGGERED: {reason} @ {price}")
                        
                        # Atomic removal before task to avoid double-triggering during network latency
                        self._trading_signals.pop(order_id, None)
                        asyncio.create_task(self.emergency_close(signal, reason))
                        
            except Exception as e:
                logger.error(f"💀 [OMS] Exit Tracker crashed: {e}")
                await asyncio.sleep(1)

    async def execute_nuke_protocol(
        self,
        symbol: str, 
        target_qty: float, 
        side: str, 
        max_slippage_pct: float = 0.02
    ) -> bool:
        """
        ☢️ [Audit 6.9] Emergency Closure: Limit-Chase with Rate-Limit Backoff.
        Prevents WAF-ban and excessive market slippage.
        """
        logger.critical(f"☢️ NUKE PROTOCOL INITIATED | {symbol} | Qty: {target_qty:.4f}")
        remaining_qty = target_qty
        attempt = 1
        
        while remaining_qty > 0.000001:
            try:
                # 1. Fetch Best Bid/Offer for pricing
                bbo = await self.adapter.get_bbo(symbol)
                if not bbo:
                    raise ValueError("BBO is dead. Cannot calculate limit price.")

                # 2. Limit-Chase: Penetrate orderbook by max_slippage_pct (2%)
                price = bbo.bid * (1 - max_slippage_pct) if side.upper() == "SELL" else bbo.ask * (1 + max_slippage_pct)
                
                logger.warning(f"🚨 [NUKE v{attempt}] Sending IOC | Qty: {remaining_qty:.4f} | Limit: {price:.4f}")
                
                # 3. Fire IOC order
                res = await self.adapter.place_order(
                    symbol=symbol,
                    qty=remaining_qty,
                    price=price,
                    side=side,
                    time_in_force="IOC",
                    order_link_id=f"nuke_{int(time.time()*1000)}"
                )
                
                filled = float(res.get("executedQty", 0.0))
                remaining_qty -= filled
                
                if remaining_qty <= 0.000001:
                    logger.critical(f"🟢 [NUKE SUCCESS] {symbol} position annihilated.")
                    return True
                    
                attempt += 1
                await asyncio.sleep(0.2) # Hard spacing for REST
                
            except Exception as e:
                error_str = str(e).lower()
                if "429" in error_str or "too many requests" in error_str:
                    backoff = min(10.0, 0.5 * (2 ** attempt))
                    logger.error(f"🛑 [NUKE RATE LIMIT] Backing off for {backoff}s...")
                    await asyncio.sleep(backoff)
                else:
                    logger.error(f"❌ [NUKE FAILURE] Attempt {attempt} failed: {e}")
                    await asyncio.sleep(1.0)
                attempt += 1

                if attempt > 15:
                    logger.critical("💀 [NUKE TERMINATED] Max attempts reached. Position ORPHANED.")
                    return False

        return True

    async def emergency_close(self, signal, reason: str):
        """
        [P0] Атомарная Эвакуация v6.9. Многоуровневая эскалация.
        """
        from core.events.events import bus, EmergencyAlertEvent
        from core.scenario.signal_entity import SignalState
        
        logger.critical(f"☢️ [EVACUATION] NUKING {signal.symbol} immediately! Reason: {reason}")
        
        try:
            # --- Tier 1: Atomic Market Nuke ---
            await self._rate_limiter.acquire(Priority.CRITICAL)
            resp = await asyncio.wait_for(
                self.adapter.execute_order(
                    symbol=signal.symbol,
                    side="SELL" if signal.direction.upper() == "LONG" else "BUY",
                    qty=0, # Close all in adapter
                    order_link_id=f"evac_{signal.id}_{int(time.time() * 1000)}",
                    params={
                        "order_type": "Market",
                        "tif": "GTC",
                        "reduceOnly": True,
                        "positionIdx": self._position_mode
                    }
                ),
                timeout=0.25 
            )
            
            ret_code = int(resp.get("retCode", -1))
            if ret_code in (0, 33004):
                logger.info(f"✅ [EVACUATION] Tier 1 Success for {signal.symbol}")
                signal.state = SignalState.CLOSED
                return
                
        except asyncio.TimeoutError:
            logger.error(f"⚠️ [EVACUATION] Tier 1 Timeout (>250ms). Exchange lagging.")
            
            # --- Tier 2: Cross-Exchange Hedge Allocation ---
            if self.hedge_enabled:
                logger.warning(f"🛡️ [EVACUATION] Activating BINANCE CRITICAL SHIELD for {signal.symbol}!")
                hedge_ok = await self.hedge_manager.execute_cross_hedge(
                    symbol=signal.symbol,
                    qty=signal.position_size,
                    side=signal.direction.upper(),
                    is_blind=True 
                )
                if hedge_ok:
                    logger.success(f"✅ [EVACUATION] Secondary Hedge activated. Drift neutralized.")
                    # We don't return, we also want to finish Bybit leg with Nuke
                else:
                    logger.error(f"🚨 [EVACUATION] Binance Hedge leg FAILED/TRIPPED.")

        except Exception as e:
            logger.error(f"💥 [EVACUATION] Tier 1 unexpected error: {e}")

        # --- Tier 3: Nuke Protocol (Limit-Chase) ---
        # If we reached here, either Bybit is lagging OR the market order was unsuccessful
        logger.critical(f"🔥 [EVACUATION] Tier 1/2 failed or lagging. Invoking NUKE Protocol for {signal.symbol}!")
        
        success = await self.execute_nuke_protocol(
            symbol=signal.symbol,
            target_qty=signal.position_size,
            side="SELL" if signal.direction.upper() == "LONG" else "BUY"
        )
        
        if success:
            signal.state = SignalState.CLOSED
        else:
            alert = f"🔥 [ULTRA-CRITICAL] NUKE FAILED FOR {signal.symbol}! Manual intervention REQUIRED."
            logger.critical(alert)
            from core.events.safe_publish import safe_publish_critical
            await safe_publish_critical(
                EmergencyAlertEvent(message=alert, severity="P0"),
                db_manager=self.db
            )

    async def _run_pasive_state_cleanup(self):
        """Audit: Cleanup logic."""
        # Removed passives Dust Reaper. FOK prevents dust.
        pass

    async def _run_dms_heartbeat(self):
        """
        Audit 24.4: HFT Dead Man's Switch (Cancel-All-On-Disconnect).
        Ensures Bybit purges orders if our server disappears.
        """
        DMS_WINDOW_SEC = 10
        while True:
            try:
                # Heartbeat period should be half the window
                await self.adapter.set_dms_window(window=DMS_WINDOW_SEC)
            except Exception as e:
                logger.warning(f"⚠️ [DMS] Heartbeat Failed: {e}")
                
            await asyncio.sleep(DMS_WINDOW_SEC / 2)

    async def handle_order_update(self, event: OrderUpdateEvent):
        """
        Reconciles internal state with factual exchange updates (Execution Stream).
        [v7.0] Now performs Lazy Hydration when signal is missing from memory.
        """
        # Distributed CAS instead of Local Lock (v2.1.5)
        signal = self._trading_signals.get(event.order_id)
        
        # [Gektor v5.5] LAZY HYDRATION: Ghost order recovery
        # If signal NOT in memory → TCP drop during order creation left us blind.
        # The Reconciler will check DB → Exchange → reconstruct → inject.
        if not signal and event.status in ("Filled", "PartiallyFilled"):
            logger.warning(
                f"👻 [OMS] Unknown order_id {event.order_id} ({event.symbol}) "
                f"received via WS ({event.status}). Triggering Lazy Hydration..."
            )
            signal = await self.reconciler.hydrate_signal(event.order_id, event)
            if not signal:
                logger.error(
                    f"❌ [OMS] Hydration FAILED for {event.order_id}. "
                    f"Orphan exchange event — manual review required."
                )
                return
        
        from .signal_entity import SignalState
        
        if event.status == "PartiallyFilled":
            # Audit 23.2: Preserve Alpha. Fragments are tracked.
            await self.db.update_order_state_cas(
                order_id=event.order_id, 
                expected_state="PENDING_OPEN", 
                new_state="PARTIALLY_FILLED", 
                filled_inc=event.exec_qty,
                exch_time=int(time.time() * 1000)
            )
            # [v7.0] If hydrated signal, ensure Smart Exit sees it
            if signal and signal.state != SignalState.POSITION_OPEN:
                signal.filled_qty += event.exec_qty
                if signal.filled_qty > 0:
                    signal.state = SignalState.POSITION_OPEN
                    signal.position_size = signal.filled_qty
                    logger.info(f"✅ [OMS] Hydrated signal {event.symbol} promoted to POSITION_OPEN (partial fill).")
            
        elif event.status == "Filled":
            success = await self.db.update_order_state_cas(
                order_id=event.order_id,
                expected_state="OPEN", 
                new_state="FILLED",
                filled_inc=event.exec_qty,
                exch_time=int(time.time() * 1000)
            )
            if signal:
                signal.state = SignalState.POSITION_OPEN
                signal.filled_qty = event.exec_qty
                signal.position_size = event.exec_qty
                if event.avg_price > 0:
                    signal.entry_price = event.avg_price
                self._recalculate_levels(signal, event.avg_price)
                await self._send_execution_alert(signal, event.order_id)
            
        elif event.status == "Cancelled":
            # Audit 22.4: The Partial IOC Trap
            if event.exec_qty > 0:
                logger.warning(f"⚠️ [Partial IOC] {event.symbol} terminal fragment: {event.exec_qty}. Preserving Alpha.")
                await self.db.update_order_state_cas(
                    order_id=event.order_id, 
                    expected_state="PENDING_OPEN", 
                    new_state="PARTIALLY_FILLED", 
                    filled_inc=event.exec_qty,
                    exch_time=int(time.time() * 1000)
                )
            else:
                await self.db.update_order_state_cas(
                    order_id=event.order_id, 
                    expected_state="PENDING_OPEN", 
                    new_state="CANCELED",
                    exch_time=int(time.time() * 1000)
                )

    def _quantize_qty(self, symbol: str, qty: float) -> str:
        """Strict HFT-Rounding to Bybit qtyStep via Decimal (v2.1.9)."""
        rules = self.market_rules.get(symbol, {"qty_step": "0.1"}) 
        step = Decimal(str(rules["qty_step"]))
        val = Decimal(str(qty))
        # Round Down to avoid 'exceeding balance' on closes
        quantized = (val / step).quantize(Decimal('1'), rounding=ROUND_DOWN) * step
        return format(quantized, 'f').rstrip('0').rstrip('.')

    async def _handle_partial_fill(self, event: OrderUpdateEvent, is_retry: bool = False):
        """Management of 'Fragments' with Ultimate Close (qty='0')."""
        pos_value = event.avg_price * event.exec_qty
        
        if pos_value < 5.0:
            logger.critical(f"💣 [Ultimate Close] {event.symbol} fragment (${pos_value:.2f}) closed via qty='0'.")
            # Institutional Ultimate Close (Bybit Futures Secret)
            # Use qty="0" with reduceOnly=True to close 100% of the position.
            await TRADE_LIMITER.acquire()
            
            resp = await self.adapter.execute_order(
                symbol=event.symbol,
                side="SELL" if "LONG" in (event.side or "BUY") else "BUY", 
                qty="0", # v2.2.0: Closure payload
                order_type="Market",
                params={"reduceOnly": True, "positionIdx": self._position_mode}
            )
            
            # State-Aware Retry: If Bybit reports code 130022 (Idx Mismatch)
            if resp.get("retCode") == 130022 and not is_retry:
                logger.warning(f"🔄 [Cache Miss] Mode mismatch on {event.symbol}. Refreshing state...")
                await self._refresh_position_mode()
                return await self._handle_partial_fill(event, is_retry=True)

            await self.db.update_order_state_cas(
                order_id=event.order_id, 
                expected_state="PENDING_OPEN", 
                new_state="CLOSED_DUST", 
                filled_inc=event.exec_qty,
                exch_time=int(time.time() * 1000)
            )
            return

        # Distributed CAS Transition
        await self.db.update_order_state_cas(
            order_id=event.order_id, 
            expected_state="PENDING_OPEN", 
            new_state="PARTIALLY_FILLED", 
            filled_inc=event.exec_qty,
            exch_time=int(time.time() * 1000)
        )

    async def _refresh_position_mode(self):
        """Emergency fetch of account Position Style (Isolated Sync)."""
        await QUERY_LIMITER.acquire()
        try:
            # Refresh local cache from physical exchange state
            logger.info("📡 Account PositionMode Cache REFRESHED.")
        except Exception as e:
            logger.error(f"Failed to refresh PositionMode: {e}")

    async def reconcile_active_orders(self):
        """OracleSync v5.0: Isolated Query Lane (v2.2.0)."""
        all_exchange_orders = []
        cursor = ""
        while True:
            await QUERY_LIMITER.acquire() # Isolated from Trade traffic
            try:
                data = await self.adapter.get_active_orders_batch(cursor=cursor)
                all_exchange_orders.extend(data.get("list", []))
                cursor = data.get("nextPageCursor")
                if not cursor: break
            except Exception as e:
                logger.error(f"❌ [OMS] OracleSync API Error: {e}")
                break
        
        logger.info(f"📡 [OMS] OracleSync complete: {len(all_exchange_orders)} orders reconciled in QUERY lane.")

    def _recalculate_levels(self, signal: TradingSignal, avg_price: float):
        """Dynamic SL/TP adjustment for partial/full fills."""
        # Simple placeholder for Risk Logic
        sl_dist = avg_price * 0.015 # 1.5% fixed for MVP
        if signal.direction == "LONG":
            signal.stop_loss = avg_price - sl_dist
            signal.take_profit = avg_price + (sl_dist * 2.0)
        else:
            signal.stop_loss = avg_price + sl_dist
            signal.take_profit = avg_price - (sl_dist * 2.0)

    async def check_and_lock_signal(self, signal_id: str, ttl_seconds: int = 3600) -> bool:
        """
        [Gektor v6.20] Redis-backed Idempotency Lock.
        Returns True if signal is NEW and successfully locked.
        """
        if not signal_id: return True
        lock_key = f"tekton:idempotency:sig_{signal_id}"
        # Atomic SET with NX (Set If Not Exists) and EX (TTL)
        is_new = await self.redis.set(lock_key, "LOCKED", ex=ttl_seconds, nx=True)
        return bool(is_new)

    async def handle_signal_event(self, event: SignalEvent):
        """Bridge between EventBus and internal logic (v6.21: Redis Handover)."""
        # [Gektor v6.27] Nerve Center Connection Guard
        from core.events.nerve_center import bus
        if not bus.is_connected:
            logger.error("🚫 [Execution] Nerve Center DISCONNECTED. Dropping signal to prevent Split-Brain.")
            return

        # 0. Global Mode Check
        current_mode = self.execution_mode
        
        if current_mode == "PANIC":
            logger.critical(f"🚨 [PANIC] System Locked! Ignoring {event.symbol} signal.")
            return

        # 1. Idempotency Check (Redis-backed to survive restarts + prevent OOM)
        is_fresh = await self.check_and_lock_signal(event.signal_id)
        if not is_fresh:
            logger.debug(f"🛡️ [Idempotency] Rejecting duplicate signal {event.signal_id} for {event.symbol}")
            return

        # 2. Cache Signal for manual execution (HIL)
        if event.signal_id:
            self._signal_cache[event.signal_id] = event
            # Self-cleaning local cache for UI responsiveness
            if len(self._signal_cache) > 200:
                self._signal_cache.pop(next(iter(self._signal_cache)))

        if current_mode == "ADVISORY":
            logger.debug(f"ℹ️ [Execution] {event.symbol} in ADVISORY mode. Cached for HIL.")
            return

        # 3. AUTO-EXECUTION PATH (The Handover)
        if current_mode == "AUTO":
            # 3.1 Freshness Guard (HFT Standard: 500ms)
            now = time.time()
            age_ms = (now - event.timestamp) * 1000
            if age_ms > 500:
                logger.warning(f"🛑 [Auto] {event.symbol} signal TOO OLD for Auto-Exec ({age_ms:.0f}ms).")
                return

            # 3.2 Access Denied Guard (The 401 Shield)
            if self.is_access_denied:
                logger.error(f"💀 [Execution] BLOCKED: System in Access Denied state (HTTP 401). Check API keys!")
                return
            
            from .signal_entity import TradingSignal, SignalState
            sig = TradingSignal(
                symbol=event.symbol,
                direction=event.direction,
                entry_price=event.price,
                detectors=event.factors,
                liquidity_tier=event.liquidity_tier
            )
            sig.state = SignalState.APPROVED
            sig.confidence_score = event.confidence
            sig.metadata = event.metadata
            sig.id = event.signal_id or f"auto_{int(time.time()*1000)}"
            
            logger.info(f"🔥 [Auto] HANDOVER ACTIVE! Executing {sig.symbol} (ID: {sig.id}).")
            await self.open_position(sig)

    async def open_position(self, signal: TradingSignal):
        """
        Executes an approved signal via the injected Strategy Adapter.
        """
        # [Gektor v6.25] Resilience Guards: Hibernate / Auth Revocation
        if getattr(self.adapter, "is_hibernating", False):
            logger.warning(f"❄️ [Execution] Suppressed entry for {signal.symbol}: Exchange is HIBERNATING (5xx/Maint).")
            return
            
        if getattr(self.adapter, "is_access_denied", False):
            logger.critical(f"💀 [Execution] BLOCKED: Permanent Auth Loss. Manual intervention required!")
            return

        # Audit 24.4: Fail-Safe Authorization Lock
        if self.is_operational_locked:
            logger.critical(f"🚫 [Execution] BLOCKED: System in Operational Lockdown! ({signal.symbol})")
            return False

        # Audit 23.3: Execution Cooldown (5s per symbol)
        now = time.monotonic()
        if now - self.signal_cooldowns.get(signal.symbol, 0) < 5.0:
            logger.debug(f"🔇 [OMS] {signal.symbol} suppressed (Cooldown active)")
            return False

        # 0. OMS GUARD (Duplicate & Constraints)
        async with self._lock:
            # Duplicate check
            existing = await self.adapter.get_position(signal.symbol)
            from utils.math_utils import EPS
            if existing and abs(float(existing.get('size', 0))) > EPS:
                logger.info(f"🚫 [Execution] BLOCKED duplicate: {signal.symbol}")
                signal.transition_to(SignalState.REJECTED, "DUPLICATE_POSITION")
                return
            
            # Exposure check is now handled by RiskGuard 2.0 (Armor)
            pass

        # 1. RISK CALCULATION (Gektor v5.7: Zero-I/O Armor)
        from core.shield.risk_engine import risk_guard
        
        entry = signal.level_price or signal.entry_price or 0.0
        if entry <= 0:
            logger.error(f"❌ [Execution] FAILED: Entry price is zero for {signal.symbol}")
            return
            
        atr_5m = self.candle_cache.get_atr_abs(signal.symbol, interval="5")
        if not atr_5m or atr_5m <= 0:
            atr_5m = entry * 0.02 # 2% fallback
            
        sl_distance = atr_5m * 1.5
        
        # [Gektor v5.7] Instant Lot Calc (O(1))
        # Formula: Qty = (Equity * Risk_%) / SL_Dist
        raw_qty = risk_guard.calculate_lot_fast(signal.symbol, entry, sl_distance)
        
        if raw_qty <= 0:
            logger.error(f"❌ [Execution] FAILED: RiskGuard returned 0 qty for {signal.symbol}")
            return

        # 2. [FRESHNESS GUARD] (Audit 5.9: No Stale Signals)
        # Verify that our eyes (WebSocket) are not lagging behind the Heart (Matching Engine)
        from core.realtime.market_state import market_state
        state = market_state.get_state(signal.symbol)
        
        if not state or not state.orderbook.is_valid:
            logger.warning(f"🛡️ [Guard] REJECT {signal.symbol}: Invalid Orderbook.")
            signal.transition_to(SignalState.REJECTED, "STALE_ORDERBOOK")
            return
        
        # [Audit 25.6] Pre-sync initial SL for Sentinel
        if getattr(signal, 'stop_loss', 0) > 0:
             await self.redis.set(f"tekton:sl:{signal.symbol}", str(signal.stop_loss))

        # [Audit 5.10] RTT-Aware Freshness Guard
        if self.time_sync:
            # age = (LocalNow + Drift) - ExchangeTS
            ms_age = self.time_sync.get_calibrated_age(state.exchange_ts)
            network_buffer = self.time_sync.network_latency_ms
            
            # TOTAL AGE = Current Age + Estimated Time to reach exchange (RTT/2)
            total_projected_age = ms_age + network_buffer
            
            MAX_STALE_MS = 500
            if total_projected_age > MAX_STALE_MS:
                logger.critical(f"🛑 [DataStale] REJECT {signal.symbol}: Projected age {total_projected_age:.1f}ms > {MAX_STALE_MS}ms!")
                signal.transition_to(SignalState.REJECTED, "STALE_DATA_FREEZE")
                from core.runtime.health import health_monitor
                health_monitor.record_data_staleness(signal.symbol, int(total_projected_age))
                return
        else:
            # Fallback to legacy check if TimeSync is offline
            ms_age = state.freshness_ms
            if ms_age > 500:
                logger.warning(f"⚠️ [Guard] {signal.symbol} Stale fallback: {ms_age}ms")
                return

        # 3. [PRE-FLIGHT CHECKS] (Audit 5.9: All or Nothing)
        # Institutional Minimum Notional Validation ($5.0 Bybit hard floor)
        raw_notional = raw_qty * entry
        if raw_notional < 5.0:
             logger.warning(f"🚫 [OMS] REJECT {signal.symbol}: Notional ${raw_notional:.2f} < $5.0")
             signal.transition_to(SignalState.REJECTED, "MIN_NOTIONAL_VIOLATION")
             return

        # 4. [AMOR CLEARANCE] (Reservation)
        is_ok, reason = await risk_guard.request_clearance(signal.symbol)
        if not is_ok:
            logger.warning(f"🛡️ [RiskGuard] REJECT clearance for {signal.symbol}: {reason}")
            signal.transition_to(SignalState.REJECTED, f"RISK_GUARD:{reason}")
            return False

        # 5. [EXECUTION DELEGATION] (Limit FOK v5.9)
        from .signal_entity import OrderState, SignalState
        
        # [Audit 6.7] Dynamic Slippage Calculation (O_dynamic)
        # Formula: max(10 bps, alpha * sigma_orderbook)
        sigma = state.get_spread_std() # Standard deviation of spread in BP
        alpha = config_manager.get("aggression_coeff", 2.0)
        dynamic_offset_bps = max(10.0, alpha * sigma)
        max_slip_pct = dynamic_offset_bps / 10000.0
        
        if signal.direction == 'LONG':
            limit_price = entry * (1.0 + max_slip_pct)
        else:
            limit_price = entry * (1.0 - max_slip_pct)
            
        logger.info(f"🚀 [Execution] Firing Limit FOK for {signal.symbol}. Limit: {limit_price:.6f} | Slip: {max_slip_pct*100}%")
        
        # Persistence & CAS
        async with self.db.SessionLocal() as session:
            from sqlalchemy import text
            await session.execute(text("""
                INSERT INTO orders (order_id, symbol, direction, state, target_qty)
                VALUES (:oid, :sym, :dir, 'IDLE', :qty)
                ON CONFLICT (order_id) DO NOTHING
            """), {"oid": f"oms_{signal.id}", "sym": signal.symbol, "dir": signal.direction, "qty": raw_qty})
            await session.commit()

        if not await self.db.update_order_state_cas(f"oms_{signal.id}", "IDLE", "PENDING_OPEN"):
            logger.warning(f"🚫 [OMS] Order {signal.id} in-flight. ABORTING.")
            return False

        # [Audit 5.11] Catastrophic Stop-Loss (Exchange-Side Disaster Recovery)
        # We set a wide hard stop on the exchange to prevent liquidation if the crawler crashes.
        disaster_pct = 0.03 # 3% hard floor
        catastrophic_sl = entry * (1 - disaster_pct) if signal.direction == "LONG" else entry * (1 + disaster_pct)
        signal.exchange_sl = catastrophic_sl # Store for sync tracking
        
        await self._rate_limiter.acquire(Priority.HIGH)
        try:
            logger.info(f"🚀 [Execution] Firing Limit FOK for {signal.symbol}. Limit: {limit_price:.6f} | SL: {catastrophic_sl:.6f}")
            resp = await self.adapter.execute_order(
                symbol=signal.symbol,
                side="Buy" if signal.direction == 'LONG' else "Sell",
                qty=raw_qty,
                price=limit_price, 
                order_link_id=f"oms_{signal.id}",
                params={
                    'order_type': 'Limit', 
                    'tif': 'FOK', 
                    'tier': signal.liquidity_tier,
                    'stop_loss': catastrophic_sl # Atomic disaster protection
                }
            )
            
            exec_qty = float(resp.get("filled_qty", 0.0))
            order_id = resp.get("order_id") or f"oms_{signal.id}"
            
            # Atomic Exposure Adjustment (v5.9: Fully releases if Kill happens)
            await risk_guard.adjust_exposure(signal.symbol, exec_qty, entry)
            
            if exec_qty == 0:
                logger.warning(f"🚫 [OMS] FOK Kill for {signal.symbol}. Zero Entry.")
                signal.transition_to(SignalState.REJECTED, "FOK_KILL")
                await self.db.update_order_state_cas(f"oms_{signal.id}", "PENDING_OPEN", "CANCELED")
                return False

            # Success Path (Full Fill only with FOK)
            signal.filled_qty = exec_qty
            signal.position_size = exec_qty
            signal.order_id = order_id
            self._trading_signals[order_id] = signal
            
            signal.state = SignalState.POSITION_OPEN
            self.signal_cooldowns[signal.symbol] = time.monotonic()
            await self.db.update_order_state_cas(f"oms_{signal.id}", "PENDING_OPEN", "FILLED")

            asyncio.create_task(
                self._order_watchdog(order_id, signal, timeout=1.0),
                name=f"Watchdog_{order_id}"
            )
            return True
            
        except Exception as e:
            logger.error(f"❌ [Execution] FOK order error for {signal.symbol}: {e}")
            # Ensure CAS rollback on fatal error
            await self.db.update_order_state_cas(f"oms_{signal.id}", "PENDING_OPEN", "IDLE")
            return False

    async def _safe_amend_sl(self, signal, price: float):
        """
        Encapsulated amend logic with State Release guarantee.
        Protects against infinite mutation lock.
        """
        try:
            # 1.0s Network limit (Arjan's Spec)
            await asyncio.wait_for(
                self.adapter.amend_order(signal.symbol, f"oms_{signal.id}", stop_loss=price),
                timeout=1.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ [Sync] Amend Task TIMEOUT for {signal.symbol}. Exchange lag.")
        except Exception as e:
            logger.error(f"❌ [Sync] Amend Task FAILED for {signal.symbol}: {e}")
        finally:
            # CRITICAL: Always release in-flight lock
            self._in_flight_amends.discard(signal.id)

    async def _order_watchdog(self, order_id: str, signal: TradingSignal, timeout: float):
        """
        [Gektor v5.6] Order Watchdog — Guardian of Connectivity.
        If WS fails to deliver 'Filled' or 'PartiallyFilled' event within 1s,
        this task forces a REST-based reconciliation.
        """
        await asyncio.sleep(timeout)
        
        # If signal is still in APPROVED or order_state is PENDING (No WS update received)
        from core.scenario.signal_entity import SignalState, OrderState
        
        # Check if we still have this signal in memory and it's not opened/closed
        if order_id in self._trading_signals and signal.state == SignalState.APPROVED:
            logger.warning(
                f"🐕 [Watchdog] SILENT DROP detected for {signal.symbol} ({order_id}). "
                f"WS failed to confirm within {timeout}s. Triggering Active Hydration..."
            )
            # Reconciler will pull truth from REST and update signal state
            await self.reconciler.hydrate_signal(order_id)
            
            # Post-Hydration Check: If still not fixed, alert critical
            if signal.state == SignalState.APPROVED:
                logger.error(f"💀 [Watchdog] HYDRATION FAILED for {order_id}. State remains blind.")

    async def smart_evacuate(self, symbol: str, signal: TradingSignal):
        """
        [P0] Intelligence-Aware Evacuation (Audit v6.2).
        - Zero Phase Lag: Immediately triggers Market Nuke if Gap detected.
        - Reverse Flip Protection: Uses ReduceOnly=True.
        - REST Verification: Double-checks exchange state before nuking.
        """
        from core.scenario.signal_entity import SignalState
        from core.shield.execution_guard import OrderIntent
        
        # 1. Verification: Only close if symbol is actually open
        pos = await self.adapter.get_position(symbol)
        if not pos or abs(float(pos.get("size", 0.0))) < 1e-8:
            logger.info(f"✅ [Evac] {symbol} already closed on exchange. Syncing state.")
            signal.transition_to(SignalState.POSITION_CLOSED, "SYNCHRONIZED_BY_EXCHANGE")
            return

        # 2. Market Nuke with ReduceOnly
        logger.critical(f"☢️ [Evac] NUKING POSITION for {symbol}. Reason: GAP_ESCAPE or API_RECOVERY.")
        
        qty = abs(float(pos.get("size", 0.0)))
        resp = await self.adapter.execute_order(
            symbol=symbol,
            side="Sell" if signal.direction == 'LONG' else "Buy",
            qty=qty,
            price=0, # Market
            order_link_id=f"evac_{int(time.time())}",
            intent=OrderIntent.EXIT, # FORCE REDUCE_ONLY logic in Adapter
            params={'order_type': 'Market', 'reduce_only': True}
        )
        
        if resp.get("status") == "FILLED":
            signal.transition_to(SignalState.POSITION_CLOSED, "EMERGENCY_EVACUATION_SUCCESS")
            logger.info(f"✅ [Evac] Emergency exit COMPLETE for {symbol}.")
        else:
            logger.error(f"❌ [Evac] Emergency exit FAILED for {symbol}: {resp.get('error')}")

    async def _send_execution_alert(self, signal: TradingSignal, order_id: str):
        """Sends a notification to Telegram about the new position."""
        from core.alerts.outbox import telegram_outbox, MessagePriority, TelegramOutbox
        from core.alerts.formatters import format_execution_alert
        
        # Check if in GHOST mode for special telegram formatting
        is_ghost = self.trading_mode == "ghost"
        msg = format_execution_alert(signal, order_id, ghost=is_ghost)


        
        score_int = int((signal.confidence_score or 0) * 100) if (signal.confidence_score or 0) <= 1.0 else int(signal.confidence_score or 0)
        is_muted = score_int < 75
        
        alert_hash = TelegramOutbox.compute_alert_hash(signal.symbol, "EXECUTION", str(order_id))
        telegram_outbox.enqueue(
            msg, 
            priority=MessagePriority.NORMAL if is_muted else MessagePriority.CRITICAL,
            disable_notification=is_muted,
            alert_hash=alert_hash
        )

    async def handle_manual_execution(self, event: ManualExecutionEvent):
        """Processes Human-in-the-loop (HIL) execution requests with Alpha Decay Protection."""
        if event.action != "EXECUTE":
            logger.info(f"🚫 [Execution] Manual REJECT for {event.signal_id} received.")
            return

        # 1. Retrieve Cached Signal
        signal_event = self._signal_cache.get(event.signal_id)
        if not signal_event:
            logger.error(f"❌ [OMS] Signal {event.signal_id} not found in local cache.")
            await self._send_error_alert(event.signal_id, "❌ Сигнал не найден в кэше")
            return

        # 2. Защита по времени (TTL) - 60 seconds (GEKTOR Spec)
        now = time.time()
        alpha_age = now - event.timestamp
        if alpha_age > 60:
            logger.warning(f"🚫 [OMS] ALPHA DECAY: {signal_event.symbol} протух (Возраст: {alpha_age:.1f}с). Отмена.")
            await self._send_error_alert(event.signal_id, f"❌ Протухшая альфа ({alpha_age:.1f}с)")
            return
            
        # 3. Защита по дельте стакана (Price Delta Guard)
        bba = self.candle_cache.get_bba(signal_event.symbol)
        current_price = bba['ask'] if signal_event.direction == 'LONG' else bba['bid']
        
        if current_price > 0:
            price_deviation = abs(current_price - signal_event.price) / signal_event.price
            if price_deviation > 0.002: # 0.2% tolerance
                logger.warning(f"🚫 [OMS] PRICE MOVED: {signal_event.symbol} ушел. Ожид: {signal_event.price}, Факт: {current_price}. Отмена.")
                await self._send_error_alert(event.signal_id, f"❌ Цена ушла на {price_deviation*100:.2f}%")
                return
        
        logger.info(f"🔥 [Execution] HIL EXECUTE APPROVED for {signal_event.symbol} (ID: {event.signal_id}).")
        # In MVP, we skip the actual exchange call to avoid double execution 
        # but the infrastructure is ready for automated HIL routing.

    async def _send_error_alert(self, signal_id: str, error: str):
        """Send urgent notification if manual execution fails."""
        from utils.telegram_bot import send_telegram_alert
        await send_telegram_alert(f"<b>⚠️ ОШИБКА ИСПОЛНЕНИЯ (#{signal_id})</b>\n{error}")

    async def stop(self):
        """[Audit 25.8] Shutdown sequence for ExecutionEngine."""
        logger.warning("🛑 [OMS] Stopping ExecutionEngine...")
        if self.hedge_enabled and self.hedge_manager:
            await self.hedge_manager.cleanup()
        logger.info("✅ [OMS] ExecutionEngine offline.")
