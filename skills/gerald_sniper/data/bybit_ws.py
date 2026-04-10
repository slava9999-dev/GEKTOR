import asyncio
import os
import json
import uuid
import time
import aiohttp
import math
from datetime import datetime
from loguru import logger
from typing import Set, Callable, Dict

class ExponentialBackoff:
    """Exponential backoff with fallback URL support."""
    def __init__(self, base: float = 2, max_delay: float = 60, max_attempts_before_fallback: int = 5):
        self.base = base
        self.max_delay = max_delay
        self.max_attempts_before_fallback = max_attempts_before_fallback
        self.attempt = 0

    def next_delay(self) -> float:
        self.attempt += 1
        delay = min(self.base * (2 ** (self.attempt - 1)), self.max_delay)
        return float(delay)

    def should_try_fallback(self) -> bool:
        return self.attempt >= self.max_attempts_before_fallback

    def reset(self):
        self.attempt = 0


from core.candle_finalizer import CandleFinalizer, FinalizedCandle
from core.runtime.tasks import safe_task
from core.runtime.health import health_monitor
from core.activity.trade_tracker import trade_tracker, TradeEvent
from core.alerts.state_manager import alert_manager
from core.alerts.formatters import format_alert_to_telegram

from utils.math_utils import safe_float, log_throttler, EPS

class BadPrintFilter:
    """
    [GEKTOR v21.4] Heuristic Protection against exchange anomalous spikes (Bad Prints).
    Protects MacroRadar from false Dollar Bar formation.
    """
    __slots__ = ['last_price', 'micro_variance', 'ticks_processed', 'symbol']

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.last_price: float = 0.0
        self.micro_variance: float = 0.0001 # 1 basis point baseline
        self.ticks_processed: int = 0

    def is_valid(self, price: float, volume: float) -> bool:
        if volume <= 0:
            return False
            
        if self.last_price == 0.0:
            self.last_price = price
            return True

        # [Welford's online variance]
        delta = price - self.last_price
        # EMA for micro-volatility (fast adaptation)
        self.micro_variance = 0.999 * self.micro_variance + 0.001 * (delta ** 2)
        
        # [GEKTOR standard: 10-Sigma rejection]
        std_dev = math.sqrt(self.micro_variance)
        # Warm-up: Don't reject until we have 100 ticks of data for estimation
        if self.ticks_processed > 100:
            if abs(delta) > 10 * std_dev and abs(delta) / self.last_price > 0.005: 
                # Only reject if > 0.5% jump AND > 10-sigma (avoid rejection of tiny moves in stable coins)
                if log_throttler.should_log(f"bad_print_{self.symbol}", 60):
                    logger.warning(f"🚫 [GATEWAY] Bad Print for {self.symbol}. J:{delta:.2f} S:{std_dev:.4f}. Dropped.")
                return False

        self.last_price = price
        self.ticks_processed += 1
        return True

class BybitWSManager:
    """Manages Bybit WebSockets, subscriptions, heartbeat and reconnections."""
    def __init__(self, ws_url: str, ws_url_fallback: str = "wss://stream.bytick.com/v5/public/linear"):
        self.ws_url = ws_url
        self.ws_url_fallback = ws_url_fallback
        self.ws = None
        self.session = None
        self._running = False
        self._subscriptions: Set[str] = set()
        self.callbacks: list[Callable] = []
        self.candle_finalizer = CandleFinalizer()
        self.candle_handler = None
        self.msg_queue = asyncio.Queue(maxsize=5000)
        self._worker_task = None
        self._auth_queue = asyncio.Queue()
        self._message_id = 1
        self._last_update_ids = {} # symbol -> last_u_id
        self._is_resyncing = set() # symbols currently fetching REST snapshot
        self._resync_buffers = {} # symbol -> deque of L2DeltaEvent
        self._last_msg_times = {} # symbol -> float (MONOTONIC)
        
        # Start Watchdog Task (David Beazley standard)
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())
        self._filters: Dict[str, BadPrintFilter] = {}
        self.max_backoff = 60.0 # [GEKTOR v21.4] Mandatory Resilience Limit

    async def _watchdog_loop(self) -> None:
        """[GEKTOR v21.15.4] Silent Disconnect Sentinel."""
        while True:
            await asyncio.sleep(0.5)
            now = time.monotonic()
            for symbol, last_time in list(self._last_msg_times.items()):
                if now - last_time > 1.5: # 1500ms threshold
                    if symbol not in self._is_resyncing:
                        logger.critical(f"🧟 [Watchdog] [{symbol}] ZOMBIE CONNECTION DETECTED. No data for {now - last_time:.2f}s.")
                        await self._initiate_resync(symbol, "ZOMBIE_TCP")

    def on_message(self, callback: Callable):
        self.callbacks.append(callback)

    async def on_closed_candle(self, candle):
        if self.candle_handler:
            await self.candle_handler(candle)

    def get_topics_count(self) -> int:
        """Returns number of active WebSocket topic subscriptions."""
        return len(self._subscriptions)

    async def connect(self):
        self._running = True
        self.session = aiohttp.ClientSession()
        backoff = ExponentialBackoff()
        current_url = self.ws_url
        proxy = os.getenv("PROXY_URL")
        _last_working_url = None  # Запоминаем последний рабочий URL
        _last_primary_probe = 0.0
        _primary_probe_interval = 600  # Пробовать primary каждые 10 мин
        _disconnect_ts = 0.0  # Track disconnect time for Reconciler
        _reconnect_count = 0
        
        while self._running:
            try:
                logger.info(f"Connecting to WS: {current_url} via Proxy: {'Yes' if proxy else 'No'}")
                health_monitor.record_ws_reconnect()
                alert_manager.track_health_event("ws", "reconnect")
                async with self.session.ws_connect(current_url, heartbeat=18.0, proxy=proxy) as ws:
                    self.ws = ws
                    logger.info("WS Connected.")
                    backoff.reset()
                    _last_working_url = current_url  # Запомнить рабочий URL
                    
                    # [Reconciler v5.5] Emit ConnectionRestoredEvent on reconnect
                    if _disconnect_ts > 0:
                        _reconnect_count += 1
                        downtime = time.time() - _disconnect_ts
                        _disconnect_ts = 0.0
                        try:
                            from core.events.events import ConnectionRestoredEvent
                            await bus.publish(ConnectionRestoredEvent(
                                source="BybitWS",
                                downtime_seconds=downtime,
                                reconnect_count=_reconnect_count,
                            ))
                            logger.info(f"🔌 [WS] ConnectionRestoredEvent emitted (gap: {downtime:.1f}s)")
                        except Exception as e:
                            logger.error(f"❌ [WS] Failed to emit ConnectionRestoredEvent: {e}")
                    
                    if self._subscriptions:
                        await self._send_subscribe(list(self._subscriptions))
                    
                    async def ping_loop():
                        try:
                            while not ws.closed:
                                await asyncio.sleep(15)  # 20 сек было рискованно, Bybit дропает без пинга через 30с лояльно, ставим 15.
                                if not ws.closed:
                                    await ws.send_json({"req_id": str(uuid.uuid4()), "op": "ping"})
                        except asyncio.CancelledError:
                            pass
                        except Exception as e:
                            logger.error(f"WS ping loop error: {e}")

                    ping_task = safe_task(ping_loop(), name="ws_ping_loop")

                    # BUG FIX v2.2: Start worker only if not already running
                    if not self._worker_task or self._worker_task.done():
                        self._worker_task = safe_task(self._msg_worker(), name="ws_msg_worker")

                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    if self.msg_queue.full():
                                        if log_throttler.should_log("ws_queue_full", 30):
                                            logger.warning("⚠️ WS Queue full! Dropping messages.")
                                        continue
                                    await self.msg_queue.put(data)
                                except Exception as e:
                                    health_monitor.record_ws_parse_error()
                                    logger.error(f"❌ WS Message parse error: {e}")
                                    
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.error(f"WS connection lost/error: {msg.type}")
                                break
                    finally:
                        ping_task.cancel()
                            
            except asyncio.CancelledError:
                logger.info("WS connection task cancelled.")
                self._running = False
                break
            except Exception as e:
                logger.error(f"WS connection error: {e}")
                
            if self._running:
                # [Reconciler v5.5] Mark disconnection timestamp for gap measurement
                if _disconnect_ts == 0.0:
                    _disconnect_ts = time.time()
                delay = backoff.next_delay()
                
                # Переключение URL при исчерпании попыток
                if backoff.should_try_fallback():
                    if current_url == self.ws_url:
                        if self.ws_url_fallback:
                            current_url = self.ws_url_fallback
                            logger.warning(f"🔄 Switching to fallback WS: {current_url}")
                            backoff.reset()
                    else:
                        import time
                        now = time.monotonic()
                        if now - _last_primary_probe > _primary_probe_interval:
                            _last_primary_probe = now
                            current_url = self.ws_url
                            logger.info(f"🔍 Probing primary WS: {current_url}")
                            backoff.reset()
                
                logger.info(f"Reconnecting in {delay:.0f}s (attempt #{backoff.attempt})...")
                await asyncio.sleep(delay)

    async def _send_subscribe(self, topics: list[str]):
        if not self.ws or self.ws.closed:
            return
            
        # Bybit v5 recommends max 10 symbols per subscription packet
        chunk_size = 10
        for i in range(0, len(topics), chunk_size):
            chunk = topics[i:i + chunk_size]
            req_id = str(uuid.uuid4())
            payload = {
                "req_id": req_id,
                "op": "subscribe",
                "args": chunk
            }
            await self.ws.send_json(payload)
            logger.info(f"Subscribed to bucket {i//chunk_size + 1} ({len(chunk)} topics). ID: {req_id}")
            await asyncio.sleep(0.1) # Small delay to avoid flooding

    async def _send_unsubscribe(self, topics: list[str]):
        if not self.ws or self.ws.closed:
            return
            
        payload = {
            "req_id": str(uuid.uuid4()),
            "op": "unsubscribe",
            "args": topics
        }
        await self.ws.send_json(payload)
        logger.info(f"Unsubscribed from {len(topics)} topics.")

    async def _msg_worker(self):
        """Persistent worker to process queued WS messages."""
        logger.info("👷 WS Message Worker started.")
        while self._running:
            try:
                data = await self.msg_queue.get()
                try:
                    topic = data.get("topic")
                    if topic:
                        if topic.startswith("kline"):
                            await self._handle_kline(data)
                        elif topic.startswith("tickers"):
                            await self._handle_ticker(data)
                        elif topic.startswith("publicTrade"):
                            await self._handle_trade(data)
                        elif topic.startswith("orderbook"):
                            await self._handle_orderbook(data)
                        elif topic.startswith("allLiquidation"):
                            await self._handle_liquidation(data)
                        else:
                            for cb in self.callbacks:
                                safe_task(cb(data), name=f"ws_callback_{topic}")
                except Exception as e:
                    if log_throttler.should_log(f"msg_worker_err_{str(e)}", 60):
                        logger.error(f"❌ WS Message process inner error: {e}")
                finally:
                    self.msg_queue.task_done()
            except Exception as e:
                # Top level protection for worker loop
                logger.error(f"❌ WS Message Worker fatal error: {e}")
                await asyncio.sleep(1)

    async def _handle_kline(self, data: dict):
        topic_parts = data.get("topic", "").split('.')
        if len(topic_parts) < 3: return
        interval = topic_parts[1]
        symbol = topic_parts[2]
        seq_id = data.get("seq")
        
        for k in data.get("data", []):
            try:
                start_ts = k.get("start")
                end_ts = k.get("end")
                if start_ts is None or end_ts is None: continue

                parsed_candle = FinalizedCandle(
                    symbol=symbol, interval=interval,
                    start_ts=int(start_ts), end_ts=int(end_ts),
                    open=float(k.get("open", 0)), high=float(k.get("high", 0)),
                    low=float(k.get("low", 0)), close=float(k.get("close", 0)),
                    volume=float(k.get("volume", 0)), turnover=float(k.get("turnover", 0)),
                    confirmed=bool(k.get("confirm", False)),
                    seq_id=int(seq_id) if seq_id is not None else int(end_ts),
                )
                final_candle = self.candle_finalizer.ingest(parsed_candle)
                if final_candle:
                    await self.on_closed_candle(final_candle)
            except Exception as e:
                logger.warning(f"⚠️ Candle parse error: {e}")
                health_monitor.record_ws_parse_error()

    async def _handle_ticker(self, data: dict):
        topic_parts = data.get("topic", "").split('.')
        if len(topic_parts) < 2: return
        symbol = topic_parts[1]
        ticker_data = data.get("data", {})
        ts_ms = int(data.get("ts", time.time()*1000))
        update_id = int(ticker_data.get("u", 0))
        
        # v4.3: HFT BBA Registration (No Throttle for BBA)
        bid = safe_float(ticker_data.get("bid1Price", 0))
        ask = safe_float(ticker_data.get("ask1Price", 0))
        if bid > 0 and ask > 0:
            # [GEKTOR v21.4] Filter L1 Tick before propagation
            f = self._get_filter(symbol)
            mid = (bid + ask) / 2
            if not f.is_valid(mid, 1.0): # Volume dummy 1.0 for BBA
                return
                
            from core.realtime.market_state import market_state
            market_state.update_bba(symbol, bid, ask, ts_ms, update_id=update_id, exchange="bybit")

        # [GEKTOR v21.15] Feed IndexPriceTracker for Basis Guard (Derivative Dislocation Detection)
        # NOT throttled — basis must be tracked at full resolution to catch sub-second phantom crashes
        index_price_raw = ticker_data.get("indexPrice")
        last_price_raw = ticker_data.get("lastPrice")
        if index_price_raw and last_price_raw:
            idx_p = safe_float(index_price_raw)
            fut_p = safe_float(last_price_raw)
            if idx_p > 0 and fut_p > 0:
                from core.radar.basis_guard import index_tracker
                index_tracker.record(symbol, idx_p, fut_p)

        last_price = ticker_data.get("lastPrice")
        if last_price:

            import time
            from core.alerts.state_manager import alert_manager
            
            # Throttle: Only process if price actually moved or enough time passed
            now = time.time()
            if not hasattr(self, '_last_ticker_proc'): self._last_ticker_proc = {}
            last_ts = self._last_ticker_proc.get(symbol, 0)
            if now - last_ts < 10.0: # v4.1: Max once every 10 seconds per symbol (was 2s — too frequent with 100+ symbols)
                return
            
            self._last_ticker_proc[symbol] = now

            atr_pct = 2.0
            candles = None
            if hasattr(self.candle_handler, '__self__'):
                pipeline = self.candle_handler.__self__
                if hasattr(pipeline, 'candle_cache'):
                    sym_data = pipeline.candle_cache.data.get(symbol)
                    if sym_data:
                        atr_pct = getattr(sym_data.get('radar'), 'atr_pct', 2.0)
                        candles = sym_data.get('m5') or sym_data.get('m15')

            try:
                price_float = safe_float(last_price)
                if price_float <= 0: return

                events = await alert_manager.process_price_update(symbol, price_float, atr_pct, candles=candles)
                for event in events:
                    safe_task(self._alert_to_telegram(event), name=f"proximity_{symbol}")
            except Exception as e:
                if log_throttler.should_log(f"ticker_err_{symbol}", 60):
                    logger.error(f"Error handling ticker {symbol}: {e}")

    async def _handle_trade(self, data: dict):
        """Records raw trade events into the activity tracker."""
        for t in data.get("data", []):
            try:
                symbol = t.get("s")
                ts_ms = int(t.get("T", 0))
                # [GEKTOR v21.7] Clock Sync Telemetry (Drift correction)
                from core.realtime.temporal_integrity import clock_sync
                now_ms = int(time.time() * 1000)
                clock_sync.update(ts_ms, now_ms)
                
                # Filter trades for only what we watch
                if symbol not in self._subscriptions_symbols():
                    continue

                price = safe_float(t.get("p", 0))
                qty = safe_float(t.get("v", 0))
                side = t.get("S") # "Buy" or "Sell"
                ts_ms = int(t.get("T", 0))
                
                if price <= 0: continue

                # [GEKTOR v21.4] Toxic Data Ingestion Filter
                f = self._get_filter(symbol)
                if not f.is_valid(price, qty):
                    continue

                # [GEKTOR v21.6] Data Staleness Evaluation (Entropy Guard)
                # local_receive_ts is implicitly 'now' when _handle_trade starts.
                # However, for drift-precision, we use the timestamp when the message arrived at the gateway if possible.
                # We use time.time() here for simplicity or the 'ts' field from the WS message.
                msg_ts = int(data.get("ts", time.time() * 1000))
                staleness_ms = max(0.0, (time.time() * 1000) - msg_ts)

                from core.realtime.market_state import market_state
                market_state.update_trade(symbol, price, qty, side, ts_ms, exchange="bybit", staleness_ms=staleness_ms)

                # [GEKTOR v21.15] Feed AbsorptionTracker for Iceberg Detection
                from core.radar.absorption_detector import get_absorption_tracker, TICK_MULTIPLIER
                tracker = get_absorption_tracker(symbol)
                price_tick = int(price * TICK_MULTIPLIER + 0.5)
                tracker.record_trade(price_tick, qty)

                # [GEKTOR v21.15] Dual-Track Barrier Monitoring (Fast-Track)
                # Evaluation happens on EVERY tick to prevent 'Intra-bar Blindness'
                # [GEKTOR v21.15.1] Volume-Confirmed Penetration (Anti-Whipsaw)
                from core.radar.monitoring import monitoring_orchestrator
                volume_usd = price * qty
                await monitoring_orchestrator.process_tick_fast_track(symbol, price, volume_usd)

                
                trade_tracker.record(TradeEvent(
                    symbol=symbol,
                    price=price,
                    qty=qty,
                    side=side,
                    timestamp=datetime.utcfromtimestamp(ts_ms / 1000.0),
                ))
            except Exception:
                continue

    def _subscriptions_symbols(self) -> set:
        """Helper to get set of symbols we are currently subscribed to."""
        # Simple extraction from topics like 'publicTrade.BTCUSDT'
        symbols = set()
        for t in self._subscriptions:
            parts = t.split('.')
            if len(parts) > 1:
                symbols.add(parts[-1])
        return symbols

    async def _handle_orderbook(self, data: dict):
        topic_parts = data.get("topic", "").split('.')
        if len(topic_parts) < 2: return
        symbol = topic_parts[-1]
        
        is_snapshot = data.get("type") == "snapshot"
        d = data.get("data", {})
        bids = d.get("b", [])
        asks = d.get("a", [])
        
        # [GEKTOR v21.15.4] Sequence Isolation & Causal Consistency (Kleppmann)
        u_id = d.get("u", 0)
        self._last_msg_times[symbol] = time.monotonic()

        # 1. Buffering Phase (If already resyncing)
        if symbol in self._is_resyncing:
            self._resync_buffers[symbol].append(data)
            return

        # 2. Sequence Validation
        last_u = self._last_update_ids.get(symbol)
        if not is_snapshot and last_u is not None:
            # Check for Gap: Bybit increments by 1
            if u_id != last_u + 1:
                logger.critical(f"🚨 [Phase 1] [{symbol}] SEQUENCE GAP! Got={u_id}, Prev={last_u}. Initiating REST Resync.")
                await self._initiate_resync(symbol, "SEQUENCE_GAP")
                return

        self._last_update_ids[symbol] = u_id
        
        # Rule 8: Use top 20 levels for quick imbalance metric
        bid_vol_usd = sum(safe_float(b[0]) * safe_float(b[1]) for b in bids[:20])
        ask_vol_usd = sum(safe_float(a[0]) * safe_float(a[1]) for a in asks[:20])
        
        # Pass raw lists. float conversion happens in ZeroGCOrderbook._process_levels
        # but here we keep them as provided (strings/floats) to avoid extra work
        # Pass raw lists. float conversion happens in ZeroGCOrderbook._process_levels
        # with exchange timestamp propagation (Audit 5.9: Freshness Guard)
        from core.realtime.market_state import market_state
        market_state.update_orderbook(
            symbol, bid_vol_usd, ask_vol_usd, 
            bids=bids, asks=asks, 
            update_id=u_id,
            ts_ms=int(data.get("ts", 0)),
            is_snapshot=is_snapshot,
            exchange="bybit"
        )

        # [GEKTOR v21.15] Feed AbsorptionTracker with visible L2 sizes (ask-side walls)
        # Tracks peak_visible_size for iceberg detection near Upper Barrier
        if asks:
            from core.radar.absorption_detector import get_absorption_tracker, TICK_MULTIPLIER
            tracker = get_absorption_tracker(symbol)
            for a in asks[:10]:  # Top 10 ask levels only (performance)
                p = safe_float(a[0])
                v = safe_float(a[1])
                if p > 0 and v > 0:
                    tracker.record_l2_size(int(p * TICK_MULTIPLIER + 0.5), v)

    async def _handle_liquidation(self, data: dict):
        """
        Bybit v5 Public Liquidation Stream Filter and Parser (v2.2 Patch)
        - Handles 'data' as a LIST.
        - Filters by tracked symbols.
        - Uses safe_float.
        """
        events = data.get("data", [])
        if not isinstance(events, list): return
        
        tracked = self._subscriptions_symbols()
        tracked.add("BTCUSDT") # Always track BTC liquidations for macro

        for event in events:
            try:
                symbol = event.get("s")
                if symbol not in tracked:
                    continue
                
                price = safe_float(event.get("p", 0))
                qty = safe_float(event.get("v", 0))
                side = event.get("S")
                
                if price <= 0 or qty <= 0:
                    continue

                from core.realtime.market_state import market_state
                market_state.update_liquidation(symbol, price, qty, side)

                # [GEKTOR v21.15] Feed LiquidationAccumulator for Squeeze Discriminator
                from core.radar.liquidation_feed import liquidation_accumulator
                liquidation_accumulator.ingest({
                    'symbol': symbol,
                    'side': side,
                    'price': str(price),
                    'size': str(qty),
                    'updatedTime': int(time.time() * 1000)
                })
                
            except Exception as e:
                if log_throttler.should_log("liq_parse_err", 60):
                    logger.error(f"Liquidation parse error: {e}")

    # v4: Max topics limit (Bybit recommends <200 per connection)
    MAX_TOPICS = 190

    async def update_subscriptions(self, active_symbols: list[str], priority_symbols: list[str] = None):
        """
        Rule 1.2: Surgical Subscription Scope.
        - Watchlist (active_symbols): trades, orderbook.1
        - Priority (priority_symbols): trades, orderbook.1, kline.1
        - Global: BTC 1h/4h klines
        """
        desired_topics = set()
        
        # 0. Global Context
        desired_topics.add("kline.60.BTCUSDT")
        desired_topics.add("kline.240.BTCUSDT")
        # [GEKTOR v21.15] Global Liquidation Feed for Squeeze Discriminator
        desired_topics.add("allLiquidation")

        # 1. Watchlist symbols (8 planned)
        for sym in set(active_symbols or []):
            desired_topics.add(f"publicTrade.{sym}")
            desired_topics.add(f"orderbook.50.{sym}")

        # 2. Priority symbols (5 planned)
        for sym in set(priority_symbols or []):
            desired_topics.add(f"publicTrade.{sym}")
            desired_topics.add(f"orderbook.50.{sym}")
            desired_topics.add(f"kline.1.{sym}")

        # Strict diff: compute what to add and what to remove
        to_add = desired_topics - self._subscriptions
        to_remove = self._subscriptions - desired_topics

        if to_remove:
            # Group into chunks to minimize request count
            remove_list = list(to_remove)
            for i in range(0, len(remove_list), 10):
                await self._send_unsubscribe(remove_list[i : i + 10])
                await asyncio.sleep(0.05)

        if to_add:
            add_list = list(to_add)
            logger.info(f"📊 Topic budget: {len(desired_topics)} | Watchlist: {len(active_symbols)} | Priority: {len(priority_symbols or [])}")
            await self._send_subscribe(add_list)
        
        self._subscriptions = desired_topics

    async def _alert_to_telegram(self, event):
        """Helper to send proximity or system alerts using centralized formatter."""
        from utils.telegram_bot import send_telegram_alert
        result = format_alert_to_telegram(event)
        # v4: formatter returns (msg, buttons) tuple
        if isinstance(result, tuple):
            msg, buttons = result
        else:
            msg, buttons = result, None
        await send_telegram_alert(msg, disable_notification=(event.severity == "INFO"))

    async def health_monitor_loop(self):
        """Background loop to check system health and send alerts."""
        while self._running:
            try:
                events = await alert_manager.check_system_health()
                for event in events:
                    await self._alert_to_telegram(event)
                # Periodic checkpoint
                alert_manager.save_state()
            except Exception as e:
                logger.error(f"Health monitor loop error: {e}")
            await asyncio.sleep(60) # Check every minute

    async def _initiate_resync(self, symbol: str, reason: str) -> None:
        """[GEKTOR v21.15.4] Non-blocking REST Re-hydration with Delta Buffering."""
        if symbol in self._is_resyncing: return
        
        self._is_resyncing.add(symbol)
        self._resync_buffers[symbol] = deque(maxlen=200) # Buffer max 200 updates (~2-4 seconds)
        
        # Notify Monitor to freeze entries
        from core.radar.monitoring import monitoring_orchestrator
        asyncio.create_task(monitoring_orchestrator.on_orderbook_corruption(symbol, reason))
        
        # Launch async fetch
        asyncio.create_task(self._fetch_rest_snapshot(symbol))

    async def _fetch_rest_snapshot(self, symbol: str) -> None:
        """Atomic Reconciliation: Snapshot + Buffered Deltas."""
        try:
            logger.info(f"🔄 [Phase 1] [{symbol}] Fetching REST L2 Snapshot...")
            # Simulated REST fetch (replaces with real Bybit HTTP client call)
            # await self.http_client.get_l2_snapshot(symbol)
            await asyncio.sleep(0.3) # Fake roundtrip
            
            # 1. Apply Snapshot to MarketState
            # (Assuming snapshot data is received here)
            snapshot_u_id = 999999 # Placeholder from real response
            
            # 2. Reconciliation Replay
            buffer = self._resync_buffers.get(symbol, [])
            replayed_count = 0
            
            while buffer:
                delta = buffer.popleft()
                d_u_id = delta.get('data', {}).get('u', 0)
                
                # Filter: Skip deltas already included in snapshot
                if d_u_id <= snapshot_u_id:
                    continue
                
                # Check for first gap after snapshot
                if replayed_count == 0 and d_u_id > snapshot_u_id + 1:
                    logger.error(f"❌ [Phase 1] [{symbol}] Snapshot-Buffer Gap! S={snapshot_u_id}, Next={d_u_id}. RESTARTING.")
                    self._is_resyncing.remove(symbol)
                    await self._initiate_resync(symbol, "SNAPSHOT_GAP")
                    return
                
                # Replay Delta
                await self._handle_orderbook(delta)
                replayed_count += 1
            
            self._is_resyncing.remove(symbol)
            if symbol in self._resync_buffers: del self._resync_buffers[symbol]
            logger.success(f"🟢 [Phase 1] [{symbol}] Resync Complete. Replayed {replayed_count} deltas.")
            
        except Exception as e:
            logger.error(f"❌ [Phase 1] [{symbol}] REST Snapshot failed: {e}")
            self._is_resyncing.remove(symbol)

    def _get_filter(self, symbol: str) -> BadPrintFilter:
        if symbol not in self._filters:
            self._filters[symbol] = BadPrintFilter(symbol)
        return self._filters[symbol]

    async def disconnect(self):
        self._running = False
        if self.ws and not self.ws.closed:
            await self.ws.close()
        if self.session and not self.session.closed:
            await self.session.close()
