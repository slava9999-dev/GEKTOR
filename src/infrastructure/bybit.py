import asyncio
import aiohttp
import json
import time
from loguru import logger
from typing import List, Callable, Optional, Dict
from src.domain.math_core import MarketTick

class BybitRestClient:
    """[GEKTOR v2.0] Lightweight REST client for Vanguard Hydration."""
    def __init__(self, proxy_url: Optional[str] = None):
        self.proxy_url = proxy_url
        self.base_url = "https://api.bybit.com"

    async def get_tickers(self) -> List[dict]:
        """Fetches 24h tickers for liquidity scanning."""
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                url = f"{self.base_url}/v5/market/tickers?category=linear"
                async with session.get(url, proxy=self.proxy_url, timeout=10) as resp:
                    data = await resp.json()
                    return data.get("result", {}).get("list", [])
        except Exception as e:
            logger.error(f"⚠️ [BybitREST] Ticker fetch failed: {repr(e)}")
            return []

    async def get_recent_trades(self, symbol: str, limit: int = 1000) -> List[dict]:
        """Fetches historical trades to seed VPIN buffers (Cold Start Solution)."""
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                url = f"{self.base_url}/v5/market/recent-trade?category=linear&symbol={symbol}&limit={limit}"
                async with session.get(url, proxy=self.proxy_url, timeout=10) as resp:
                    data = await resp.json()
                    return data.get("result", {}).get("list", [])
        except Exception as e:
            logger.error(f"⚠️ [BybitREST] Trade fetch failed ({symbol}): {repr(e)}")
            return []

class NetworkLagMonitor:
    """[GEKTOR v2.0] MONOTONIC Integrity check (Clock Drift Protection)."""
    __slots__ = ['max_allowed_lag_ms', '_start_monotonic', '_start_wall']
    def __init__(self, max_allowed_lag_ms: int = 5000):
        self.max_allowed_lag_ms = max_allowed_lag_ms
        self._start_monotonic = time.monotonic()
        self._start_wall = time.time() * 1000

    def check_exchange_lag(self, exchange_ts: int) -> bool:
        elapsed_ms = (time.monotonic() - self._start_monotonic) * 1000
        current_monotonic_wall = self._start_wall + elapsed_ms
        lag_ms = current_monotonic_wall - exchange_ts
        if lag_ms > self.max_allowed_lag_ms:
             logger.debug(f"⚠️ [Network] LAG Detected: {lag_ms:.1f}ms. Drift ARMED.")
             return False
        return True

class BybitIngestor:
    """[GEKTOR v2.0] High-Availability Ingestor with Schema Drift Watchdog."""
    def __init__(self, symbols: List[str], on_tick_callback: Callable, on_snapshot_callback: Callable, alert_callback: Optional[Callable] = None):
        self.symbols = symbols
        self.on_tick = on_tick_callback
        self.on_snapshot = on_snapshot_callback
        self.alert_callback = alert_callback
        self._running = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.lag_monitor = NetworkLagMonitor()
        self._last_symbol_tick: dict[str, float] = {s: time.monotonic() for s in symbols}
        self._last_snapshot_wall: dict[str, float] = {s: 0.0 for s in symbols} # NEW: Throttling
        self.observed_count = 0
        self.parsed_count = 0
        self.lag_dropped_count = 0
        self.last_metrics_reset = time.time()

    def parse_bybit_trade(self, raw_data: dict) -> Optional[MarketTick]:
        self.last_tick_wall = time.monotonic()
        try:
            symbol = raw_data.get('s') or raw_data.get('symbol', 'UNKNOWN')
            if symbol in self._last_symbol_tick:
                self._last_symbol_tick[symbol] = self.last_tick_wall
            timestamp = raw_data.get('T') or raw_data.get('ts') or raw_data.get('timestamp')
            price = raw_data.get('p') or raw_data.get('price')
            volume = raw_data.get('v') or raw_data.get('volume') or raw_data.get('qty')
            side = raw_data.get('S') or raw_data.get('side', 'Buy')
            if not all([symbol, timestamp, price, volume]): return None
            return MarketTick(symbol=symbol, timestamp=int(timestamp), price=float(price), volume=float(volume), side=side)
        except Exception: return None

    async def _watchdog_loop(self):
        while self._running:
            await asyncio.sleep(20)
            now = time.monotonic()
            for symbol in self.symbols:
                stale_sec = now - self._last_symbol_tick.get(symbol, now)
                if stale_sec > 60:
                    logger.critical(f"🚨 [Ingestor] PARTIAL BLINDNESS: {symbol} is silent for {stale_sec:.1f}s.")
                    if self.alert_callback:
                        res = self.alert_callback(f"🚨 <b>[PARTIAL BLINDNESS]</b> Инструмент <code>{symbol}</code> застыл. Проверь статус биржи.")
                        if asyncio.iscoroutine(res): await res
                    self._last_symbol_tick[symbol] = now 
            total_stale = now - self.last_tick_wall
            if total_stale > 90:
                logger.critical(f"💀 [Ingestor] TOTAL FLATLINE for {total_stale:.1f}s.")
                if self.alert_callback:
                    res = self.alert_callback(f"💀 <b>[TOTAL FLATLINE] БИРЖА ПОТЕРЯНА.</b>")
                    if asyncio.iscoroutine(res): await res
            if self.observed_count > 100:
                parse_rate = self.parsed_count / self.observed_count
                if parse_rate < 0.5:
                    reason = "LAG" if self.lag_dropped_count > self.parsed_count else "SCHEMA_DRIFT"
                    if self.alert_callback:
                        res = self.alert_callback(f"🚨 <b>[SYSTEM BLINDNESS]</b> Сбой парсинга: {1-parse_rate:.1%}. {reason}")
                        if asyncio.iscoroutine(res): await res
            self.observed_count = 0
            self.parsed_count = 0
            self.lag_dropped_count = 0

    async def subscribe(self, symbols: List[str]):
        if not self._ws: return
        args = []
        for s in symbols:
            args.extend([f"publicTrade.{s}", f"orderbook.1.{s}"])
            if s not in self._last_symbol_tick:
                self._last_symbol_tick[s] = time.monotonic()
        await self._ws.send_json({"op": "subscribe", "args": args})

    async def unsubscribe(self, symbols: List[str]):
        if not self._ws: return
        args = []
        for s in symbols:
            args.extend([f"publicTrade.{s}", f"orderbook.1.{s}"])
            if s in self._last_symbol_tick: del self._last_symbol_tick[s]
        await self._ws.send_json({"op": "unsubscribe", "args": args})

    async def run(self):
        self._running = True
        asyncio.create_task(self._watchdog_loop())
        url = "wss://stream.bybit.com/v5/public/linear"
        while self._running:
            session = None
            try:
                session = aiohttp.ClientSession(trust_env=True)
                self._ws = await session.ws_connect(url, heartbeat=20.0)
                await self.subscribe(self.symbols)
                logger.success(f"🟢 [Bybit] Connected (Watchdog ARMED).")
                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                            topic = payload.get("topic", "")
                            if "publicTrade" in topic:
                                for trade_data in payload["data"]:
                                    self.observed_count += 1
                                    tick = self.parse_bybit_trade(trade_data)
                                    if not tick: continue
                                    if self.lag_monitor.check_exchange_lag(tick.timestamp):
                                        self.parsed_count += 1
                                        await self.on_tick(tick.symbol, {"symbol": tick.symbol, "timestamp": tick.timestamp, "price": tick.price, "volume": tick.volume, "side": tick.side})
                                    else: self.lag_dropped_count += 1
                            elif "orderbook" in topic:
                                data = payload["data"]
                                symbol = payload.get("symbol") or topic.split(".")[-1]
                                
                                # [THROTTLING] Быстрый выход, если мы уже обрабатывали этот стакан менее 500мс назад.
                                # Это защищает Event Loop от микро-шума (Нарушение правила 12 Манифеста).
                                now_wall = time.monotonic()
                                if now_wall - self._last_snapshot_wall.get(symbol, 0) < 0.5:
                                    continue
                                self._last_snapshot_wall[symbol] = now_wall

                                if data.get("b") and data.get("a"):
                                    await self.on_snapshot(symbol, {
                                        "bid_p": float(data["b"][0][0]),
                                        "bid_v": float(data["b"][0][1]),
                                        "ask_p": float(data["a"][0][0]),
                                        "ask_v": float(data["a"][0][1]),
                                        "ts": payload["ts"]
                                    })
                        except Exception as e: logger.error(f"💥 [Bybit] Dispatch error: {e}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
            except Exception as e:
                if self._running: await asyncio.sleep(5)
            finally:
                if session: await session.close()

    async def stop(self):
        self._running = False
        if self._ws: await self._ws.close()
