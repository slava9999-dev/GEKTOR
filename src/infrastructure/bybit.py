# src/infrastructure/bybit.py
import asyncio
import aiohttp
import json
import time
from loguru import logger
from typing import List, Callable, Optional
from src.domain.math_core import MarketTick

class NetworkLagMonitor:
    """[GEKTOR v2.0] Integrity check for transoceanic clock drift."""
    __slots__ = ['max_allowed_lag_ms']
    def __init__(self, max_allowed_lag_ms: int = 2000): # Increased to 2.0s temporarily for setup
        self.max_allowed_lag_ms = max_allowed_lag_ms

    def check_exchange_lag(self, exchange_ts: int) -> bool:
        local_ts = int(time.time() * 1000)
        lag_ms = local_ts - exchange_ts
        
        # We check ABS drift. If local time is wildly off, we must block.
        if abs(lag_ms) > self.max_allowed_lag_ms:
             logger.debug(f"⚠️ [Network] Excessive Drift: {lag_ms}ms. Check NTP!")
             return False
        return True

class BybitIngestor:
    """[GEKTOR v2.0] High-Availability Ingestor with Schema Drift Watchdog."""
    def __init__(self, symbols: List[str], on_tick_callback: Callable, alert_callback: Optional[Callable] = None):
        self.symbols = symbols
        self.on_tick = on_tick_callback
        self.alert_callback = alert_callback
        self._running = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        
        self.lag_monitor = NetworkLagMonitor()
        
        # Schema Drift Metrics
        self.observed_count = 0
        self.parsed_count = 0
        self.lag_dropped_count = 0
        self.last_metrics_reset = time.time()

    def parse_bybit_trade(self, raw_data: dict) -> Optional[MarketTick]:
        try:
            return MarketTick(
                symbol=raw_data.get('s', 'UNKNOWN'),
                timestamp=int(raw_data['T']),
                price=float(raw_data['p']),
                volume=float(raw_data['v']),
                side='B' if raw_data.get('S') == 'Buy' else 'S'
            )
        except (KeyError, ValueError, TypeError) as e:
            return None

    async def _watchdog_loop(self):
        while self._running:
            await asyncio.sleep(60)
            if self.observed_count > 20:
                parse_rate = self.parsed_count / self.observed_count
                if parse_rate < 0.2:
                    reason = "LAG" if self.lag_dropped_count > self.parsed_count else "SCHEMA"
                    logger.critical(f"🚨 [Ingestor] DEGRADATION! Rate: {parse_rate:.1%} | Reason: {reason}")
                    if self.alert_callback:
                        # Ensure string message
                        await self.alert_callback(f"🚨 [DEGRADATION] ACL отбрасывает {1-parse_rate:.1%} трафика. Причина: {reason}")
            
            # Reset
            self.observed_count = 0
            self.parsed_count = 0
            self.lag_dropped_count = 0

    async def run(self):
        self._running = True
        asyncio.create_task(self._watchdog_loop())
        url = "wss://stream.bybit.com/v5/public/linear"
        
        while self._running:
            session = None
            try:
                session = aiohttp.ClientSession(trust_env=True)
                self._ws = await session.ws_connect(url, heartbeat=20.0)
                await self._ws.send_json({"op": "subscribe", "args": [f"publicTrade.{s}" for s in self.symbols]})
                
                logger.success(f"🟢 [Bybit] Connected (Watchdog ARMED).")
                
                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                            if "data" in payload:
                                for trade_data in payload["data"]:
                                    self.observed_count += 1
                                    tick = self.parse_bybit_trade(trade_data)
                                    if not tick: continue
                                    
                                    if self.lag_monitor.check_exchange_lag(tick.timestamp):
                                        self.parsed_count += 1
                                        await self.on_tick(tick.symbol, {
                                            "symbol": tick.symbol, "timestamp": tick.timestamp,
                                            "price": tick.price, "volume": tick.volume, "side": tick.side
                                        })
                                    else:
                                        self.lag_dropped_count += 1
                        except Exception as e:
                            logger.error(f"💥 [Ingestor] Loop crash: {e}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): break
            except Exception as e:
                if self._running: await asyncio.sleep(5)
            finally:
                if session: await session.close()

    async def stop(self):
        self._running = False
        if self._ws: await self._ws.close()
