import asyncio
import orjson
import websockets
from loguru import logger
from typing import List, Set, Optional
from core.events.events import RawWSEvent
from core.events.nerve_center import bus

class BinanceWSManager:
    """
    Binance L2 Depth Stream for SOR Lead-Lag Analysis and GHOST_MODE Simulation.
    Streams @depth20@100ms for high-frequency updates.
    """
    def __init__(self, symbols: List[str]):
        self.symbols = [s.lower() for s in symbols]
        self._running = False
        self._ws = None
        # Lead-Lag V2: Use Zero-Latency bookTicker (BBA) and aggTrade (CVD)
        streams = "/".join([f"{s}@bookTicker/{s}@aggTrade" for s in self.symbols])
        self.url = f"wss://fstream.binance.com/stream?streams={streams}"

    async def run(self):
        self._running = True
        while self._running:
            try:
                # Audit 16.7: HFT connection params
                async with websockets.connect(self.url, ping_interval=10, ping_timeout=5) as ws:
                    self._ws = ws
                    logger.info(f"🔌 [Binance HFT] Connected to {self.url}")
                    async for msg in ws:
                        payload = orjson.loads(msg)
                        if "data" in payload:
                            await self._handle_hft_event(payload["data"], payload["stream"])
            except Exception as e:
                logger.error(f"❌ [Binance HFT] Error: {e}. Reconnecting...")
                await asyncio.sleep(1)

    async def _handle_hft_event(self, data: dict, stream: str):
        from core.realtime.market_state import market_state
        symbol = data.get("s", "").upper()
        
        if "@bookTicker" in stream:
            # { "u":400900217, "s":"BTCUSDT", "b":"20000.5", "B":"1.5", "a":"20000.6", "A":"2.1" }
            # bookTicker has Update ID 'u'
            update_id = int(data.get("u", 0))
            ts_ms = int(time.time() * 1000) # bookTicker lacks 'E'/'T' (!!)
            bid = float(data.get("b", 0))
            ask = float(data.get("a", 0))
            market_state.update_bba(symbol, bid, ask, ts_ms, update_id=update_id, exchange="binance")
            
        elif "@aggTrade" in stream:
            # { "e":"aggTrade", "E":12345, "s":"BTCUSDT", "p":"20000.5", "q":"0.1", "f":10, "l":12, "T":5678, "m":true }
            ts_ms = data.get("T", int(time.time() * 1000))
            price = float(data.get("p", 0))
            qty = float(data.get("q", 0))
            side = "Sell" if data.get("m") else "Buy" # m=True means buyer is maker -> Sell order
            market_state.update_trade(symbol, price, qty, side, ts_ms, exchange="binance")



    def stop(self):
        self._running = False
