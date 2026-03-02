import asyncio
import json
import uuid
import aiohttp
from loguru import logger
from typing import Set, Callable, Dict

class ExponentialBackoff:
    """Exponential backoff with fallback URL support."""
    def __init__(self, base: float = 2, max_delay: float = 300, max_attempts_before_fallback: int = 10):
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

    def on_message(self, callback: Callable):
        self.callbacks.append(callback)

    async def connect(self):
        self._running = True
        self.session = aiohttp.ClientSession()
        backoff = ExponentialBackoff()
        current_url = self.ws_url
        
        while self._running:
            try:
                logger.info(f"Connecting to WS: {current_url}")
                async with self.session.ws_connect(current_url, heartbeat=18.0) as ws:
                    self.ws = ws
                    logger.info("WS Connected.")
                    backoff.reset()
                    current_url = self.ws_url  # Reset to primary on success
                    
                    if self._subscriptions:
                        await self._send_subscribe(list(self._subscriptions))
                    
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            
                            # Handle standard PONG
                            if data.get("op") == "pong":
                                continue
                                
                            for cb in self.callbacks:
                                asyncio.create_task(cb(data))
                                
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.error(f"WS connection lost: {msg.type}")
                            break
                            
            except asyncio.CancelledError:
                logger.info("WS connection task cancelled.")
                self._running = False
                break
            except Exception as e:
                logger.error(f"WS connection error: {e}")
                
            if self._running:
                if backoff.should_try_fallback():
                    current_url = self.ws_url_fallback
                delay = backoff.next_delay()
                logger.info(f"Reconnecting in {delay:.0f}s (attempt #{backoff.attempt})...")
                await asyncio.sleep(delay)

    async def _send_subscribe(self, topics: list[str]):
        if not self.ws or self.ws.closed:
            return
            
        req_id = str(uuid.uuid4())
        payload = {
            "req_id": req_id,
            "op": "subscribe",
            "args": topics
        }
        await self.ws.send_json(payload)
        logger.info(f"Subscribed to {len(topics)} topics. ID: {req_id}")

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

    async def update_subscriptions(self, symbols: list[str]):
        """Smart update: unsubscribe old, subscribe new."""
        desired_topics = set()
        for sym in symbols:
            desired_topics.add(f"kline.60.{sym}")
            desired_topics.add(f"kline.15.{sym}")
            desired_topics.add(f"kline.5.{sym}")
            desired_topics.add(f"tickers.{sym}")
            desired_topics.add(f"liquidation.{sym}")

        to_add = desired_topics - self._subscriptions
        to_remove = self._subscriptions - desired_topics

        if to_remove:
            await self._send_unsubscribe(list(to_remove))
        if to_add:
            await self._send_subscribe(list(to_add))
            
        self._subscriptions = desired_topics

    async def disconnect(self):
        self._running = False
        if self.ws and not self.ws.closed:
            await self.ws.close()
        if self.session and not self.session.closed:
            await self.session.close()
