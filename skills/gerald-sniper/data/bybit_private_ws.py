import hmac
import hashlib
import time
import orjson
import asyncio
from typing import Dict, Any, Optional
from loguru import logger
import websockets

class PrivateWSWorker:
    """
    Bybit V5 Private WebSocket Worker (Audit 14.1).
    Tracks 'execution' and 'order' streams for sub-millisecond status updates.
    Implements Zombie Detection (Heartbeat) and Auto-cleanup (Leak protection).
    """
    def __init__(self, api_key: str, api_secret: str, url: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.url = url
        
        # Registry for pending order confirmations (orderLinkId -> Future)
        self._pending_orders: Dict[str, asyncio.Future] = {}
        self._is_healthy = False
        self._last_pong = 0.0
        self._stop_event = asyncio.Event()

    def is_healthy(self) -> bool:
        # Rule: Connection is zombie if no PONG for > 30s
        if not self._is_healthy: return False
        return (time.monotonic() - self._last_pong) < 30

    def register_order(self, order_link_id: str, timeout: float = 30.0) -> asyncio.Future:
        """
        Registers Future with guaranteed memory cleanup (Audit 14.1).
        """
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_orders[order_link_id] = fut
        
        # Prevent Memory Leak: auto-cleanup if WS never sends the event
        loop.call_later(timeout, self._cleanup_future, order_link_id)
        return fut

    def _cleanup_future(self, order_link_id: str):
        fut = self._pending_orders.pop(order_link_id, None)
        if fut and not fut.done():
            fut.set_exception(asyncio.TimeoutError("WS_CONFIRMATION_TIMED_OUT"))

    async def _heartbeat(self, ws):
        """Active Application-level PING/PONG (Zombie Detection)."""
        while self._is_healthy:
            try:
                await ws.send(orjson.dumps({"op": "ping"}).decode('utf-8'))
                await asyncio.sleep(20) # Bybit Spec: 20s
            except:
                self._is_healthy = False
                break

    async def run(self):
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self.url) as ws:
                    # 1. Auth & Sub
                    await ws.send(self._generate_auth_payload())
                    sub_msg = orjson.dumps({"op": "subscribe", "args": ["execution", "order"]}).decode('utf-8')
                    await ws.send(sub_msg)
                    
                    self._is_healthy = True
                    self._last_pong = time.monotonic()
                    
                    # Start Heartbeat
                    asyncio.create_task(self._heartbeat(ws))
                    
                    async for message in ws:
                        data = orjson.loads(message)
                        
                        # Handle PONG
                        if data.get("op") == "pong" or "pong" in str(data):
                            self._last_pong = time.monotonic()
                            continue

                        topic = data.get("topic")
                        if topic in ("execution", "order"):
                            events = data.get("data", [])
                            for event in events:
                                ol_id = event.get("orderLinkId")
                                if ol_id in self._pending_orders:
                                    fut = self._pending_orders.pop(ol_id, None)
                                    if fut and not fut.done():
                                        fut.set_result(self._parse_ws_event(event))

            except Exception as e:
                self._is_healthy = False
                logger.error(f"❌ [PrivateWS] Connection zombie or dead: {e}. Retry in 5s...")
                await asyncio.sleep(5)

    def _parse_ws_event(self, event: Dict) -> Dict:
        """Converts raw WS event to normalized status dict."""
        status = event.get("orderStatus") or event.get("execType")
        return {
            "status": status,
            "order_id": event.get("orderId"),
            "filled_qty": float(event.get("cumExecQty", 0)),
            "avg_price": float(event.get("avgPrice", 0))
        }
