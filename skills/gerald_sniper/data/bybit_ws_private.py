# data/bybit_ws_private.py

import asyncio
import os
import json
import hmac
import hashlib
import time
import aiohttp
from loguru import logger
from typing import Optional
from core.events.events import OrderUpdateEvent
from core.events.nerve_center import bus
from utils.math_utils import safe_float

class BybitPrivateWSManager:
    """
    [Gektor v5.7] Zero-I/O Private Feed Manager.
    - Subscribes to 'wallet' for high-speed Equity Cache.
    - Subscribes to 'order' for instant Reconciliation.
    - Subscribes to 'position' for local risk check.
    """
    def __init__(self, api_key: str, api_secret: str, ws_url: str = "wss://stream.bybit.com/v5/private"):
        self.api_key = api_key
        self.api_secret = api_secret.encode('utf-8')
        self.ws_url = ws_url
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws = None
        self._running = False
        self._auth_ok = False

    async def connect(self):
        self._running = True
        self.session = aiohttp.ClientSession()
        proxy = os.getenv("PROXY_OMS")
        
        while self._running:
            try:
                logger.info(f"🔑 [PrivateWS] Connecting to {self.ws_url}...")
                async with self.session.ws_connect(self.ws_url, heartbeat=20.0, proxy=proxy) as ws:
                    self.ws = ws
                    
                    # 1. Authenticate
                    if await self._authenticate():
                        logger.info("✅ [PrivateWS] Authentication SUCCESS.")
                        # 2. Subscribe
                        await self._subscribe(["wallet", "order", "position"])
                        
                        # 3. Handle Messages
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_message(json.loads(msg.data))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    else:
                        logger.error("❌ [PrivateWS] Authentication FAILED.")
                        await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [PrivateWS] Connection error: {e}")
                await asyncio.sleep(5)

    async def _authenticate(self) -> bool:
        """Bybit V5 Private Auth signature generation."""
        expires = int((time.time() + 10) * 1000)
        param_str = f"GET/realtime{expires}"
        signature = hmac.new(self.api_secret, param_str.encode('utf-8'), hashlib.sha256).hexdigest()
        
        payload = {
            "op": "auth",
            "args": [self.api_key, expires, signature]
        }
        await self.ws.send_json(payload)
        
        # Wait for auth response
        resp = await self.ws.receive_json()
        self._auth_ok = resp.get("success", False)
        return self._auth_ok

    async def _subscribe(self, topics: list):
        payload = {"op": "subscribe", "args": topics}
        await self.ws.send_json(payload)
        logger.info(f"📡 [PrivateWS] Subscribed to {topics}")

    async def _handle_message(self, data: dict):
        topic = data.get("topic")
        if not topic: return

        if topic == "wallet":
            # [ZERO-I/O] Update RiskGuard Equity instantly
            await self._handle_wallet(data.get("data", []))
        elif topic == "order":
            # [RECON] Instant Order Status Update
            await self._handle_order(data.get("data", []))
        elif topic == "position":
            # [STATE] Update in-memory positions
            pass

    async def _handle_wallet(self, info: list):
        if not info: return
        account_data = info[0]
        # We only care about USDT balance in Unified account
        for coin_data in account_data.get("coin", []):
            if coin_data.get("coin") == "USDT":
                equity = float(coin_data.get("walletBalance", 0.0))
                
                # Update RiskGuard Cache (No Lock required for this property)
                from core.shield.risk_engine import risk_guard
                if risk_guard:
                    risk_guard.update_equity_from_ws(equity)

    async def _handle_order(self, orders: list):
        """
        Translates raw Bybit orders to OrderUpdateEvent for the OMS.
        This provides 'Instant Reconciliation' faster than REST.
        """
        for o in orders:
            try:
                # Map Bybit v5 order status to OMS internal model
                # Field: orderStatus: New, Filled, PartiallyFilled, Cancelled, Rejected
                status = o.get("orderStatus")
                filled_qty = float(o.get("cumExecQty", 0))
                avg_price = float(o.get("avgPrice", 0))
                
                # Publish event to bus for ExecutionEngine to pick up
                bus.publish(OrderUpdateEvent(
                    symbol=o.get("symbol"),
                    order_id=o.get("orderId"),
                    order_link_id=o.get("orderLinkId"),
                    status=status,
                    filled_qty=filled_qty,
                    avg_price=avg_price,
                    # Piggyback wallet balance if available for O(1) RiskGuard update
                    wallet_balance=None # Wallet topic is separate, but we could cross-link
                ))
            except Exception as e:
                logger.error(f"❌ [PrivateWS] Order parse error: {e}")

    async def disconnect(self):
        self._running = False
        if self.ws: await self.ws.close()
        if self.session: await self.session.close()
