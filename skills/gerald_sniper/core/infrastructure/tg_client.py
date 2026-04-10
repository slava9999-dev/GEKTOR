# core/infrastructure/tg_client.py
import asyncio
import logging
import aiohttp
from aiohttp_socks import ProxyConnector
from typing import Optional
from loguru import logger

class ProxyAwareTelegramClient:
    """
    [GEKTOR v21.10] Resilient Telegram API Client.
    Bypasses regional DPI/IP blocks using SOCKS5 proxy layering.
    Isolates alert traffic from critical market data streams.
    """
    def __init__(self, token: str, proxy_url: Optional[str] = None):
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.proxy_url = proxy_url
        self._session: Optional[aiohttp.ClientSession] = None

    async def get_session(self) -> aiohttp.ClientSession:
        """Lazy-init session with SOCKS5 support."""
        if self._session is None or self._session.closed:
            connector = ProxyConnector.from_url(self.proxy_url) if self.proxy_url else None
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def send_message(self, chat_id: str, text: str) -> bool:
        """
        Hard-timeout delivery (3.0s limit). 
        Failure here triggers retry-loop in OutboxRelay.
        """
        session = await self.get_session()
        payload = {
            "chat_id": chat_id, 
            "text": text, 
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        
        try:
            # 3 second circuit-breaker for proxy response
            async with session.post(self.url, json=payload, timeout=3.0) as resp:
                if resp.status == 200:
                    return True
                
                err_text = await resp.text()
                if resp.status == 429:
                    logger.warning(f"📡 [TG] Rate Limited (429). {err_text}")
                else:
                    logger.error(f"📡 [TG] Error {resp.status}: {err_text}")
                return False
                
        except asyncio.TimeoutError:
            logger.error(f"📡 [TG] Proxy Timeout! Connection to api.telegram.org failed via {self.proxy_url}")
            return False
        except Exception as e:
            logger.error(f"📡 [TG] Network Failure: {e}")
            return False
