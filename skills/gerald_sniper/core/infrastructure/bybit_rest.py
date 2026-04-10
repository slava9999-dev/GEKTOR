# skills/gerald-sniper/core/infrastructure/bybit_rest.py
import aiohttp
import asyncio
import time
from typing import Optional, List, Dict
from loguru import logger

class BybitRestClient:
    """
    [GEKTOR v21.58] Institutional REST Adapter with Professional Proxy Pool.
    """
    def __init__(self, proxy_provider=None):
        self.base_url = "https://api.bybit.com"
        self.proxy_provider = proxy_provider
        self.current_proxy: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                trust_env=True,
                timeout=aiohttp.ClientTimeout(total=5.0) # Faster timeout for HFT backfill
            )
        return self._session

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> List[Dict]:
        """Fetches recent trades with automatic proxy reporting on failure."""
        session = await self._get_session()
        url = f"{self.base_url}/v5/market/recent-trade"
        params = {"category": "linear", "symbol": symbol, "limit": limit}
        
        self.current_proxy = self.proxy_provider.get_best_node() if self.proxy_provider else None
        
        try:
            async with session.get(url, params=params, proxy=self.current_proxy) as resp:
                if resp.status == 429:
                    logger.warning(f"⚠️ [REST] Rate limit on {self.current_proxy}. Reporting toxic Node...")
                    if self.proxy_provider: 
                        self.proxy_provider.report_failure(self.current_proxy)
                    return []
                
                data = await resp.json()
                if data.get("retCode") == 0:
                    return sorted(data["result"]["list"], key=lambda x: int(x["time"]))
                return []
        except Exception as e:
            logger.error(f"💥 [REST] Connection failed on {self.current_proxy}: {e}")
            if self.proxy_provider and self.current_proxy: 
                self.proxy_provider.report_failure(self.current_proxy)
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
