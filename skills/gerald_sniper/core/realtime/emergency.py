import asyncio
import aiohttp
import time
import hmac
import hashlib
from loguru import logger
from typing import Optional

class EmergencyBypass:
    """
    [GEKTOR v21.60] Emergency Bypass Gateway.
    Provides direct, non-proxy access to Bybit API for mandatory position protection
    when the primary proxy infrastructure is untenable.
    """
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"

    def _generate_signature(self, params: dict, timestamp: str) -> str:
        param_str = str(timestamp) + self.api_key + "5000" + "".join(f"{k}{v}" for k, v in sorted(params.items()))
        hash = hmac.new(self.api_secret.encode("utf-8"), param_str.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()

    async def secure_positions(self, symbol: Optional[str] = None):
        """
        [GEKTOR v21.60] Last Resort: Atomic order cancellation via Direct Bypass.
        Bypasses all proxies to ensure signals reach the exchange.
        """
        logger.warning(f"🚨 [EMERGENCY] ACTIVATING DIRECT BYPASS for {symbol or 'ALL SYMBOLS'}")
        
        timestamp = str(int(time.time() * 1000))
        params = {"category": "linear"}
        if symbol:
            params["symbol"] = symbol

        signature = self._generate_signature(params, timestamp)
        
        headers = {
            "X-BAPID-API-KEY": self.api_key,
            "X-BAPID-SIGN": signature,
            "X-BAPID-TIMESTAMP": timestamp,
            "X-BAPID-RECV-WINDOW": "5000",
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            try:
                # Direct POST to cancel all orders
                async with session.post(
                    f"{self.base_url}/v5/order/cancel-all", 
                    json=params, 
                    headers=headers,
                    timeout=5.0
                ) as resp:
                    data = await resp.json()
                    ret_code = data.get("retCode")
                    
                    if ret_code == 0:
                        logger.success("✅ [EMERGENCY] Orders successfully cancelled via DIRECT BYPASS.")
                    elif ret_code == 10001: # Already cancelled or not found
                        logger.info("ℹ️ [EMERGENCY] Bypass returned 404/Success (No active orders to cancel).")
                    else:
                        logger.error(f"❌ [EMERGENCY] Bypass FAILED. Code: {ret_code}, Msg: {data.get('retMsg')}")
                        
            except Exception as e:
                logger.critical(f"💥 [EMERGENCY] TOTAL NETWORK BLACKOUT. Direct connection failed: {e}")
                # [GEKTOR] At this point, only manual intervention or local persistence saves the state.
