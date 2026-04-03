import time
import hmac
import hashlib
import orjson
import asyncio
from typing import Dict, Any, Tuple
from loguru import logger

class BybitAuthManager:
    """
    Bybit V5 Authentication Provider with Clock Drift compensation.
    Implements Task 14.1/14.2 institutional auth following GEKTOR spec.
    """
    def __init__(self, api_key: str, api_secret: str, recv_window: int = 5000):
        self._api_key = api_key
        self._api_secret = api_secret.encode('utf-8')
        self._recv_window = str(recv_window)
        
        # Time Offset: Server Time - Local Time
        self._time_offset_ms: int = 0
        self._lock = asyncio.Lock()
        self._last_sync = 0.0

    def needs_sync(self) -> bool:
        """Returns True if initial sync is missing or stale (e.g. 1 hour old)."""
        if self._last_sync == 0.0:
            return True
        # Rule 14.1: Refresh drift every 1 hour even without error
        return (time.monotonic() - self._last_sync) > 3600

    async def sync_time(self, rest_client) -> None:
        """
        Synchronizes local clock with Bybit server time.
        Calculates offset accounting for RTT (Round Trip Time).
        """
        try:
            t0 = int(time.time() * 1000)
            # Response from GET /v5/market/time
            response = await rest_client.get_server_time()
            t1 = int(time.time() * 1000)
            
            if not response or 'time' not in response:
                logger.error("❌ [AuthManager] Could not fetch server time.")
                return

            server_time = int(response['time'])
            # Estimated server time when it received the request is mid-flight
            local_mid = (t0 + t1) // 2
            
            async with self._lock:
                self._time_offset_ms = server_time - local_mid
                self._last_sync = time.monotonic()
                
                # Update Global Market State for Freshness Guard (Audit 5.9)
                from core.realtime.market_state import market_state
                market_state.server_time_offset = self._time_offset_ms
                
            logger.info(f"⏱ [AuthManager] Clock Sync: Offset={self._time_offset_ms}ms, RTT={t1 - t0}ms")
        except Exception as e:
            logger.error(f"❌ [AuthManager] Time sync failed: {e}")

    def _get_adjusted_timestamp(self) -> str:
        """Returns unix timestamp in ms adjusted by server offset."""
        return str(int(time.time() * 1000) + self._time_offset_ms)

    def generate_signature(self, payload: str = "") -> Dict[str, str]:
        """
        Generates V5 HMAC SHA256 headers.
        Rule: timestamp + api_key + recv_window + payload
        """
        timestamp = self._get_adjusted_timestamp()
        
        param_str = timestamp + self._api_key + self._recv_window + payload
        
        signature = hmac.new(
            self._api_secret,
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        return {
            "X-BAPI-API-KEY": self._api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self._recv_window,
            "Content-Type": "application/json"
        }

    def sign_post_request(self, params: Dict[str, Any]) -> Tuple[Dict[str, str], str]:
        """Prepares headers and payload for POST requests (e.g. order creation)."""
        payload = orjson.dumps(params).decode('utf-8')
        headers = self.generate_signature(payload)
        return headers, payload
        
    def sign_get_request(self, params: Dict[str, Any] = None) -> Dict[str, str]:
        """Prepares headers for GET requests. Payload is query string."""
        if not params:
            return self.generate_signature("")
        
        # For GET, the "payload" part of signature is the query string
        from urllib.parse import urlencode
        query_string = urlencode(sorted(params.items()))
        return self.generate_signature(query_string)
