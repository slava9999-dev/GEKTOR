import asyncio
import hashlib
import os
import random
import time
from collections import deque
from dataclasses import dataclass, field
from loguru import logger
from typing import List, Dict, Any, Optional
import aiohttp
import orjson
from utils.math_utils import log_throttler


@dataclass
class EndpointCBState:
    """Per-endpoint circuit breaker state."""
    name: str
    failures: int = 0
    open: bool = False
    open_until: float = 0.0
    cooldown: float = 30.0
    max_cooldown: float = 300.0
    threshold: int = 3

    def record_failure(self, error_msg: str):
        self.failures += 1
        if self.failures >= self.threshold:
            if not self.open:
                self.open = True
                self.cooldown = 30.0 # Initial 30s
            else:
                self.cooldown = min(300.0, self.cooldown * 2.0) # Exponential
                
            self.open_until = time.monotonic() + self.cooldown
            logger.warning(
                f"🔴 CB [{self.name}] OPEN — {self.failures} failures. "
                f"Retry in {self.cooldown:.0f}s. Last: {error_msg[:80]}"
            )

    def record_success(self):
        if self.failures > 0 or self.open:
            logger.info(f"🟢 CB [{self.name}] RECOVERED")
        self.failures = 0
        self.open = False
        self.cooldown = 30.0

    @property
    def is_available(self) -> bool:
        if not self.open:
            return True
        if time.monotonic() >= self.open_until:
            return True  # half-open
        return False


class BybitREST:
    """
    Gerald v5 — Bybit REST API v5 client refactored for aiohttp (Audit 25.1).
    
    Architecture:
    - aiohttp-based high-concurrency engine
    - Per-endpoint circuit breakers
    - Strict Semaphore(15) for Rate Limit protection
    - Sliding window request budget (3000/min)
    - Jittered exponential backoff
    """

    MAX_REQUESTS_PER_MIN = 3000
    SEMAPHORE_LIMIT = 15 # Gektor's Guard: Lower concurrency to avoid 429
    
    # [GEKTOR v8.2] Granular Timeout Matrix — Anti Half-Open Sockets
    # total:        Hard ceiling for the entire operation (request + response read)
    # sock_connect: TCP handshake timeout (detects unreachable proxy/host)
    # sock_read:    Time to wait for next chunk of response data
    #               This is the PRIMARY defense against Half-Open sockets:
    #               if the remote closes its end, our read() hangs forever.
    #               With sock_read=8s, Python raises TimeoutError after 8s of silence.
    # connect:      Total connection setup including SSL handshake
    REQUEST_TIMEOUT_TOTAL = 15  # Hard ceiling (seconds)
    REQUEST_TIMEOUT_SOCK_CONNECT = 5  # TCP SYN → ACK (seconds)
    REQUEST_TIMEOUT_SOCK_READ = 8  # Response chunk silence limit (seconds)
    REQUEST_TIMEOUT_CONNECT = 5  # Connection + SSL setup (seconds)
    
    # Session recycling: Force-close and recreate session every N seconds
    # This proactively kills any TCP connections that may have gone Half-Open
    # due to proxy IP rotation, NAT table flush, or link flaps.
    SESSION_RECYCLE_INTERVAL = 1800  # 30 minutes
    
    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        fallback_url: str = "https://api.bytick.com",
        is_execution: bool = False,
        proxy_url: Optional[str] = None
    ):
        self.primary_url = base_url
        self.fallback_url = fallback_url
        self.is_execution = is_execution
        self.proxy_url = proxy_url
        self.session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

        # Circuit breaker state
        self._endpoint_cbs: Dict[str, EndpointCBState] = {}
        self._circuit_open = False
        self._circuit_open_until = 0.0
        self._consecutive_failures = 0
        self._current_cooldown = 30.0 if not self.is_execution else 5.0
        
        # Concurrency & Budget
        self._rate_semaphore = asyncio.Semaphore(self.SEMAPHORE_LIMIT)
        self._request_timestamps: deque = deque(maxlen=self.MAX_REQUESTS_PER_MIN)
        self._inflight: Dict[str, asyncio.Future] = {}
        self._inflight_lock = asyncio.Lock()
        
        # Fallback switching
        self._using_fallback = False
        self._primary_fail_streak = 0
        self._fallback_switch_threshold = 2 if self.is_execution else 3
        self._last_primary_check = 0.0

    async def _get_session(self) -> aiohttp.ClientSession:
        now = time.monotonic()
        
        # [GEKTOR v8.2] Session Recycling — proactively kill stale TCP pools
        if self.session and not self.session.closed:
            if hasattr(self, '_session_created_at') and (now - self._session_created_at) > self.SESSION_RECYCLE_INTERVAL:
                logger.info("🔄 [TCP] Session recycling — closing stale connection pool.")
                try:
                    await self.session.close()
                except Exception:
                    pass
                self.session = None
        
        if self.session is None or self.session.closed:
            async with self._lock:
                if self.session is None or self.session.closed:
                    # Determine appropriate proxy
                    proxy = self.proxy_url or os.getenv("PROXY_OMS" if self.is_execution else "PROXY_RADAR")
                    
                    # [GEKTOR v8.2] TCP Connector with Keepalive Probes
                    # enable_cleanup_closed: Detect and cleanup closed connections
                    # keepalive_timeout: How long to keep idle connections alive (30s)
                    # force_close=False: Reuse connections (HTTP/1.1 keep-alive)
                    conn = aiohttp.TCPConnector(
                        limit=50, 
                        use_dns_cache=True, 
                        force_close=False,
                        enable_cleanup_closed=True,
                        keepalive_timeout=30,  # Close idle connections after 30s
                        ttl_dns_cache=300,     # Refresh DNS every 5 min (proxy IP rotation)
                    )
                    
                    # [GEKTOR v8.2] Granular timeouts — anti Half-Open socket matrix
                    timeout = aiohttp.ClientTimeout(
                        total=self.REQUEST_TIMEOUT_TOTAL,
                        connect=self.REQUEST_TIMEOUT_CONNECT,
                        sock_connect=self.REQUEST_TIMEOUT_SOCK_CONNECT,
                        sock_read=self.REQUEST_TIMEOUT_SOCK_READ,
                    )
                    
                    self.session = aiohttp.ClientSession(
                        connector=conn, 
                        timeout=timeout,
                        json_serialize=orjson.dumps
                    )
                    self._session_created_at = now
                    if proxy:
                        logger.info(f"🛡️ BybitREST [{'OMS' if self.is_execution else 'Radar'}] initiated via proxy: {proxy}")
        return self.session

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _get_endpoint_key(self, endpoint: str) -> str:
        if "tickers" in endpoint: return "tickers"
        if "kline" in endpoint: return "kline"
        if "recent-trade" in endpoint: return "recent-trade"
        return "other"

    def _get_endpoint_cb(self, endpoint: str) -> EndpointCBState:
        key = self._get_endpoint_key(endpoint)
        if key not in self._endpoint_cbs:
            self._endpoint_cbs[key] = EndpointCBState(name=key)
        return self._endpoint_cbs[key]

    async def _check_budget(self):
        now = time.monotonic()
        while self._request_timestamps and (now - self._request_timestamps[0]) > 60:
            self._request_timestamps.popleft()
        
        if len(self._request_timestamps) >= self.MAX_REQUESTS_PER_MIN:
            wait = 60.1 - (now - self._request_timestamps[0])
            if wait > 0:
                if log_throttler.should_log("rest_budget_hit", 30):
                    logger.warning(f"⏳ Budget hit ({self.MAX_REQUESTS_PER_MIN}/min). Pausing {wait:.1f}s.")
                await asyncio.sleep(wait)
        self._request_timestamps.append(now)

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Dict = None,
        retries: int = 3,
        headers: Dict = None,
    ) -> Dict:
        """Coalesced request with aiohttp (Audit 25.1)."""
        # 1. Coalescing (Zero Duplication)
        coal_key = f"{endpoint}|{sorted(params.items()) if params else ''}"
        async with self._inflight_lock:
            if coal_key in self._inflight:
                return await asyncio.shield(self._inflight[coal_key])
            
            future = asyncio.get_event_loop().create_future()
            self._inflight[coal_key] = future

        try:
            result = await self._execute_request(method, endpoint, params, retries, headers)
            if not future.done(): future.set_result(result)
            return result
        except Exception as e:
            if not future.done(): future.set_exception(e)
            return {"error": str(e)}
        finally:
            async with self._inflight_lock:
                self._inflight.pop(coal_key, None)

    async def _execute_request(self, method: str, endpoint: str, params: Dict, retries: int, headers: Dict) -> Dict:
        ep_cb = self._get_endpoint_cb(endpoint)
        if not ep_cb.is_available:
            return {"error": "CIRCUIT_BREAKER_OPEN"}

        if self._circuit_open and time.monotonic() < self._circuit_open_until:
            return {"error": "GLOBAL_CB_OPEN"}

        async with self._rate_semaphore:
            await self._check_budget()
            session = await self._get_session()
            base = self.fallback_url if self._using_fallback else self.primary_url
            url = f"{base}{endpoint}"
            proxy = os.getenv("PROXY_RADAR" if not self.is_execution else "PROXY_OMS")

            for attempt in range(retries):
                try:
                    # aiohttp handles params and json differently:
                    kwargs = {"headers": headers, "proxy": proxy}
                    if method.upper() == "GET":
                        kwargs["params"] = params
                    else:
                        kwargs["json"] = params

                    async with session.request(method, url, **kwargs) as resp:
                        if resp.status == 200:
                            data = await resp.json(loads=orjson.loads)
                            ret_code = data.get("retCode")
                            if ret_code == 0:
                                self._consecutive_failures = 0
                                ep_cb.record_success()
                                return data.get("result", {})
                            
                            # Rate limits 
                            if ret_code in (10006, 10018):
                                wait = (2 ** attempt) + random.uniform(0.1, 0.5)
                                logger.warning(f"⚠️ Rate limited {ret_code} on {endpoint}. Wait {wait:.1f}s")
                                await asyncio.sleep(wait)
                                continue
                            
                            return data # Explicitly return API error
                        
                        elif resp.status == 429:
                            # [NERVE REPAIR v4.3] Intellectual Backoff
                            retry_after = float(resp.headers.get("Retry-After", 0.5))
                            jitter = random.uniform(0.1, 0.4)
                            wait = retry_after + jitter
                            logger.warning(f"🛑 [429] Too Many Requests. Retry-After: {retry_after}s. Sleeping {wait:.2f}s (Jitter applied)")
                            await asyncio.sleep(wait)
                            # Rotate endpoint on consecutive 429s as implied by GEKTOR
                            self.rotate_endpoint()
                            continue
                        
                        else:
                            text = await resp.text()
                            logger.error(f"❌ HTTP {resp.status} on {endpoint}: {text[:100]}")
                            ep_cb.record_failure(f"HTTP_{resp.status}")
                
                except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError) as e:
                    # [NERVE REPAIR v4.3] Domain Rotation on Network Failure
                    logger.error(f"🌐 Connectivity failure on {endpoint} (Attempt {attempt+1}): {e}")
                    self.rotate_endpoint()
                    
                    if attempt == retries - 1 and not self._using_fallback:
                        # Permanent switch for this process if primary is unreachable
                        self._using_fallback = True
                        logger.warning("🔄 Switching permanently to fallback API due to network errors.")
                except Exception as e:
                    logger.error(f"💥 Request error sub {endpoint}: {e}")
                
                # Exponential Backoff with Jitter for network-level retries
                wait = min(2.5, (0.5 * (2 ** attempt))) + random.uniform(0.01, 0.1)
                await asyncio.sleep(wait)
        
        return {"error": "MAX_RETRIES_EXCEEDED"}

    def rotate_endpoint(self):
        """[NERVE REPAIR v4.3] Endpoint Fallback Pattern."""
        self._using_fallback = not self._using_fallback
        current = self.fallback_url if self._using_fallback else self.primary_url
        logger.debug(f"🔄 [REST] Endpoint Rotated to: {current}")

    async def get_tickers(self) -> List[Dict]:
        res = await self._request("GET", "/v5/market/tickers", {"category": "linear"})
        return res.get("list", []) if isinstance(res, dict) else []

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[Dict]:
        p = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        res = await self._request("GET", "/v5/market/kline", params=p)
        klines = res.get("list", []) if isinstance(res, dict) else []
        klines.reverse()
        return klines

    async def get_orderbook_snapshot(self, symbol: str, limit: int = 50) -> Dict:
        return await self._request("GET", "/v5/market/orderbook", {"category": "linear", "symbol": symbol, "limit": limit})

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> List[Dict]:
        res = await self._request("GET", "/v5/market/recent-trade", {"category": "linear", "symbol": symbol, "limit": limit})
        return res.get("list", []) if isinstance(res, dict) else []

    async def get_instruments_info(self, category: str = "linear") -> Dict:
        return await self._request("GET", "/v5/market/instruments-info", {"category": category})

    async def get_server_time(self) -> Dict:
        return await self._request("GET", "/v5/market/time")

    async def get_wallet_balance(self, headers: Dict) -> Dict:
        return await self._request("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": "USDT"}, headers=headers)

    async def get_order_realtime(self, symbol: str, order_link_id: str, headers: Dict) -> Dict:
        return await self._request("GET", "/v5/order/realtime", {"category": "linear", "symbol": symbol, "orderLinkId": order_link_id}, headers=headers)

    async def get_api_info(self, headers: Dict) -> Dict:
        return await self._request("GET", "/v5/user/query-api", headers=headers)

    async def get_positions(self, category: str = "linear", symbol: str = None) -> List[Dict]:
        p = {"category": category}
        if symbol: p["symbol"] = symbol
        res = await self._request("GET", "/v5/position/list", p)
        return res.get("list", []) if isinstance(res, dict) else []
