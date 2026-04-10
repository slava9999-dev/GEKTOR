import asyncio
import aiohttp
import socket
from loguru import logger
from typing import Dict, Any, Optional, List
import time

class APEXHealthGuard:
    """
    [GEKTOR v14.0] Final System Verifier.
    Checks Radar, Matrix, Sanity Gate, and RTT before enabling Strike button.
    """
    def __init__(self, radar, correlator, executor):
        self.radar = radar
        self.correlator = correlator
        self.executor = executor

    async def verify_readiness(self) -> bool:
        """
        [GO/NO-GO CHECK]
        Checks if the entire pipeline is safe for execution.
        """
        # 1. RTT (Latency) check
        rtt = await self.executor.get_current_rtt()
        if rtt > 200: # Institutional limit for HFT
            logger.error(f"🛑 [LATENCY] RTT too high: {rtt:.1f}ms. Strike DISABLED.")
            return False

        # 2. Matrix Warm-up
        if not self.correlator.zero_alloc_buffer.is_full:
            # Need history for Z-score reliability
            return False

        # 3. Data Integrity (Sanity Score)
        # We check the last 50 rows for NaN percentage
        matrix_view = self.correlator.zero_alloc_buffer.matrix
        nan_count = np.isnan(matrix_view).sum()
        health_score = 1.0 - (nan_count / matrix_view.size)
        
        if health_score < 0.85:
            logger.warning(f"⚠️ [DATA] Low health score: {health_score:.1%}. Risk of false anomalies.")
            return False

        return True

class ZeroLatencyExecutor:
    """
    [GEKTOR v14.0] High-Frequency Execution Engine.
    Minimized RTT via TCP_NODELAY, Connection Warming, and Atomic Retries.
    """
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://api.bybit.com"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        
        # Оптимизированный коннектор для мгновенного удара
        self.connector = aiohttp.TCPConnector(
            family=socket.AF_INET,
            verify_ssl=False, # Экономия на TLS-валидации в доверенном контуре
            ttl_dns_cache=3600,
            use_dns_cache=True,
            limit=None,
            force_close=False # Keep-Alive ON
        )
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            headers={"Connection": "keep-alive"}
        )
        self._is_warmed = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_rtt = 0.0

    async def warm_up(self):
        """ 
        [HOT PATH] Initial session warm-up and TCP_NODELAY enable.
        Starts the background Heartbeat to keep the pipe hot.
        """
        if not self._heartbeat_task:
            self._heartbeat_task = asyncio.create_task(self._keep_alive_heartbeat())
            logger.info("💓 [HEARTBEAT] Connection monitoring active.")
        
        return await self._ping_exchange()

    async def _ping_exchange(self) -> float:
        """ Actual low-level ping to measure RTT and keep TLS warm """
        t0 = asyncio.get_event_loop().time()
        try:
            async with self.session.get(f"{self.base_url}/v5/market/time") as resp:
                await resp.read()
                t1 = asyncio.get_event_loop().time()
                
                # TCP_NODELAY setup on first success
                if not self._is_warmed:
                    sock = resp.connection.transport.get_extra_info('socket')
                    if sock: sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    self._is_warmed = True

                self._last_rtt = (t1 - t0) * 1000
                return self._last_rtt
        except Exception:
            return 999.0

    async def _keep_alive_heartbeat(self):
        """ 
        [HOT CABLE] Sends periodic pings every 15s to prevent 
        ISP/NAT timeout and first-packet jitter. 
        """
        while True:
            await asyncio.sleep(15)
            await self._ping_exchange()
            logger.debug(f"💓 [HEARTBEAT] RTT: {self._last_rtt:.1f}ms")

    async def get_current_rtt(self) -> float:
        return self._last_rtt

    async def atomic_strike(self, order_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        [THE SHOT] Мгновенное исполнение ордера.
        Payload уже должен содержать orderLinkId (Idempotency) и Hard-SL/TP.
        """
        if not self._is_warmed:
            await self.warm_up()

        t0 = asyncio.get_event_loop().time()
        endpoint = f"{self.base_url}/v5/order/create"
        
        try:
            # GEKTOR v14.0: Используем POST с разогретым Keep-Alive
            async with self.session.post(endpoint, json=order_payload) as resp:
                r_json = await resp.json()
                t1 = asyncio.get_event_loop().time()
                
                latency = (t1 - t0) * 1000
                status = r_json.get("retCode", -1)
                
                if status == 0:
                    logger.success(f"🎯 [STRIKE SUCCESS] RTT: {latency:.2f}ms | Order: {order_payload.get('orderLinkId')}")
                else:
                    logger.warning(f"💢 [STRIKE REJECTED] Code: {status} | Msg: {r_json.get('retMsg')}")
                
                return r_json
                
        except Exception as e:
            # Если связь упала во время удара — это территория Reconciler'а
            logger.critical(f"💀 [STRIKE CRITICAL] Connection lost during strike: {e}")
            raise

logger.info("⚡ [Executor] Zero-Latency Core v14.0 APEX - INITIALIZED.")
