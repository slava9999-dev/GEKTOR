from core.infrastructure.proxy_pool import ProxyPool
from core.infrastructure.tg_client import ProxyAwareTelegramClient
import random
import json

class SharedProxyTransport:
    """
    [GEKTOR v21.12] Centralized Network Interface.
    Maintains session state and handles transport-level circuit breakers.
    """
    def __init__(self, token: str, proxy_pool):
        self.token = token
        self.proxy_pool = proxy_pool
        self._clients = {} # proxy -> Client mapping

    def get_client(self, proxy: str):
        if proxy not in self._clients:
            self._clients[proxy] = ProxyAwareTelegramClient(self.token, proxy)
        return self._clients[proxy]

    async def close(self):
        for client in self._clients.values():
            await client.close()

class MultiLaneOutboxRelay:
    """
    [GEKTOR v21.11] Multi-Lane Outbox Architecture.
    - Emergency Lane: Isolated high-priority queue for ABORT messages.
    - General Lane: Common throughput for entries and targets.
    - Proxy Rotation: Automatic failover between SOCKS5 nodes.
    - Exponential Backpressure: Prevents DB-hammering during network outages.
    """
    def __init__(self, db, token: str, proxies: List[str], signal_ttl_sec: float = 5.0):
        self.db = db
        self.transport = SharedProxyTransport(token, ProxyPool(proxies))
        self.proxy_pool = self.transport.proxy_pool
        self.signal_ttl = signal_ttl_sec
        self.last_emergency_warn = 0

    async def run(self, shutdown_event: asyncio.Event):
        """Orchestrates parallel lanes."""
        logger.info("📡 [Outbox] Multi-Lane Relay Engine Scaling Up.")
        # Start both lanes in parallel
        await asyncio.gather(
            self._emergency_worker(shutdown_event),
            self._general_worker(shutdown_event)
        )

    async def _emergency_worker(self, shutdown_event: asyncio.Event):
        """Dedicated high-speed lane for TIER 1 (Emergency) only."""
        logger.info("📡 [EmergencyLane] Tier 1 Monitor ACTIVE.")
        while not shutdown_event.is_set():
            try:
                # 1. Fetch only priority 100
                batch = await self.db.fetch_outbox_by_priority(min_priority=100, max_priority=100, limit=5)
                
                # 2. [GEKTOR v21.12] Dead Man's Switch Check
                if batch:
                    oldest_ts = min([e['created_at'] for e in batch])
                    silence_duration = time.time() - oldest_ts
                    if silence_duration > 600: # 10 Minutes
                         if time.time() - self.last_emergency_warn > 60:
                             logger.critical(f"🆘 [DeadManSwitch] TELEGRAM BLACKOUT DETECTED: Emergency alert pending for {silence_duration/60:.1f}m!")
                             # Trigger Secondary Channel (Pushover/NTFY) here
                             self.last_emergency_warn = time.time()
                
                for entry in batch:
                    await self._process_entry(entry, is_emergency=True)
                
                # High frequency check for emergency
                await asyncio.sleep(0.1 if batch else 0.5)
            except Exception as e:
                logger.error(f"📡 [EmergencyLane] Crash: {e}")
                await asyncio.sleep(1.0)

    async def _general_worker(self, shutdown_event: asyncio.Event):
        """Low-priority lane for TIER 2 & 3."""
        logger.info("📡 [GeneralLane] Tier 2/3 Monitor ACTIVE.")
        while not shutdown_event.is_set():
            try:
                # Fetch priority 0-99
                batch = await self.db.fetch_outbox_by_priority(min_priority=0, max_priority=99, limit=10)
                for entry in batch:
                    await self._process_entry(entry, is_emergency=False)
                
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"📡 [GeneralLane] Crash: {e}")
                await asyncio.sleep(2.0)

    async def _process_entry(self, entry: dict, is_emergency: bool):
        """Individual dispatch logic with Proxy Rotation and Backpressure."""
        now = time.time()
        
        # 1. TTL Check (Safety threshold)
        wait_time = now - entry.get('created_at', now)
        if not is_emergency and wait_time > self.signal_ttl:
             logger.warning(f"📡 [Outbox] [{entry['id']}] TTL EXPIRED ({wait_time:.1f}s). Purging.")
             await self.db.mark_outbox_status(entry['id'], 'EXPIRED')
             return

        # 2. Late-Stage Validity Check
        payload = entry.get('payload', {})
        if isinstance(payload, str): payload = json.loads(payload)
        
        signal_id = payload.get('id') or payload.get('signal_id')
        is_active = await self.db.is_signal_still_active(signal_id)
        if not is_active:
             logger.info(f"📡 [Outbox] [{entry['id']}] Signal no longer valid. CANCELLING.")
             await self.db.mark_outbox_status(entry['id'], 'CANCELED')
             return

        # 3. Proxy-Aware Dispatch via Shared Transport
        proxy = self.proxy_pool.get_proxy()
        client = self.transport.get_client(proxy)
        
        # Prepare text with E2E Telemetry
        created_at_precise = entry.get('created_at_precise', now)
        delta_out_ms = int((time.time() - created_at_precise) * 1000)
        
        text = payload.get('text', 'NO_TEXT')
        footer = f"\n\n[Latency: E2E {delta_out_ms}ms | Proxy: {proxy.split('@')[-1]}]"
        
        success = await client.send_message(payload.get('chat_id'), text + footer)
            
        if success:
            await self.db.mark_outbox_status(entry['id'], 'SENT', time.time())
        else:
            # 4. Exponential Backoff & Jitter Protocol
            self.proxy_pool.mark_bad(proxy)
            attempt = entry.get('attempt_count', 0)
            
            # Backoff: 2^n with caps (30s for emergency, 300s for general)
            base_wait = min(2 ** attempt, 30 if is_emergency else 300)
            jitter = random.uniform(0.5, 1.5)
            wait_time = base_wait * jitter
            
            logger.warning(f"📡 [Outbox] [{entry['id']}] Failed. Scheduling retry in {wait_time:.1f}s (Attempt #{attempt+1})")
            await self.db.mark_retry(entry['id'], wait_time)
