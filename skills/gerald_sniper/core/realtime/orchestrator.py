# skills/gerald_sniper/core/realtime/orchestrator.py
import asyncio
import signal
import os
import sys
import time
import aiohttp
import json
from typing import Optional, List, Dict
from loguru import logger

from .zero_alloc import ZeroAllocationTickBuffer, tick_store
from .signal_router import TacticalSignalRouter
from .deduplicator import TickDeduplicator
from ..quant.bar_engine import AdaptiveDollarBarEngine
from ..quant.gap_guard import VolumeGapGuard
from ..quant.quarantine import ClockSynchronizer, MarketQuarantineGuard, LatencyGate
from .consensus import WatermarkAligner
from .drift_monitor import ConceptDriftMonitor
from .shadow_tracker import ShadowPositionTracker
from .relay import CompactingOutboxRelay
from .telemetry import TelemetryManager
from ...data.database import DatabaseManager
from ..infrastructure.bybit_rest import BybitRestClient
from .proxy_provider import ProfessionalProxyProvider
from .emergency import EmergencyBypass

_WS_CONNECT_TIMEOUT = 10.0
_WS_SUBSCRIBE_TIMEOUT = 5.0

# ProxyRotator replaced by ProfessionalProxyProvider in proxy_provider.py

class BybitStreamIngestor:
    """[GEKTOR v21.58] High-Availability WebSocket Ingestor."""
    def __init__(self, symbols: List[str], gap_guard: VolumeGapGuard, proxy_provider: ProfessionalProxyProvider):
        self.symbols = symbols
        self.gap_guard = gap_guard
        self.proxy_provider = proxy_provider
        self._running = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.current_proxy: Optional[str] = None

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    async def run(self, proxy_override: Optional[str] = None):
        """Main Loop with built-in reconnection logic."""
        self._running = True
        url = "wss://stream.bybit.com/v5/public/linear"
        
        while self._running:
            session = None
            self.current_proxy = proxy_override or self.proxy_provider.get_best_node()
            
            try:
                # [GEKTOR v21.58] Make-before-break: Ensuring session health
                session = aiohttp.ClientSession(trust_env=True)
                self._ws = await asyncio.wait_for(
                    session.ws_connect(url, heartbeat=20.0, proxy=self.current_proxy), 
                    timeout=_WS_CONNECT_TIMEOUT
                )
                
                # Subscription
                topics = [f"publicTrade.{s}" for s in self.symbols]
                await self._ws.send_json({"op": "subscribe", "args": topics})
                
                logger.success(f"🟢 [Ingestor] Connected via {self.current_proxy or 'DIRECT'}.")
                
                async for msg in self._ws:
                    if not self._running:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if "data" in data:
                            for trade in data["data"]:
                                await self.gap_guard.on_new_tick({
                                    "E": int(trade["T"]),
                                    "p": float(trade["p"]),
                                    "q": float(trade["v"]),
                                    "s": trade["s"],
                                    "i": trade.get("i", ""),
                                    "side": "B" if trade.get("L") == "Buy" else "S"
                                })
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): 
                        break
            except Exception as e:
                if self._running:
                    logger.error(f"🔴 [Ingestor] Stream Error on {self.current_proxy}: {e}")
                    if self.current_proxy and not proxy_override:
                        self.proxy_provider.report_failure(self.current_proxy)
                    await asyncio.sleep(2)
            finally:
                if session: await session.close()
                if proxy_override: # If we are a shadow ingestor, don't auto-reconnect
                    break

class GektorAPEXDaemon:
    """[GEKTOR APEX] Institutional Orchestrator v21.58."""
    def __init__(self, db_manager: DatabaseManager, proxy_list: List[str]):
        self.db = db_manager
        self.proxy_provider = ProfessionalProxyProvider(proxy_list)
        
        # Architecture Layer
        self.clock_sync = ClockSynchronizer()
        self.quarantine = MarketQuarantineGuard(self.clock_sync)
        self.dedup = TickDeduplicator()
        
        # Engine Layer
        tg_token = os.getenv("TG_BOT_TOKEN")
        tg_chat = os.getenv("TG_CHAT_ID")
        self.tg_client = None
        if tg_token and tg_chat:
            from .telegram_client import TelegramClient
            self.tg_client = TelegramClient(tg_token, tg_chat, proxy=self.proxy_provider.get_best_node())
            logger.info("📡 [CORE] Egress Relay: Telegram READY.")

        self.bar_engine = AdaptiveDollarBarEngine(self.clock_sync, grid=self.quarantine)
        self.relay = CompactingOutboxRelay(self.db, self.tg_client)
        self.signal_router = TacticalSignalRouter(self.relay, self.db)
        
        self._shutdown_event = asyncio.Event()

        # Transport Layer
        self.rest_client = BybitRestClient(proxy_provider=self.proxy_provider)
        self.gap_guard = VolumeGapGuard(
            self.bar_engine, 
            self.rest_client, 
            self.dedup, 
            on_integrity_violation=self.proxy_provider.report_gap,
            signal_router=self.signal_router,
            shutdown_event=self._shutdown_event
        )
        self.bar_engine.grid.gap_guard = self.gap_guard
        
        self.ingestor = BybitStreamIngestor(
            ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            self.gap_guard, self.proxy_provider
        )
        self.gap_guard.source = self.ingestor
        
        # Monitoring Layer
        self.inference = ConceptDriftMonitor()
        self.consensus = WatermarkAligner(exchanges=["bybit"])
        self.drift = ConceptDriftMonitor()
        self.shadow = ShadowPositionTracker(self.db)
        
        self.telemetry: Optional[TelemetryManager] = None
        self._shutdown_event = asyncio.Event()
        self.consecutive_failures = 0
        self.max_backoff = 60 # seconds
        self._critical_tasks: List[asyncio.Task] = []

        # Emergency Protection Layer
        self.emergency = EmergencyBypass(
            api_key=os.getenv("BYBIT_API_KEY", ""),
            api_secret=os.getenv("BYBIT_API_SECRET", ""),
            testnet=os.getenv("BYBIT_TESTNET", "false").lower() == "true"
        )

    async def _on_environment_degrade(self):
        """[GEKTOR v21.63] Institutional Hot-Swap: Deep Hibernate with Clean Teardown."""
        self.consecutive_failures += 1
        
        # 0. DEEP HIBERNATE (Anti-Thrashing for restricted infrastructure)
        if self.proxy_provider.total_nodes <= 1:
            wait_time = min(300, 30 * self.consecutive_failures)
            logger.critical(f"🚨 [CORE] SINGLE PROXY TOXIC. Entering Deep Hibernate for {wait_time}s.")
            
            # [GEKTOR v21.63] Clean Teardown: Physical disconnect
            await self.ingestor.stop()
            
            await asyncio.sleep(wait_time)
            
            # [GEKTOR v21.63] Wake-up: Force cold restart and re-spawn ingestor
            await self.gap_guard.force_cold_restart()
            if hasattr(self, 'tg') and self.tg:
                self.ingestor_task = self.tg.create_task(self.ingestor.run(), name="Ingestor")
            return

        wait_time = min(self.max_backoff, 2 ** self.consecutive_failures)
        toxic_proxy = self.ingestor.current_proxy
        logger.warning(
            f"🔄 [HotSwap] Environment Toxic ({self.consecutive_failures} fails). "
            f"Cooling down for {wait_time}s before rotation attempt."
        )
        
        # 1. Period of silence to avoid Event Loop thrashing / CPU tax during blackout
        await asyncio.sleep(wait_time)
        
        # 2. Candidate selection
        new_proxy = self.proxy_provider.get_best_node(exclude=[toxic_proxy] if toxic_proxy else [])
        
        if not new_proxy:
            if not self.proxy_provider.cb.can_attempt:
                logger.critical("🔥 [GlobalCB] Infrastructure Starvation: Pool Exhausted. ACTIVATING EMERGENCY PROTECTION.")
                await self.shutdown() # Force emergency shutdown
            return

        # 2. Migration Logic
        shadow_ingestor = BybitStreamIngestor(
            self.ingestor.symbols, self.gap_guard, self.proxy_provider
        )
        shadow_task = None
        
        try:
            shadow_task = asyncio.create_task(
                shadow_ingestor.run(proxy_override=new_proxy), 
                name="ShadowIngestor"
            )
            
            # Stabilization wait
            start_wait = time.time()
            while time.time() - start_wait < 10:
                if shadow_ingestor._ws and not shadow_ingestor._ws.closed:
                    break
                await asyncio.sleep(0.5)
            else:
                raise RuntimeError(f"Shadow node {new_proxy} failed to stabilize.")

            # 3. Cutover (HFT Optimized: 1s overlap)
            old_ingestor = self.ingestor
            self.ingestor = shadow_ingestor
            self.gap_guard.source = self.ingestor
            
            # 4. Graceful Burial
            async def bury_old():
                try:
                    await asyncio.sleep(1) 
                    await old_ingestor.stop()
                    logger.info("⚰️ [HotSwap] Old session buried.")
                except Exception as e:
                    logger.error(f"Burial error: {e}")

            asyncio.create_task(bury_old())
            
            # Refresh REST Gateway with new node
            await self.rest_client.close()
            self.rest_client = BybitRestClient(proxy_provider=self.proxy_provider)
            self.consecutive_failures = 0 # Reset backoff on success
            logger.success(f"✅ [HotSwap] Migration successful to {new_proxy}.")
            
        except Exception as e:
            logger.error(f"❌ [HotSwap] Shadow warming failed: {e}")
            self.proxy_provider.report_failure(new_proxy)
            if shadow_task: 
                shadow_task.cancel()
                await shadow_ingestor.stop()
            if toxic_proxy: self.proxy_provider.report_failure(toxic_proxy)

    async def run(self):
        """[GEKTOR v21.60] Mission Runner using Python 3.11+ TaskGroup."""
        logger.info("🕰️ [CORE] Calibrating clocks with v21.60 HFT-APEX Pool...")
        
        try:
            async with asyncio.TaskGroup() as tg:
                self.tg = tg
                # 1. Bootstrap Ingress
                self.ingestor_task = tg.create_task(self.ingestor.run(), name="Ingestor")
                
                # 2. Bootstrap Sentinel
                self.sentinel_task = tg.create_task(
                    self.quarantine.run_sentinel(self._on_environment_degrade), 
                    name="Sentinel"
                )
                
                logger.success("🚀 [GEKTOR] System ARMED and MONITORING.")
                
                # 3. Liveness Check
                while not self._shutdown_event.is_set():
                    await asyncio.sleep(2)
                    
        except* Exception as eg:
            for e in eg.exceptions:
                logger.critical(f"💥 [CORE] Mission Critical Task Failure: {e}")
        finally:
            # [GEKTOR v21.60] EMERGENCY POSITION PROTECTION
            # asyncio.shield ensures we attempt cleanup even if the parent task is being cancelled
            await asyncio.shield(self.shutdown())

    async def shutdown(self):
        """[GEKTOR v21.60] Graceful Shutdown with Emergency Direct-Bypass."""
        if self._shutdown_event.is_set() and not self.proxy_provider.cb.can_attempt:
            # If we are already shutting down due to pool exhaustion, protect positions
            await self.emergency.secure_positions()

        logger.info("🔌 [CORE] Initiating shutdown sequence...")
        self._shutdown_event.set()
        
        # Cleanup
        await self.ingestor.stop()
        await self.rest_client.close()
        
        logger.info("💤 [CORE] Hibernation Complete.")
