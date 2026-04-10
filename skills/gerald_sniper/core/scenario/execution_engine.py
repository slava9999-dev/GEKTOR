# core/scenario/execution_engine.py
import asyncio
from loguru import logger
from typing import Any, Optional
from core.execution.gateway import OrderRoutingGateway, SnipeCommand

class ExecutionEngine:
    """
    [GEKTOR v14.8.1] Execution Orchestrator.
    Manages the OrderRoutingGateway and handles the High-Resolution execution loop.
    """
    def __init__(
        self, 
        db_manager: Any, 
        adapter: Any, 
        redis_client: Any, 
        candle_cache: Any, 
        config: dict, 
        time_sync: Optional[Any] = None,
        kill_switch: Optional[Any] = None
    ):
        self.db = db_manager
        self.adapter = adapter
        self.redis = redis_client
        self.candle_cache = candle_cache
        self.config = config
        self.time_sync = time_sync
        self.kill_switch = kill_switch
        
        # Initialize the low-level Gateway
        self.command_queue = asyncio.Queue()
        self.gateway = OrderRoutingGateway(
            exchange_client=adapter, 
            command_queue=self.command_queue,
            config=config
        )
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Starts the execution gateway loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self.gateway.run())
        logger.info("🚀 [ExecutionEngine] Gateway loop ACTIVE.")

    async def stop(self):
        """Stops the execution gateway loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 [ExecutionEngine] Gateway loop STOPPED.")

    async def submit_snipe(
        self, 
        symbol: str, 
        side: str, 
        qty: float, 
        price: float, 
        slippage_bps: float = 10.0,
        is_evacuation: bool = False
    ):
        """Queues a snipe command for the Gateway to process."""
        cmd = SnipeCommand(
            symbol=symbol,
            side=side,
            qty=qty,
            base_price=price,
            max_slippage_bps=slippage_bps,
            cl_ord_id="", # Assigned by Gateway
            is_evacuation=is_evacuation
        )
        await self.command_queue.put(cmd)
        logger.debug(f"📥 [ExecutionEngine] Snipe queued for {symbol} {side} @ {price}")
