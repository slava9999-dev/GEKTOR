# core/runtime/context.py

from dataclasses import dataclass
from typing import Optional, Any
from core.events.nerve_center import NerveCenter
from core.shield.risk_engine import RiskEngine, init_risk_guard
from core.scenario.tactical_orchestrator import TacticalOrchestrator
from core.scenario.oms_adapter import IExchangeAdapter
from loguru import logger

@dataclass
class TradingContext:
    """
    [GEKTOR v8.5] Isolated Trading Context.
    Ensures strict separation between Combat (Real Capital) and Shadow (Virtual) environments.
    """
    name: str # 'COMBAT' or 'SHADOW'
    bus: NerveCenter
    risk_engine: RiskEngine
    orchestrator: TacticalOrchestrator
    adapter: IExchangeAdapter
    kill_switch: Optional[Any] = None # KillSwitch instance
    is_active: bool = False

    async def start(self, db_manager, paper_tracker, candle_mgr, level_service, time_sync):
        """Initializes all components within this context."""
        logger.info(f"🏗️ [Context:{self.name}] Initializing...")
        
        # 1. Start Bus
        await self.bus.start_listening()
        
        # 2. Risk Engine starts background monitoring
        self.risk_engine.start()
        
        # 3. Start Orchestrator (Decision Engine)
        await self.orchestrator.start(
            db_manager=db_manager,
            paper_tracker=paper_tracker,
            candle_cache=candle_mgr,
            adapter=self.adapter,
            level_service=level_service,
            time_sync=time_sync,
            kill_switch=self.kill_switch
        )
        
        self.is_active = True
        logger.success(f"✅ [Context:{self.name}] ACTIVE")

    async def process_signal(self, signal: Any):
        """SignalConsumer implementation for Dispatcher."""
        if not self.is_active: return
        
        # Route the signal into the orchestrator's queue
        # For MacroRadar signals, they usually trigger a DetectorEvent or a direct evaluation
        from core.scenario.signal_entity import TradingSignal
        if isinstance(signal, TradingSignal):
             await self.orchestrator.put_signal(signal)
        else:
             # Handle MacroSignal conversion or direct processing
             logger.warning(f"⚠️ [Context:{self.name}] Unknown signal type: {type(signal)}")
