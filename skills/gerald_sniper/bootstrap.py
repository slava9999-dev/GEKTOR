# skills/gerald_sniper/bootstrap.py
import asyncio
import os
from loguru import logger
from typing import Protocol, Optional
from .core.realtime.executor import BybitExecutionGateway, ExecutionMission
from .core.simulation.engine import VirtualTimeEventLoop
from .core.realtime.signal_router import TacticalSignalRouter
from .data.database import DatabaseManager

class ExchangeGateway(Protocol):
    async def place_order(self, symbol: str, side: str, qty: float, price: float) -> Optional[str]: ...
    async def get_order_status(self, clOrdID: str) -> str: ...

class GektorBootstrap:
    """
    [GEKTOR v21.49] Dependency Injection Container.
    Сборка системы в зависимости от GEKTOR_ENV (SIMULATION/LIVE_FIRE).
    """
    @staticmethod
    def build_system(db: DatabaseManager):
        env = os.getenv("GEKTOR_ENV", "SIMULATION")
        
        if env == "LIVE_FIRE":
            logger.critical("🔥 [BOOT] INITIATING LIVE FIRE PROTOCOL. REAL CAPITAL AT RISK.")
            # Для Linux/Production используем uvloop (макс. производительность)
            try:
                import uvloop
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            except ImportError:
                logger.warning("[BOOT] uvloop not available, using default SelectorLoop.")
            
            loop = asyncio.new_event_loop()
            # Реальный шлюз исполнения
            gateway = BybitExecutionGateway(db, None) 
        
        elif env == "SIMULATION":
            logger.info("🧪 [BOOT] INITIATING WARP-SPEED SIMULATION.")
            # Кастомный луп для бэктеста со сдвигом времени
            loop = VirtualTimeEventLoop()
            # Мок-шлюз (в реальности вынесен в отдельный класс)
            gateway = None 
            
        else:
            raise ValueError(f"FATAL: Unknown environment {env}")
            
        asyncio.set_event_loop(loop)
        return loop, gateway

async def launch_gektor():
    db = DatabaseManager()
    await db.initialize()
    
    loop, gateway = GektorBootstrap.build_system(db)
    
    # Инициализация всех доменов через внедренные зависимости
    # ...
    logger.success("🚀 [BOOT] GEKTOR APEX System Assembled and Synchronized.")
    return loop, gateway
