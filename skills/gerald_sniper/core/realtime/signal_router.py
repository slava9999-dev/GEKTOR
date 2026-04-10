# skills/gerald_sniper/core/realtime/signal_router.py
import asyncio
import time
from typing import Dict, Optional
from loguru import logger
from .dto import SignalDTO, TelegramFormatter
from .persistence import InfrastructureStateRepo
from .telemetry import TelemetryManager
from .shadow import ShadowTrackerApex
from .inference import MetaLabelingInferenceNode
from ..quant.bar_engine import DollarBar

class TacticalSignalRouter:
    """
    [GEKTOR v21.49] Оркестратор сигналов.
    - Внедрена Asynchronous Inference Boundary для ML.
    - Внедрена защита от Concept Drift.
    - Event Loop защищен через ProcessPool.
    """
    
    def __init__(self, relay, db, base_equity: float = 10000.0, vpin_threshold: float = 0.7, cooldown_sec: int = 5):
        self.relay = relay
        self.db = db
        self.repo = InfrastructureStateRepo(db)
        self.shadow = ShadowTrackerApex(db)
        self.ml = MetaLabelingInferenceNode() # Изолированный ML-узел
        self.vpin_threshold = vpin_threshold
        self.ml_threshold = 0.65 # Барьер для Meta-Labeling
        self.cooldown_sec = cooldown_sec
        self.base_equity = base_equity
        self.min_sigma_floor = 0.0005 
        self.min_ticks_threshold = 200 
        self.last_signal_ts: Dict[str, float] = {}
        
    async def on_new_bar(self, bar: DollarBar):
        """
        Главный конвейер принятия решений (Pipeline).
        """
        now = time.time()
        
        # 1. ТЕХНИЧЕСКИЙ ФИЛЬТР (VPIN + Sigma + Cooldown)
        if bar.vpin < self.vpin_threshold:
            return
        
        if bar.sigma < self.min_sigma_floor:
            return

        if now - self.last_signal_ts.get(bar.symbol, 0) < self.cooldown_sec:
            logger.debug(f"⏳ [Router] {bar.symbol} on cooldown.")
            return

        # 2. РАСЧЕТ ОБЪЕМА (Position Sizing)
        position_size = self.base_equity * (bar.vpin - 0.5) * 2
        side = "BUY" if bar.vpin > self.vpin_threshold else "SELL" # Условная логика сторон

        # 3. [GEKTOR v21.49] ASYNCHRONOUS META-LABELING (AI Score)
        # Формируем вектор признаков. ВНИМАНИЕ: это CPU-bound задача, выполняется в ProcessPool.
        features = [bar.vpin, bar.sigma, float(bar.ticks), bar.volume]
        ml_score = await self.ml.score_signal(features)
        
        if ml_score < self.ml_threshold:
            logger.info(f"🙅 [ML] Signal {bar.symbol} REJECTED by AI. Score: {ml_score:.2f}")
            return

        # 4. ATOMIC PERSISTENCE (Transactional Outbox)
        dto: Optional[SignalDTO] = None
        accuracy = ml_score # Используем предиктивную вероятность как оценку точности
        
        try:
            async with self.db.SessionLocal() as session:
                async with session.begin():
                    dto = await self.repo.persist_state_and_signal(session, bar, side, position_size, accuracy)
        except Exception as e:
            logger.error(f"❌ [Router] Transactional Persistence Failure: {e}")
            return

        if not dto:
            return

        # 5. REGISTER SHADOW MISSION (Forward Testing)
        await self.shadow.register_signal(dto)

        self.last_signal_ts[bar.symbol] = now 
        
        logger.success(
            f"🎯 [Router] Signal {dto.signal_id} APPROVED BY AI. "
            f"VPIN: {dto.vpin:.4f} | Size: ${dto.volume_usd:,.0f} | AI Score: {ml_score:.2f}"
        )

        # 6. RELAY NOTIFICATION
        if self.relay:
            self.relay._flush_event.set()
        
        return dto
