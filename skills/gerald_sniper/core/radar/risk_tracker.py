# core/radar/risk_tracker.py
import asyncio
import logging
import json
from dataclasses import dataclass
from typing import Optional
from loguru import logger

@dataclass(slots=True, frozen=True)
class RiskRecommendation:
    approved_size_pct: float
    is_suppressed: bool
    reason: Optional[str] = None

class PortfolioRiskTracker:
    """
    [GEKTOR v21.5] Atomic Systemic Risk Controller.
    Eliminates I/O Bottlenecks and Race Conditions by maintaining
    active exposure state in RAM.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(PortfolioRiskTracker, cls).__new__(cls)
        return cls._instance

    def __init__(self, total_cap_pct: float = 0.9, single_cap_pct: float = 0.25):
        if hasattr(self, '_initialized'): return
        self.total_limit = total_cap_pct
        self.single_limit = single_cap_pct
        self.current_exposure: float = 0.0
        self._lock = asyncio.Lock()
        self._initialized = True

    async def hydrate(self, db_manager) -> None:
        """
        Re-syncs in-memory state with DB on cold start.
        Crucial for survival across process restarts.
        """
        try:
            records = await db_manager.fetch_active_contexts()
            total_exposure = 0.0
            
            # Assume constant total_equity for PCT calculation here, or pass it
            # For v21.5 we use relative sizing (pct of equity)
            for r in records:
                ctx_data = json.loads(r['context'])
                # Exposure here is stored as a percentage of equity at entry
                total_exposure += ctx_data.get('size_pct', 0.0)
            
            self.current_exposure = total_exposure
            logger.info(f"🛡️ [RiskTracker] Hydrated. Current Exposure: {self.current_exposure:.2%}")
        except Exception as e:
            logger.error(f"🛡️ [RiskTracker] Hydration failed: {e}")
            self.current_exposure = 0.0

    async def validate_signal(self, kelly_f: float, win_prob: float) -> RiskRecommendation:
        """
        Atomic Check-and-Reserve pattern.
        Applies: Half-Kelly -> Correlation Haircut -> Single Cap -> Global Cap.
        """
        async with self._lock:
            # 1. Half-Kelly (Conservative Core)
            suggested_size = kelly_f * 0.5
            
            # 2. Correlation Haircut (Beta Risk)
            # If current exposure > 50%, we shift to Quarter-Kelly logic
            if self.current_exposure > 0.5:
                suggested_size *= 0.5
                logger.debug("🛡️ [RiskTracker] High exposure (>50%). Applying Correlation Haircut (0.5x).")

            # 3. Single Asset Cap (Max 25% to prevent idiosyncratic ruins)
            suggested_size = min(suggested_size, self.single_limit)

            # 4. Global Resilience Cap (90% Hard Ceiling)
            remaining_space = self.total_limit - self.current_exposure
            
            if remaining_space <= 0.01: # Less than 1% margin
                return RiskRecommendation(0.0, True, "GLOBAL_LIMIT_REACHED")

            final_size = min(suggested_size, remaining_space)
            
            # 5. Atomic Reservation (Optimistic update)
            if final_size > 0.01:
                self.current_exposure += final_size
                logger.info(f"🛡️ [RiskTracker] Reserved {final_size:.2%}. Total Exposure: {self.current_exposure:.2%}")
                return RiskRecommendation(approved_size_pct=final_size, is_suppressed=False)
            else:
                return RiskRecommendation(0.0, True, "SIZE_TOO_SMALL")

    async def release_exposure(self, size_pct: float) -> None:
        """Phase 5 Callback: Released exposure when Monitoring Context is closed."""
        async with self._lock:
            old_exp = self.current_exposure
            self.current_exposure = max(0.0, self.current_exposure - size_pct)
            logger.info(f"🛡️ [RiskTracker] Released {size_pct:.2%}. Exposure: {old_exp:.2%} -> {self.current_exposure:.2%}")

# Global Singleton instance
risk_tracker = PortfolioRiskTracker()
