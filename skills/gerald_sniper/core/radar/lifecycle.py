import asyncio
import logging
import time
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from enum import Enum

logger = logging.getLogger("GEKTOR.Lifecycle")

class RadarSignalState(BaseModel):
    id: str
    symbol: str
    direction: str  # "LONG" / "SHORT"
    price: float
    tp: float
    sl: float
    timestamp: float
    message_id: Optional[int] = None
    chat_id: Optional[int] = None

class ActiveAlert(BaseModel):
    id: str
    asset: str
    direction: int
    entry_p: float
    tp_p: float
    sl_p: float
    best_p: float
    worst_p: float
    tp_volume_accum: float = 0.0
    min_fill_volume: float = 50000.0 
    is_closed: bool = False

class AlertLifecycleMonitor:
    """
    [GEKTOR v11.8] Execution-Fidelity Advisor.
    Calculates MAE/MFE and avoids 'Phantom Liquidity' illusions.
    """
    def __init__(self):
        self._active: Dict[str, List[ActiveAlert]] = {}
        self._signal_states: Dict[str, RadarSignalState] = {} # In-memory cache for demo/fast-recovery

    def register(self, alert_id: str, asset: str, direction: int, entry: float, tp: float, sl: float):
        if asset not in self._active:
            self._active[asset] = []
        
        self._active[asset].append(ActiveAlert(
            id=alert_id, asset=asset, direction=direction,
            entry_p=entry, tp_p=tp, sl_p=sl,
            best_p=entry, worst_p=entry
        ))
        
        # Also store as SignalState for BotListener integration
        self._signal_states[alert_id] = RadarSignalState(
            id=alert_id, symbol=asset, 
            direction="LONG" if direction == 1 else "SHORT",
            price=entry, tp=tp, sl=sl, timestamp=time.time()
        )
        logger.debug(f"📜 [Lifecycle] Registered {alert_id} for {asset}")

    async def get_signal_state(self, signal_id: str) -> Optional[RadarSignalState]:
        """[Audit 10.2] Retrieval from memory (fallback) or Redis."""
        # For now, memory-only. In production this should hit Redis.
        return self._signal_states.get(signal_id)

    async def get_audited_entry_price(self, state: RadarSignalState, tap_time: float) -> float:
        """[Pessimistic Auditor] Calculates realistic entry based on time elapsed."""
        # Minimal penalty logic for now. 
        # In real APEX, this scans CandleCache for slippage during the delay.
        return state.price 

    async def link_message(self, signal_id: str, chat_id: int, message_id: int):
        """Links Telegram message ID to signal for lifecycle management (editing/deletion)."""
        if signal_id in self._signal_states:
            self._signal_states[signal_id].chat_id = chat_id
            self._signal_states[signal_id].message_id = message_id
            logger.debug(f"🔗 [Lifecycle] Linked signal {signal_id} -> msg {message_id}")

    async def start_reaper(self, bot: Any):
        """[GEKTOR v10.2] Background task to invalidate/clean up old signals."""
        logger.info("💀 [Lifecycle] Reaper Daemon ACTIVE.")
        # Reaper logic (stub)
        pass

    async def update(self, asset: str, price: float, volume_usd: float):
        """Processes each trade. Validates MFE/MAE with Liquidity Check."""
        if asset not in self._active or not self._active[asset]: return

        active = self._active[asset]
        survivors = []

        for alert in active:
            if alert.direction == 1: # LONG
                alert.best_p = max(alert.best_p, price)
                alert.worst_p = min(alert.worst_p, price)
                at_tp = price >= alert.tp_p
                at_sl = price <= alert.sl_p
            else: # SHORT
                alert.best_p = min(alert.best_p, price)
                alert.worst_p = max(alert.worst_p, price)
                at_tp = price <= alert.tp_p
                at_sl = price >= alert.sl_p

            if at_tp:
                alert.tp_volume_accum += volume_usd
                if alert.tp_volume_accum >= alert.min_fill_volume:
                    self._close_alert(alert, "WIN (TP)")
                    continue
                else:
                    survivors.append(alert)
                    continue

            if at_sl:
                self._close_alert(alert, "LOSS (SL)")
                continue

            survivors.append(alert)

        self._active[asset] = survivors

    def _close_alert(self, alert: ActiveAlert, status: str):
        mfe = abs(alert.best_p - alert.entry_p) / alert.entry_p * 100
        mae = abs(alert.entry_p - alert.worst_p) / alert.entry_p * 100
        logger.info(
            f"🏁 [Lifecycle] Alert {alert.id} CLOSED [{status}]. "
            f"MFE: +{mfe:.2f}%, MAE: -{mae:.2f}%"
        )

# Global Instance
lifecycle_manager = AlertLifecycleMonitor()
