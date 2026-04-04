import asyncio
import logging
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from collections import deque

logger = logging.getLogger("GEKTOR.Lifecycle")

class ActiveAlert(BaseModel):
    id: str
    asset: str
    direction: int
    entry_p: float
    tp_p: float
    sl_p: float
    best_p: float
    worst_p: float
    
    # [GEKTOR v11.8] Fidelity Tracking: Total volume traded at/past TP
    tp_volume_accum: float = 0.0
    min_fill_volume: float = 50000.0 # Standard Institutional Threshold ($50k) 
    is_closed: bool = False

class AlertLifecycleMonitor:
    """
    [GEKTOR v11.8] Execution-Fidelity Advisor.
    Calculates MAE/MFE and avoids 'Phantom Liquidity' illusions.
    """
    def __init__(self):
        self._active: Dict[str, List[ActiveAlert]] = {}

    def register(self, alert_id: str, asset: str, direction: int, entry: float, tp: float, sl: float):
        if asset not in self._active:
            self._active[asset] = []
        
        self._active[asset].append(ActiveAlert(
            id=alert_id, asset=asset, direction=direction,
            entry_p=entry, tp_p=tp, sl_p=sl,
            best_p=entry, worst_p=entry
        ))
        logger.debug(f"📜 [Lifecycle] Registered {alert_id} for {asset}")

    async def update(self, asset: str, price: float, volume_usd: float):
        """Processes each trade. Validates MFE/MAE with Liquidity Check."""
        if asset not in self._active or not self._active[asset]: return

        active = self._active[asset]
        survivors = []

        for alert in active:
            # 1. Update MAE (Maximum Adverse Excursion)
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

            # 2. PHANTOM LIQUIDITY GUARD (v11.8)
            # TP is only triggered if Price is at level AND significant volume TRADED there.
            if at_tp:
                alert.tp_volume_accum += volume_usd
                if alert.tp_volume_accum >= alert.min_fill_volume:
                    self._close_alert(alert, "WIN (TP)")
                    continue
                else:
                    # Not enough volume yet. Price is touching but don't count as win.
                    survivors.append(alert)
                    continue

            # 3. SL Check (Immediate trigger, no liquidity guard for losses - pessimistic view)
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
        # Store in DB...
