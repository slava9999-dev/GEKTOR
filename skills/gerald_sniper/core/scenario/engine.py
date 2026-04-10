# core/scenario/engine.py

from typing import List, Dict, Any, Optional
from loguru import logger
from datetime import datetime

from core.alerts.models import AlertType, AlertEvent
from core.levels.models import LevelCluster
from core.structure.models import StructureType

class ScenarioEngine:
    """
    Analyzes price-level interactions and behavioral structures to generate high-level alert events.
    Fulfills the 'Visual Alert' pipeline requirements in AlertStateManager.
    """
    
    def __init__(self):
        self.last_alerts = {} # symbol:lid -> ts

    def evaluate(
        self, 
        cluster: LevelCluster, 
        activity: Any, 
        structures: List[Any], 
        current_price: float
    ) -> List[AlertEvent]:
        """
        Processes detected structures and proximity to generate alert events.
        Args:
            cluster: The price level being approached/hit.
            activity: Market intensity metrics (e.g. from trade_tracker).
            structures: List of detected behavioral patterns (COMPRESSION, BREAK, etc.).
            current_price: Latest ticker price.
        """
        events = []
        symbol = cluster.symbol
        lid = cluster.level_id
        
        # 1. Proximity Logic (LEVEL_NEAR)
        dist_pct = abs(current_price - cluster.price) / cluster.price * 100
        
        # 2. Behavioral Mapping
        for struct in structures:
            if struct.type == StructureType.COMPRESSION:
                events.append(self._create_alert(cluster, AlertType.LEVEL_NEAR, {
                    "pattern": "COMPRESSION",
                    "strength": struct.strength,
                    "dist": dist_pct
                }))
            
            elif struct.type == StructureType.BREAK:
                events.append(self._create_alert(cluster, AlertType.LEVEL_BROKEN, {
                    "pattern": "BREAKOUT",
                    "strength": struct.strength,
                    "direction": struct.extra.get("direction"),
                    "vol_spike": struct.extra.get("vol_spike")
                }))
                
            elif struct.type == StructureType.SWEEP:
                events.append(self._create_alert(cluster, AlertType.LEVEL_TOUCHED, {
                    "pattern": "SWEEP/REJECTION",
                    "strength": struct.strength,
                    "wick_pct": struct.extra.get("wick_pct")
                }))

        return events

    def _create_alert(self, cluster: LevelCluster, alert_type: AlertType, payload: dict) -> AlertEvent:
        return AlertEvent(
            type=alert_type,
            symbol=cluster.symbol,
            level_id=cluster.level_id,
            severity="INFO" if alert_type != AlertType.LEVEL_BROKEN else "WARN",
            payload={
                "level_price": cluster.price,
                "side": str(cluster.side),
                "source": "+".join(cluster.sources),
                "score": cluster.strength,
                **payload
            }
        )

# Global Instance
scenario_engine = ScenarioEngine()
