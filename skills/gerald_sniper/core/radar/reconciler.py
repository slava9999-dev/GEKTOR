import asyncio
from loguru import logger
from typing import Dict, List, Optional, Any

class StateReconciler:
    """
    [GEKTOR v13.9] The Keeper of Truth.
    Performs atomic reconciliation between Intent (Radar) and Reality (Exchange).
    Handles API Blackouts with a Pessimistic Fallback strategy.
    """
    def __init__(self, radar_client, exchange_client):
        self.radar = radar_client
        self.exchange = exchange_client
        self.is_synced = False
        self.last_known_diff = {}

    async def reconcile(self) -> Dict[str, Any]:
        """
        [ATOMIC SNAPSHOT SYNC]
        Attempt to merge intent and reality. 
        If exchange is offline, pivot to PESSIMISTIC_MODE.
        """
        logger.info("🔄 [RECONCILER] Initiating State Reconciliation...")

        # 1. Fetch Radar Intent (This is usually local network or Redis - high reliability)
        try:
            intent_snapshot = await self.radar.get_current_intent_snapshot()
        except Exception as e:
            logger.critical(f"🚨 [RADAR LOST] Cannot fetch intent from Radar VPS: {e}")
            return {"status": "FAULT_RADAR", "data": None}

        # 2. Fetch Exchange Positions (Internet-dependent - potential blackout)
        try:
            actual_positions = await self.exchange.get_open_positions()
            # SUCCESS PATH: Realistic diff
            self.last_known_diff = self._build_diff_report(intent_snapshot, actual_positions)
            self.is_synced = True
            return {"status": "SUCCESS", "report": self.last_known_diff}
            
        except Exception as e:
            logger.warning(f"⚠️ [EXCHANGE BLACKOUT] API Offline during sync: {e}")
            # PESSIMISTIC FALLBACK: Assume every 'EVACUATING' intent is a GHOST FILL
            return self._pessimistic_fallback(intent_snapshot)

    def _build_diff_report(self, intent: Dict, actual: List[Dict]) -> Dict:
        """ 
        The 'Gold Standard' Sync. 
        Compares Intent vs actual Bybit matching engine state.
        """
        report = {"ghost_fills": [], "matched_positions": []}
        actual_map = {p['symbol']: float(p['size']) for p in actual}

        for symbol, data in intent.get('orders', {}).items():
            actual_size = actual_map.get(symbol, 0.0)
            
            # Scenario: We intended to cancel/evacuate, but position exists
            if data['status'] == 'EVACUATING' and actual_size > 0:
                report['ghost_fills'].append({
                    "symbol": symbol,
                    "size": actual_size,
                    "msg": f"CRITICAL: Ghost Fill confirmed in {symbol}!"
                })
            elif actual_size > 0:
                report['matched_positions'].append({"symbol": symbol, "size": actual_size})

        return report

    def _pessimistic_fallback(self, intent: Dict) -> Dict:
        """ 
        [PESSIMISTIC MODE] - Total API Blackout.
        We cannot verify reality, so we assume the worst case for safety.
        """
        logger.error("🛑 [SAFETY] Entering PESSIMISTIC MODE - Assuming Ghost Fills on all active intents!")
        potential_risks = []
        
        for symbol, data in intent.get('orders', {}).items():
            if data['status'] == 'EVACUATING':
                potential_risks.append({
                    "symbol": symbol,
                    "size": "UNKNOWN",
                    "msg": "WARNING: Potential Ghost Fill (Exchange Offline!)"
                })
        
        return {
            "status": "PESSIMISTIC_LOCK", 
            "report": {"ghost_fills": potential_risks},
            "warning": "EXCHANGE_CONNECTION_FAILED"
        }

logger.info("🛡️ [Reconciler] APEX Protocol v13.9 - READY.")
