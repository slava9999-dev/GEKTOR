# core/radar/rehydration.py
import json
import aiosqlite
from loguru import logger
from typing import List, Dict

class StateRehydrator:
    """
    [GEKTOR v21.3] Institutional State Rehydration.
    Handles cold-start recovery by restoring active monitoring contexts from the database.
    Ensures that if the system reboots while signals are active, they are not lost.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def get_active_contexts(self) -> List[Dict]:
        """Queries the database for signals that were being monitored before the shutdown."""
        active_contexts = []
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT * FROM monitoring_contexts WHERE state = 'ACTIVE';"
                ) as cursor:
                    rows = await cursor.fetchall()
                    for row in rows:
                        active_contexts.append({
                            'signal_id': row['signal_id'],
                            'symbol': row['symbol'],
                            'context': json.loads(row['context']),
                            'created_at': row['created_at']
                        })
            
            if active_contexts:
                logger.info(f"♻️ [Rehydration] Found {len(active_contexts)} active monitoring contexts.")
            else:
                logger.info("♻️ [Rehydration] No active contexts found. Starting clean.")
                
            return active_contexts
        except Exception as e:
            logger.error(f"♻️ [Rehydration] Failed to fetch active contexts: {e}")
            return []

    async def update_context_state(self, signal_id: str, new_state: str):
        """Updates or closes a monitoring context."""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "UPDATE monitoring_contexts SET state = ?, updated_at = CURRENT_TIMESTAMP WHERE signal_id = ?;",
                    (new_state, signal_id)
                )
                await db.commit()
                logger.info(f"♻️ [Rehydration] Updated signal {signal_id} state to {new_state}.")
        except Exception as e:
            logger.error(f"♻️ [Rehydration] Failed to update context state for {signal_id}: {e}")
