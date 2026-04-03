from datetime import datetime, timedelta
from typing import Dict, Optional


COOLDOWN_DURATIONS: Dict[str, int] = {
    "radar_update":      15,
    "level_armed":       120,  # v4.1: raised from 30 to 120 min
    "level_proximity":   60,   # v4.1: raised from 20 to 60 min
    "trigger_fired":     60,   # 60 min
    "momentum_breakout": 30,   # 30 min (New Signal Type)
    "system_degraded":   10,
    "system_recovered":  10,
}


class CooldownStore:
    """
    Manages temporal alert suppression.
    Avoids signal spam while tracking multi-source cooldowns.
    """
    def __init__(self):
        # key -> expiration_datetime
        self._store: Dict[str, datetime] = {}

    def _key(
        self,
        alert_type: str,
        symbol: Optional[str],
        level_id: Optional[str]
    ) -> str:
        return f"{alert_type}|{symbol or 'global'}|{level_id or 'none'}"

    def is_active(
        self,
        alert_type: str,
        symbol: Optional[str] = None,
        level_id: Optional[str] = None
    ) -> bool:
        key = self._key(alert_type, symbol, level_id)
        expires = self._store.get(key)
        if expires is None:
            return False
        return datetime.utcnow() < expires

    def set(
        self,
        alert_type: str,
        symbol: Optional[str] = None,
        level_id: Optional[str] = None,
        minutes: Optional[int] = None
    ) -> None:
        key = self._key(alert_type, symbol, level_id)
        duration = minutes or COOLDOWN_DURATIONS.get(alert_type, 15)
        self._store[key] = datetime.utcnow() + timedelta(minutes=duration)

    def cleanup(self) -> None:
        """Removes expired cooldown entries from memory."""
        now = datetime.utcnow()
        expired_keys = [k for k, v in self._store.items() if v < now]
        for k in expired_keys:
            del self._store[k]

    def get_all_active(self) -> Dict[str, datetime]:
        """Returns all currently active cooldowns."""
        self.cleanup()
        return self._store.copy()

# Global Instance for runtime management
cooldown_store = CooldownStore()
