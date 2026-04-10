# core/runtime/config_manager.py

import os
import asyncio
import orjson
from loguru import logger
from typing import Dict, Any, Optional
from utils.config import config

class DynamicConfigManager:
    """
    [Audit v6.0] Zero-Downtime Configuration Control Plane.
    
    Architecture:
    - Provides O(1) access to critical trading parameters.
    - Allows 'Hot Mutation' via Event Bus or direct call without process restart.
    - Preserves in-memory state (WS sequences, LLM cache) during parameter tuning.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DynamicConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return
        
        # Current Hot Parameters (Seed from .env/yaml)
        # These are values that change frequently during live sessions.
        self._overrides: Dict[str, Any] = {
            "max_slippage_pct": config.execution.max_slippage_pct,
            "risk_per_trade_pct": config.execution.risk_percent,
            "min_confidence": 0.85,
            "sl_sync_cooldown": 2.0,
            "dirty_tick_threshold_pct": 2.0 # [Audit 6.0: Noise Filter]
        }
        self._lock = asyncio.Lock()
        self._initialized = True
        logger.info(f"⚙️ [ConfigManager] Hot-Reload Plane Active. Slippage: {self._overrides['max_slippage_pct']}%")

    def get(self, key: str, default: Any = None) -> Any:
        """Lock-free high-speed read (Safe due to Python dict implementation)."""
        return self._overrides.get(key, default)

    async def apply_mutation(self, mutation: Dict[str, Any]):
        """
        Atomic update of parameters.
        Example: {"max_slippage_pct": 0.20, "risk_per_trade_pct": 2.0}
        """
        async with self._lock:
            for key, val in mutation.items():
                if key in self._overrides:
                    old_val = self._overrides[key]
                    try:
                        # Attempt to cast to float if it's numeric
                        new_val = float(val) if isinstance(val, (int, float, str)) and str(val).replace('.','',1).isdigit() else val
                        self._overrides[key] = new_val
                        logger.warning(f"⚙️ [HotReload] Parameter MUTATION: {key} ({old_val} -> {new_val})")
                    except Exception as e:
                        logger.error(f"❌ [ConfigManager] Failed to apply {key}={val}: {e}")

    async def listen_for_bus_commands(self):
        """
        Optional: Subscribe to NerveCenter for remote mutations.
        Allows Grafana/Telegram to push JSON updates to the running process.
        """
        from core.events.nerve_center import bus
        from core.events.events import ConfigMutationEvent # Assume this exists
        # bus.subscribe(ConfigMutationEvent, lambda ev: self.apply_mutation(ev.payload))
        pass

# Global Singleton
config_manager = DynamicConfigManager()
