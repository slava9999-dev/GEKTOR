# core/realtime/detectors.py

import time
import asyncio
from collections import deque
from loguru import logger
from typing import List, Dict, Optional, Set
from .market_state import market_state
from .models import SymbolLiveState
from core.events.event_bus import bus
from core.events.events import DetectorEvent

DETECTOR_CONFIGS = {
    "VelocityShock": {
        "threshold": 6.0,
        "warmup_bars": 12,
        "cooldown_sec": 120,
    },
    "Acceleration": {
        "threshold": 0.8,
        "warmup_bars": 6,
        "cooldown_sec": 90,
    },
    "OBImbalance": {
        "threshold": 8.0,
        "min_notional_usd": 50_000,
        "warmup_bars": 5,
        "cooldown_sec": 60,
        "persistence_required": 2,
    },
    "MicroMomentum": {
        "threshold": 0.8,
        "min_trades": 200,
        "warmup_bars": 8,
        "cooldown_sec": 90,
    },
}

class DetectorStateMachine:
    """State machine for each symbol × detector type (Rule 1.3)"""
    
    def __init__(self, symbol: str, detector_type: str, config: dict):
        self.symbol = symbol
        self.detector_type = detector_type
        self.config = config
        self.state = "COLD"  # COLD → WARM → COOLDOWN
        self.warmup_bars = config.get("warmup_bars", 10)
        self.cooldown_sec = config.get("cooldown_sec", 120)
        self.observations = deque(maxlen=self.warmup_bars)
        self.last_trigger_ts = 0
        self.persistence_count = 0

    def feed(self, value: float, direction: str, ts: float) -> bool:
        """Returns True only on valid new trigger based on strict > threshold."""
        self.observations.append(value)
        
        if self.state == "COLD":
            if len(self.observations) >= self.warmup_bars:
                self.state = "WARM"
                logger.debug(f"🔥 [{self.detector_type}] {self.symbol} is now WARM")
            return False
        
        if self.state == "COOLDOWN":
            if ts - self.last_trigger_ts > self.cooldown_sec:
                self.state = "WARM"
            else:
                return False
        
        # Trigger only if WARM and strict > threshold
        threshold = self.config.get("threshold", 0.0)
        if self.state == "WARM" and value > threshold:
            # Special persistence check for OB
            if "persistence_required" in self.config:
                self.persistence_count += 1
                if self.persistence_count < self.config["persistence_required"]:
                    return False
            
            self.state = "COOLDOWN"
            self.last_trigger_ts = ts
            self.persistence_count = 0
            return True
            
        # Reset persistence if condition not met
        self.persistence_count = 0
        return False

class AntiMissSystem:
    def __init__(self, config=None):
        self._states: Dict[str, Dict[str, DetectorStateMachine]] = {}

    def _get_state(self, symbol: str, detector_type: str) -> DetectorStateMachine:
        if symbol not in self._states:
            self._states[symbol] = {}
        if detector_type not in self._states[symbol]:
            cfg = DETECTOR_CONFIGS.get(detector_type, {})
            self._states[symbol][detector_type] = DetectorStateMachine(symbol, detector_type, cfg)
        return self._states[symbol][detector_type]

    async def scan_market(self, symbols: Set[str], ts: float, radar_scores: Dict[str, int] = None, radar_tiers: Dict[str, str] = None) -> List[Dict]:
        """
        Rule 1.3: Scan tracked symbols + Event Bus broadcasting.
        """
        radar_scores = radar_scores or {}
        radar_tiers = radar_tiers or {}
        hits = []
        for symbol in symbols:
            state = market_state.symbols.get(symbol)
            if not state: continue
            
            tier = radar_tiers.get(symbol, "D")

            # 1. MicroMomentum
            try:
                change = state.get_price_change_pct(30)
                trades = state.get_trade_count(30)
                cfg = DETECTOR_CONFIGS["MicroMomentum"]
                if trades >= cfg["min_trades"]:
                    sm = self._get_state(symbol, "MicroMomentum")
                    if sm.feed(abs(change), "LONG" if change > 0 else "SHORT", ts):
                        direction = "LONG" if change > 0 else "SHORT"
                        payload = {"change": change, "trades": trades, "radar_score": radar_scores.get(symbol, 0)}
                        # Publish to Event Bus
                        asyncio.create_task(bus.publish(DetectorEvent(
                            symbol=symbol, detector="MICRO_MOMENTUM", 
                            direction=direction, price=state.current_price, payload=payload,
                            liquidity_tier=tier
                        )))
                        hits.append({"symbol": symbol, "type": "MICRO_MOMENTUM", "direction": direction, "price": state.current_price, "tier": tier, **payload})
            except Exception: pass

            # 2. VelocityShock
            try:
                trades_10s = state.get_trade_count(10)
                trades_2m = state.get_trade_count(120)
                baseline = max(trades_2m / 12.0, 8.0)
                ratio = trades_10s / baseline
                sm = self._get_state(symbol, "VelocityShock")
                if sm.feed(ratio, "LONG", ts):
                    ofi = state.get_orderflow_imbalance(10)
                    direction = "LONG" if ofi >= 1.0 else "SHORT"
                    payload = {"ratio": ratio, "radar_score": radar_scores.get(symbol, 0)}
                    # Publish to Event Bus
                    asyncio.create_task(bus.publish(DetectorEvent(
                        symbol=symbol, detector="VELOCITY_SHOCK",
                        direction=direction, price=state.current_price, payload=payload,
                        liquidity_tier=tier
                    )))
                    hits.append({"symbol": symbol, "type": "VELOCITY_SHOCK", "direction": direction, "price": state.current_price, "tier": tier, **payload})
            except Exception: pass

            # 3. Acceleration
            try:
                accel = state.get_acceleration()
                sm = self._get_state(symbol, "Acceleration")
                if sm.feed(abs(accel), "LONG" if accel > 0 else "SHORT", ts):
                    direction = "LONG" if accel > 0 else "SHORT"
                    payload = {"accel": accel, "radar_score": radar_scores.get(symbol, 0)}
                    # Publish to Event Bus
                    asyncio.create_task(bus.publish(DetectorEvent(
                        symbol=symbol, detector="ACCELERATION",
                        direction=direction, price=state.current_price, payload=payload,
                        liquidity_tier=tier
                    )))
                    hits.append({"symbol": symbol, "type": "ACCELERATION", "direction": direction, "price": state.current_price, "tier": tier, **payload})
            except Exception: pass

            # 4. OB Imbalance
            try:
                bids_usd, asks_usd = state.get_orderbook_imbalance()
                cfg = DETECTOR_CONFIGS["OBImbalance"]
                if min(bids_usd, asks_usd) >= cfg["min_notional_usd"]:
                    ratio = max(bids_usd, asks_usd) / max(min(bids_usd, asks_usd), 1.0)
                    sm = self._get_state(symbol, "OBImbalance")
                    if sm.feed(ratio, "LONG" if bids_usd > asks_usd else "SHORT", ts):
                        direction = "LONG" if bids_usd > asks_usd else "SHORT"
                        payload = {"ratio": ratio, "bids": bids_usd, "asks": asks_usd, "radar_score": radar_scores.get(symbol, 0)}
                        # Publish to Event Bus
                        asyncio.create_task(bus.publish(DetectorEvent(
                            symbol=symbol, detector="ORDERBOOK_IMBALANCE",
                            direction=direction, price=state.current_price, payload=payload,
                            liquidity_tier=tier
                        )))
                        hits.append({"symbol": symbol, "type": "ORDERBOOK_IMBALANCE", "direction": direction, "price": state.current_price, "tier": tier, **payload})
            except Exception: pass

        return hits
