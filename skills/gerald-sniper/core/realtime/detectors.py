# core/realtime/detectors.py

import time
import asyncio
from collections import deque
from loguru import logger
from typing import List, Dict, Optional, Set
from .market_state import market_state
from .models import SymbolLiveState
from core.events.nerve_center import bus
from core.events.events import DetectorEvent

TIER_MIN_VOLUME_USD = {
    "A": 30_000, # Lowered from 50k
    "B": 10_000, # Lowered from 20k
    "C": 3_000,  # Lowered from 5k
    "D": 1_500   # Lowered from 5k (This was the main block)
}

DETECTOR_CONFIGS = {
    "VelocityShock": {
        "threshold": 4.5, # Softer from 6.0
        "warmup_bars": 12,
        "cooldown_sec": 120,
    },
    "Acceleration": {
        "threshold": 0.5, # Softer from 0.8
        "warmup_bars": 6,
        "cooldown_sec": 90,
    },
    "OBImbalance": {
        "threshold": 2.5, # RADICAL SOFTER FROM 8.0!
        "min_notional_usd": 15_000, # Lowered from 50k
        "warmup_bars": 3, # Faster from 5
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

    def feed(self, value: float, direction: str, ts: float, volume: float = None, min_volume: float = None) -> bool:
        """
        Feed real-time tick value to candidate state machine.
        Implements Fail-Closed Volume Gate (Задача 3.1).
        """
        self.observations.append(value)
        
        # 0. TRANSITION: COLD -> WARM
        if self.state == "COLD":
            if len(self.observations) >= self.warmup_bars:
                self.state = "WARM"
                logger.debug(f"🔥 [{self.detector_type}] {self.symbol} is now WARM")
            return False
        
        # 1. COOLDOWN management
        if self.state == "COOLDOWN":
            if ts - self.last_trigger_ts > self.cooldown_sec:
                self.state = "WARM"
            else:
                return False
        
        # 2. FAIL-CLOSED VOLUME GATE (Task 3.1)
        # We assume volume is invalid UNLESS proven otherwise.
        volume_ok = False
        
        # Strict identification: data must be available AND must be numerical
        try:
            if volume is not None and min_volume is not None:
                v_f = float(volume)
                mv_f = float(min_volume)
                if v_f >= mv_f:
                    volume_ok = True
        except (ValueError, TypeError) as e:
            # Fail-Closed with Telemetry: Log the corruption but keep the gate closed (Rule 5.1)
            logger.debug(f"⚠️ [Detector] Malformed volume data for {self.symbol}: {repr(e)} | volume={volume}, min={min_volume}")
            # volume_ok remains False
            
        # 3. TRIGGER: WARM -> COOLDOWN
        threshold = self.config.get("threshold", 0.0)
        if self.state == "WARM" and value > threshold:
            # All triggers MUST pass Volume Gate (Fail-Closed)
            if not volume_ok:
                if value > threshold * 1.5:
                    logger.debug(f"🛡️ [Fail-Closed] {self.symbol} trigger gated by missing/low volume: {volume}/{min_volume}")
                return False

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
                
                # EPIC 1: Absolute Volume Gate
                vol_usd = state.get_volume_usd(30)
                min_vol = TIER_MIN_VOLUME_USD.get(tier, 10_000)
                
                if trades >= cfg["min_trades"]:
                    sm = self._get_state(symbol, "MicroMomentum")
                    if sm.feed(abs(change), "LONG" if change > 0 else "SHORT", ts, volume=vol_usd, min_volume=min_vol):
                        direction = "LONG" if change > 0 else "SHORT"
                        
                        # EPIC 1: Delta Gate
                        delta_usd = state.get_volume_delta_usd(30)
                        if direction == "LONG" and delta_usd <= 0:
                            logger.debug(f"🔇 [MicroMomentum] {symbol} Rejected LONG! Negative delta ({delta_usd}$)")
                            continue
                        if direction == "SHORT" and delta_usd >= 0:
                            logger.debug(f"🔇 [MicroMomentum] {symbol} Rejected SHORT! Positive delta ({delta_usd}$)")
                            continue
                        
                        payload = {"change": change, "trades": trades, "vol_usd": vol_usd, "delta_usd": delta_usd, "radar_score": radar_scores.get(symbol, 0)}
                        # Publish to Event Bus
                        asyncio.create_task(bus.publish(DetectorEvent(
                            symbol=symbol, detector="MICRO_MOMENTUM", 
                            direction=direction, price=state.current_price, payload=payload,
                            liquidity_tier=tier
                        )))
                        hits.append({"symbol": symbol, "type": "MICRO_MOMENTUM", "direction": direction, "price": state.current_price, "tier": tier, **payload})
            except Exception as e:
                logger.error(f"❌ [Detector] MicroMomentum error on {symbol}: {repr(e)}")

            # 2. VelocityShock
            try:
                trades_10s = state.get_trade_count(10)
                trades_2m = state.get_trade_count(120)
                baseline = max(trades_2m / 12.0, 8.0)
                ratio = trades_10s / baseline
                
                # EPIC 1: Absolute Volume Gate
                vol_usd = state.get_volume_usd(10)
                min_vol = TIER_MIN_VOLUME_USD.get(tier, 10_000)
                
                sm = self._get_state(symbol, "VelocityShock")
                if sm.feed(ratio, "LONG", ts, volume=vol_usd, min_volume=min_vol):
                    ofi = state.get_orderflow_imbalance(10)
                    direction = "LONG" if ofi >= 1.0 else "SHORT"
                    payload = {"ratio": ratio, "vol_usd": vol_usd, "radar_score": radar_scores.get(symbol, 0)}
                    # Publish to Event Bus
                    asyncio.create_task(bus.publish(DetectorEvent(
                        symbol=symbol, detector="VELOCITY_SHOCK",
                        direction=direction, price=state.current_price, payload=payload,
                        liquidity_tier=tier
                    )))
                    hits.append({"symbol": symbol, "type": "VELOCITY_SHOCK", "direction": direction, "price": state.current_price, "tier": tier, **payload})
            except Exception as e:
                logger.error(f"❌ [Detector] VelocityShock error on {symbol}: {repr(e)}")

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
            except Exception as e:
                logger.error(f"❌ [Detector] Acceleration error on {symbol}: {repr(e)}")

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
            except Exception as e:
                logger.error(f"❌ [Detector] OBImbalance error on {symbol}: {repr(e)}")

        return hits

# Global Instance
antimiss_system = AntiMissSystem()
