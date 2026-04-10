# core/metrics/reconciler.py

import asyncio
import time
from typing import Dict, List, Optional
from loguru import logger
from dataclasses import dataclass, field

from core.events.nerve_center import bus
from core.events.events import SignalEvent, OrderExecutedEvent

@dataclass
class IntentRecord:
    symbol: str
    direction: str
    signal_price: float
    timestamp: float = field(default_factory=time.time)
    signal_id: str = ""

class RealityReconciler:
    """
    [Audit 6.16] BEHAVIORAL DELTA ENGINE (Human vs Algo).
    Calculates slippage, human reaction time, and exit discipline.
    """
    def __init__(self, db_manager=None):
        self.db = db_manager
        self.pending_intents: List[IntentRecord] = []
        self._max_history = 100
        self._match_window = 15.0 # 15s window to match human action to signal

    async def start(self):
        bus.subscribe(SignalEvent, self._on_signal)
        bus.subscribe(OrderExecutedEvent, self._on_execution)
        logger.info("📐 [RealityCheck] Behavioral Reconciler ACTIVE")

    async def _on_signal(self, event: SignalEvent):
        """Record the 'Intent' of the algorithm."""
        now = time.time()
        # Only track winner signals for performance
        if not event.metadata.get("is_winner", True):
            return
            
        record = IntentRecord(
            symbol=event.symbol,
            direction=event.direction,
            signal_price=event.price,
            timestamp=event.timestamp,
            signal_id=event.signal_id
        )
        self.pending_intents.append(record)
        
        # Keep buffer small
        if len(self.pending_intents) > self._max_history:
            self.pending_intents.pop(0)

    async def _on_execution(self, event: OrderExecutedEvent):
        """Match actual Bybit fulfillment with algorithm intent."""
        now = time.time()
        
        # 1. Match with Signal
        match = None
        for intent in reversed(self.pending_intents):
            age = now - intent.timestamp
            if age > 60: continue # Hard limit for human reaction
            
            if intent.symbol == event.symbol:
                match = intent
                break
        
        # 2. Behavioral Metrics Calculation
        if match:
            latency_ms = int((now - match.timestamp) * 1000)
            # Slippage calculation
            if event.side.upper() == "BUY": # LONG
                slippage_pct = (event.price - match.signal_price) / match.signal_price * 100
            else: # SHORT (Sell)
                slippage_pct = (match.signal_price - event.price) / match.signal_price * 100
                
            logger.info(
                f"📈 [REALITY CHECK] {event.symbol} | "
                f"Reaction: {latency_ms/1000:.2f}s | "
                f"Slippage: {slippage_pct:+.2f}%"
            )
            is_rogue = False
            signal_price = match.signal_price
            signal_id = match.signal_id
            direction = match.direction
        else:
            # rogue Trade detected (Manual entry without signal)
            logger.warning(f"🚨 [ROGUE TRADE] {event.symbol} executed without Signal! (Human FOMO detected)")
            latency_ms = 0
            slippage_pct = 0.0
            is_rogue = True
            signal_price = event.price # No signal price
            signal_id = None
            direction = "LONG" if event.side.lower() == "buy" else "SHORT"

        # 3. Persistence (Gerald v2.2.0: Institutional Grade)
        if self.db:
            try:
                data = {
                    "symbol": event.symbol,
                    "signal_id": signal_id,
                    "direction": direction,
                    "latency_ms": latency_ms,
                    "slippage_pct": slippage_pct,
                    "signal_price": signal_price,
                    "executed_price": event.price,
                    "is_rogue": is_rogue
                }
                await self.db.insert_reality_metric(data)
                
                # Also update global metrics for real-time monitoring
                try:
                    from core.metrics.metrics import metrics
                    metrics.record_slippage(event.symbol, slippage_pct)
                    if not is_rogue:
                        metrics.record_human_latency(latency_ms)
                except: pass
            except Exception as e:
                logger.error(f"❌ [RealityCheck] DB commit failed: {e}")

# Global singleton
reality_reconciler = RealityReconciler()
