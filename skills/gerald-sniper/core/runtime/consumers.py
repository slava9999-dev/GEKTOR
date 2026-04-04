# core/runtime/consumers.py

import asyncio
from typing import Any
from loguru import logger
from pydantic import BaseModel
from core.radar.macro_radar import MacroSignal
from core.scenario.signal_entity import SignalState, TradingSignal

class AdvisoryConsumer:
    """
    [GEKTOR v8.5] Combat Consumer (Human-in-the-loop).
    Processes signals by formatting them for Telegram with tactical keyboards.
    """
    def __init__(self, context):
        self.context = context

    async def process_signal(self, signal: BaseModel):
        if not isinstance(signal, MacroSignal):
            return
            
        # [GEKTOR v8.5] Format and Send Alert
        # This was previously in MacroRadar, now isolated to the Combat context.
        from core.alerts.outbox import telegram_outbox, MessagePriority
        from core.alerts.formatters import format_macro_alert
        
        message, reply_markup = format_macro_alert(signal, include_keyboard=True)
        alert_hash = f"macro_{signal.symbol}_{int(signal.timestamp)}"
        
        telegram_outbox.enqueue(
            message,
            priority=MessagePriority.CRITICAL,
            alert_hash=alert_hash,
            reply_markup=reply_markup
        )
        logger.info(f"🎭 [Advisory] Alert delivered for {signal.symbol} (Context: {self.context.name})")

class ShadowExecutionConsumer:
    """
    [GEKTOR v8.5] Shadow Execution Consumer (Autonomous Forward Testing).
    Automatically enters virtual positions for every detected signal to profile performance.
    """
    def __init__(self, context):
        self.context = context

    async def process_signal(self, signal: BaseModel):
        if not isinstance(signal, MacroSignal):
            return

        logger.info(f"👁️ [Shadow] Auto-executing virtual test for {signal.symbol}...")
        
        # 1. Convert MacroSignal to TradingSignal for the engine
        direction = "LONG" if signal.direction == "LONG_IMPULSE" else "SHORT"
        
        # Use existing Orchestrator logic within the Shadow Context
        # Note: In SHADOW context, orchestrator uses VirtualExchangeAdapter
        ts_signal = TradingSignal(
            symbol=signal.symbol,
            direction=direction,
            entry_price=signal.price,
            detectors=["MACRO_RADAR"],
            radar_score=int(signal.rvol * 10),
            liquidity_tier=signal.liquidity_tier
        )
        
        # 2. Inject into Shadow Orchestrator
        # This will trigger Risk Check -> Execution on Virtual Adapter
        await self.context.orchestrator.put_signal(ts_signal)
        logger.info(f"🧪 [Shadow] Signal injected into virtual flow: {signal.symbol} {direction}")
