import asyncio
from typing import Optional
from loguru import logger

from schemas import SignalContext, EliteTradePlan
from debate_agents import run_bull_agent, run_bear_agent, run_arbiter_agent
from scenario_planner import run_scenario_planner
from quant_tools import format_candles_for_prompt, calculate_stop_loss, calculate_take_profits, validate_rr

class SuperAnalyst:
    """
    Main orchestrator for the Gerald Super Analyst module.
    """
    def __init__(self, llm_router):
        self.llm_router = llm_router
        from debate_agents import run_bull_agent, run_bear_agent, run_arbiter_agent
        from scenario_planner import run_scenario_planner
        self.run_bull = run_bull_agent
        self.run_bear = run_bear_agent
        self.run_arbiter = run_arbiter_agent
        self.run_planner = run_scenario_planner

    async def analyze_signal(self, context: SignalContext) -> EliteTradePlan:
        logger.info(f"Starting Elite Analysis for {context.symbol}")
        
        candles_text = format_candles_for_prompt(context.candles_m5, last_n=5)
        
        # --- LAYER 3 FIRST: Quant ---
        stop = calculate_stop_loss(
            entry=context.level.price,
            direction=context.direction,
            atr=context.atr_pct,
            level=context.level.price
        )

        tp1, tp2 = calculate_take_profits(
            entry=context.level.price,
            stop=stop,
            direction=context.direction
        )

        if not validate_rr(entry=context.level.price, stop=stop, tp1=tp1):
            logger.warning(f"Quant check blocked trade for {context.symbol}: RR < 1.5")
            return EliteTradePlan(
                symbol=context.symbol,
                verdict="BLOCKED",
                conviction=0,
                risk_notes="RR < 1.5",
                bull_summary=None,
                bear_summary=None
            )

        # Set calculated values in context string format for prompts
        context_str = f"stop: {stop}, tp1: {tp1}, tp2: {tp2}" # Note: We pass this via kwargs or just adapt formatting if needed. Let's just use it conceptually if we need. Wait, debate agents use context. Let's just pass them raw or modify context. Since we already changed prompts to expect {stop} and {tp1}, we must modify format_signal_data or pass them.

        # I will fix debate_agents to accept these variables!
        
        # --- LAYER 1: Decision Firewall ---
        bull_case, bear_case = await asyncio.gather(
            self.run_bull(self.llm_router, context, candles_text, stop, tp1),
            self.run_bear(self.llm_router, context, candles_text, stop, tp1)
        )
        
        arbiter_verdict = await self.run_arbiter(
            self.llm_router, context, candles_text, bull_case, bear_case
        )
        
        if arbiter_verdict.verdict == "SKIP":
            logger.info(f"Arbiter decided to SKIP {context.symbol}: {arbiter_verdict.primary_reason}")
            return EliteTradePlan(
                symbol=context.symbol,
                verdict="SKIP",
                conviction=arbiter_verdict.conviction,
                risk_notes=arbiter_verdict.primary_reason,
                bull_summary=bull_case,
                bear_summary=bear_case
            )

        # --- LAYER 2: Scenario Planner ---
        plan = await self.run_planner(
            self.llm_router, context, candles_text, arbiter_verdict,
            entry_price=context.level.price,
            stop_price=stop,
            tp1_price=tp1,
            tp2_price=tp2
        )
        
        if plan is None:
            logger.error("Scenario planner failed to generate a plan.")
            return EliteTradePlan(
                symbol=context.symbol,
                verdict="SKIP",
                conviction=arbiter_verdict.conviction,
                risk_notes="Scenario planner failed.",
                bull_summary=bull_case,
                bear_summary=bear_case
            )

        # Final Successful Plan
        return EliteTradePlan(
            symbol=context.symbol,
            verdict=arbiter_verdict.verdict,
            conviction=arbiter_verdict.conviction,
            risk_notes=arbiter_verdict.primary_reason,
            bull_summary=bull_case,
            bear_summary=bear_case,
            entry_mode="MARKET" if context.level.distance_pct < 0.3 else "LIMIT",
            entry_price=context.level.price,
            stop_loss=stop,
            stop_distance_pct=abs(context.level.price - stop) / context.level.price * 100,
            take_profits=[{"price": tp1, "size_pct": 50, "rr": 1.5}, {"price": tp2, "size_pct": 50, "rr": 3.0}],
            scenarios=plan
        )
        
    def format_telegram_alert(self, plan: EliteTradePlan) -> str:
        """
        Formats the final Elite Trade Plan for Telegram delivery in Russian.
        """
        lines = [
            f"🎩 <b>ELITE TRADE PLAN | {plan.symbol}</b>",
            f"━━━━━━━━━━━━━━━━━━━━━━",
            f"<b>Вердикт:</b> {plan.verdict} ({plan.conviction}/10)\n"
        ]
        
        if plan.verdict in ("SKIP", "BLOCKED") and plan.risk_notes:
            lines.append(f"⚠️ <b>Причина отказа ({plan.symbol}):</b> {plan.risk_notes}\n")
        
        if plan.bull_summary:
            lines.append(f"🟢 <b>Бычий сценарий ({plan.symbol}):</b> {plan.bull_summary.best_case}")
        if plan.bear_summary:
            lines.append(f"🔴 <b>Медвежий сценарий ({plan.symbol}):</b> {plan.bear_summary.worst_case}\n")
            
        if plan.verdict in ("TAKE", "WAIT"):
            lines.extend([
                f"📋 <b>План действий:</b>",
                f"• {plan.entry_mode}: {plan.entry_price}",
                f"• SL: {plan.stop_loss} ({plan.stop_distance_pct}%)"
            ])
            for i, tp in enumerate(plan.take_profits or []):
                lines.append(f"• TP{i+1}: {tp['price']} ({tp['size_pct']}%)")
            
            if plan.scenarios:
                lines.append(f"\n❌ <b>Отмена сделки, если:</b> {plan.scenarios.scenario_c}")
                
        return "\n".join(lines)

