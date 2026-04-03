import os
import asyncio
from pathlib import Path
from loguru import logger
from schemas import SignalContext, BullResponse, BearResponse, ArbiterResponse

PROMPTS_DIR = Path(__file__).parent / "prompts"

def load_prompt(filename: str) -> str:
    path = PROMPTS_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def format_signal_data(context: SignalContext, candles_text: str) -> str:
    radar_str = "N/A"
    if context.radar:
        radar_str = (
            f"Score: {context.radar.final_score}/100, "
            f"VolSpike: {context.radar.volume_spike:.1f}x, "
            f"TradeVelocity: {context.radar.velocity:.1f}t/m, "
            f"Momentum: {context.radar.momentum_pct:.1f}%, "
            f"ATR_Ratio: {context.radar.atr_ratio:.2f}"
        )

    return f"""Symbol: {context.symbol}
Direction: {context.direction}
Trigger: {context.trigger}
Score: {context.score}
Level: {context.level.type} @ {context.level.price} ({context.level.source}, {context.level.touches} touches, {context.level.distance_pct}%)
BTC: {context.btc_context.trend} (1h: {context.btc_context.change_1h}%, 4h: {context.btc_context.change_4h}%)
Radar (v2): {radar_str}
ATR_Context: {context.atr_pct}%

Recent Candles:
{candles_text}
"""

async def run_bull_agent(llm_router, context: SignalContext, candles_text: str, stop: float, tp1: float) -> BullResponse:
    prompt_template = load_prompt("bull_agent.txt")
    signal_data = format_signal_data(context, candles_text)
    prompt = prompt_template.replace("{signal_data}", signal_data)
    prompt = prompt.replace("{direction}", context.direction)
    prompt = prompt.replace("{symbol}", context.symbol)
    prompt = prompt.replace("{entry}", str(context.level.price))
    prompt = prompt.replace("{level}", str(context.level.price))
    prompt = prompt.replace("{stop}", str(stop))
    prompt = prompt.replace("{tp1}", str(tp1))
    
    logger.info(f"Running Bull Agent for {context.symbol}")
    try:
        response = await asyncio.wait_for(
            llm_router.generate_structured(
                prompt=prompt,
                response_model=BullResponse,
                task_type="simple"
            ),
            timeout=15.0
        )
        return response
    except asyncio.TimeoutError:
        logger.error("LLM timeout for Bull Agent")
        return BullResponse(pros=["LLM Timeout"], best_case="Unknown due to timeout", confidence=1)
    except Exception as e:
        logger.error(f"LLM error for Bull Agent: {e}")
        return BullResponse(pros=[f"Error: {e}"], best_case="Unknown", confidence=1)

async def run_bear_agent(llm_router, context: SignalContext, candles_text: str, stop: float, tp1: float) -> BearResponse:
    prompt_template = load_prompt("bear_agent.txt")
    signal_data = format_signal_data(context, candles_text)
    prompt = prompt_template.replace("{signal_data}", signal_data)
    prompt = prompt.replace("{direction}", context.direction)
    prompt = prompt.replace("{symbol}", context.symbol)
    prompt = prompt.replace("{entry}", str(context.level.price))
    prompt = prompt.replace("{level}", str(context.level.price))
    prompt = prompt.replace("{stop}", str(stop))
    prompt = prompt.replace("{tp1}", str(tp1))
    
    logger.info(f"Running Bear Agent for {context.symbol}")
    try:
        response = await asyncio.wait_for(
            llm_router.generate_structured(
                prompt=prompt,
                response_model=BearResponse,
                task_type="simple"
            ),
            timeout=15.0
        )
        return response
    except asyncio.TimeoutError:
        logger.error("LLM timeout for Bear Agent")
        return BearResponse(risks=["LLM Timeout"], worst_case="Unknown due to timeout", confidence=10)
    except Exception as e:
        logger.error(f"LLM error for Bear Agent: {e}")
        return BearResponse(risks=[f"Error: {e}"], worst_case="Unknown", confidence=10)

async def run_arbiter_agent(
    llm_router, 
    context: SignalContext,
    candles_text: str,
    bull_case: BullResponse, 
    bear_case: BearResponse
) -> ArbiterResponse:
    prompt_template = load_prompt("arbiter.txt")
    signal_data = format_signal_data(context, candles_text)
    
    bull_case_str = f"Confidence: {bull_case.confidence}/10\nPros: {', '.join(bull_case.pros)}\nBest case: {bull_case.best_case}"
    bear_case_str = f"Confidence: {bear_case.confidence}/10\nRisks: {', '.join(bear_case.risks)}\nWorst case: {bear_case.worst_case}"
    
    prompt = prompt_template.replace("{signal_data}", signal_data)
    prompt = prompt.replace("{bull_case}", bull_case_str)
    prompt = prompt.replace("{bear_case}", bear_case_str)
    
    logger.info(f"Running Arbiter for {context.symbol}")
    try:
        response = await asyncio.wait_for(
            llm_router.generate_structured(
                prompt=prompt,
                response_model=ArbiterResponse,
                task_type="analysis"
            ),
            timeout=15.0
        )
        return response
    except asyncio.TimeoutError:
        logger.error("LLM timeout for Arbiter Agent")
        return ArbiterResponse(verdict="SKIP", conviction=1, primary_reason="LLM Timeout", hidden_stop_hint="Unknown")
    except Exception as e:
        logger.error(f"LLM error for Arbiter Agent: {e}")
        return ArbiterResponse(verdict="SKIP", conviction=1, primary_reason=f"LLM Error: {e}", hidden_stop_hint="Unknown")
