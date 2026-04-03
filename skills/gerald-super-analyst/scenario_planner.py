import asyncio
from loguru import logger
from schemas import SignalContext, ArbiterResponse, ScenarioPlannerResponse
from debate_agents import load_prompt, format_signal_data

async def run_scenario_planner(
    llm_router,
    context: SignalContext,
    candles_text: str,
    arbiter_verdict: ArbiterResponse,
    entry_price: float,
    stop_price: float,
    tp1_price: float,
    tp2_price: float
) -> ScenarioPlannerResponse:
    """
    Layer 2: Scenario Planner (Tree of Thoughts)
    Executes ONLY if Arbiter verdict != SKIP
    """
    prompt_template = load_prompt("scenario_planner.txt")
    
    arbiter_str = f"Verdict: {arbiter_verdict.verdict}\nConviction: {arbiter_verdict.conviction}/10"
    
    prompt = prompt_template.replace("{symbol}", context.symbol)
    prompt = prompt.replace("{direction}", context.direction)
    prompt = prompt.replace("{entry_price}", str(entry_price))
    prompt = prompt.replace("{stop_price}", str(stop_price))
    prompt = prompt.replace("{tp1_price}", str(tp1_price))
    prompt = prompt.replace("{tp2_price}", str(tp2_price))
    prompt = prompt.replace("{btc_trend}", context.btc_context.trend)
    prompt = prompt.replace("{btc_4h}", str(context.btc_context.change_4h))
    prompt = prompt.replace("{atr_pct}", str(context.atr_pct))
    
    logger.info(f"Running Scenario Planner for {context.symbol}")
    try:
        response = await asyncio.wait_for(
            llm_router.generate_structured(
                prompt=prompt,
                response_model=ScenarioPlannerResponse,
                task_type="analysis"
            ),
            timeout=15.0
        )
        return response
    except asyncio.TimeoutError:
        logger.error("LLM timeout for Scenario Planner")
        return None
    except Exception as e:
        logger.error(f"LLM error for Scenario Planner: {e}")
        return None
