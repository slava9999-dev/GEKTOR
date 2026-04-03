import os
import sys
import asyncio
from loguru import logger
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

try:
    from src.infrastructure.llm.router import SmartRouter
except ImportError:
    print("Error: Could not import SmartRouter. Ensure src.infrastructure.llm.router is available.")
    sys.exit(1)

# Add current dir to pythonpath to avoid hyphen import issues
sys.path.append(os.path.dirname(__file__))
from schemas import SignalContext, LevelContext, BTCContext
from debate_agents import run_bull_agent, run_bear_agent, run_arbiter_agent
from scenario_planner import run_scenario_planner
from quant_tools import format_candles_for_prompt
from analyst import SuperAnalyst

async def main():
    llm_config = {
        "sanitizer": False,
        "providers": {
            "gpt4o_mini": {
                "enabled": True,
                "api_key_env": "OPENROUTER_API_KEY",
                "base_url": "https://openrouter.ai/api/v1",
                "model": "openai/gpt-4o-mini",
                "rpm": 60,
                "supports_strict_schema": False,
                "supports_json_object": True
            }
        },
        "routing": {
            "simple": {"primary": "gpt4o_mini", "fallback": "gpt4o_mini", "emergency": "gpt4o_mini"},
            "analysis": {"primary": "gpt4o_mini", "fallback": "gpt4o_mini", "emergency": "gpt4o_mini"}
        }
    }
    
    router = SmartRouter(llm_config)
    
    # Fake Signal - Good Context to trigger TAKE
    signal = SignalContext(
        symbol="CYSUSDT",
        direction="LONG",
        trigger="SQUEEZE_FIRE",
        score=95,
        level=LevelContext(
            price=0.385,
            type="SUPPORT",
            source="KDE+ROUND_NUMBER",
            touches=12,
            distance_pct=0.1
        ),
        btc_context=BTCContext(
            trend="STRONG_UP",
            change_1h=0.5,
            change_4h=2.1
        ),
        candles_m5=[
            {"open": 0.380, "high": 0.386, "low": 0.378, "close": 0.382, "volume": 120000},
            {"open": 0.382, "high": 0.388, "low": 0.381, "close": 0.387, "volume": 150000},
            {"open": 0.387, "high": 0.390, "low": 0.385, "close": 0.388, "volume": 90000},
            {"open": 0.388, "high": 0.395, "low": 0.387, "close": 0.392, "volume": 320000},
            {"open": 0.392, "high": 0.393, "low": 0.386, "close": 0.386, "volume": 280000},
        ],
        candles_m15=[],
        atr_pct=1.5
    )
    
    print("="*50)
    print(" DRY RUN: CYSUSDT LONG (SQUEEZE_FIRE)")
    print("="*50)

    analyst = SuperAnalyst(router)
    plan = await analyst.analyze_signal(signal)
    
    print("\n--- ELITE TRADE PLAN ---")
    print(plan.model_dump_json(indent=2))
    
    print("\n--- TELEGRAM ALERT FORMAT ---")
    print(analyst.format_telegram_alert(plan))
    
    
if __name__ == "__main__":
    asyncio.run(main())
