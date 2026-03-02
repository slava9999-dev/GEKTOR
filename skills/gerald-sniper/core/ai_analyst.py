import os
from loguru import logger
from pydantic import BaseModel
import sys

# We assume PROJECT_ROOT is in sys.path from main.py
try:
    from src.infrastructure.llm.router import SmartRouter
except ImportError:
    SmartRouter = None

class MarketAnalysisOutput(BaseModel):
    summary: str
    sentiment: str  # BULLISH, BEARISH, NEUTRAL

class GeraldSniperAnalyst:
    """
    Complex AI Market Analysis using Cloud Router.
    Analyzes the top hot coins from the Radar and provides a smart summary.
    """
    def __init__(self):
        self.enabled = False
        if SmartRouter is None:
            logger.warning("SmartRouter not found. AI Analyst is disabled.")
            return

        llm_config = {
            "sanitizer": False,
            "providers": {
                "deepseek": {
                    "enabled": False,  # Disabled: insufficient balance
                    "api_key_env": "DEEPSEEK_API_KEY",
                    "base_url": "https://api.deepseek.com/v1",
                    "model": "deepseek-chat",
                    "rpm": 60,
                    "supports_strict_schema": False,
                    "supports_json_object": True
                },
                "gpt4o_mini": {
                    "enabled": True,
                    "api_key_env": "OPENROUTER_API_KEY",
                    "base_url": "https://openrouter.ai/api/v1",
                    "model": "openai/gpt-4o-mini",
                    "rpm": 60,
                    "supports_strict_schema": False,
                    "supports_json_object": True,
                    "extra_body": {
                        "provider": {
                            "order": ["OpenAI"]  # Bypass Azure jailbreak filters
                        }
                    }
                }
            },
            "routing": {
                "simple": {"primary": "gpt4o_mini", "fallback": "gpt4o_mini", "emergency": "gpt4o_mini"},
                "agent":  {"primary": "gpt4o_mini", "fallback": "gpt4o_mini", "emergency": "gpt4o_mini"},
                "deep":   {"primary": "gpt4o_mini", "fallback": "gpt4o_mini", "emergency": "gpt4o_mini"}
            }
        }
        
        self.router = SmartRouter(llm_config)
        self.enabled = True

    async def analyze_hot_coins(self, top_coins: list) -> str:
        if not self.enabled:
            return ""
            
        try:
            # We construct a prompt analyzing the math stats of the top 5 coins
            prompt = (
                "Ты — Gerald Sniper, элитный ИИ-криптоаналитик.\n"
                "Вот топ монет, которые сейчас пульсируют на радаре по объему и ATR:\n"
            )
            for c in top_coins[:10]:
                prompt += f"- {c['metrics'].symbol}: Score {c['score']:.1f}, Volume {c['metrics'].volume_24h_usd/1e6:.1f}M, ATR {c['metrics'].atr_pct:.1f}%\n"
            
            prompt += (
                "\nНапиши ОЧЕНЬ КОРОТКОЕ (1-2 предложения) резюме для трейдера в Telegram.\n"
                "Укажи, где сейчас максимальный перегрев или интерес. "
                "Используй иконки 💰🔥."
            )

            result = await self.router.generate_structured(
                prompt=prompt,
                response_model=MarketAnalysisOutput,
                task_type="simple"
            )
            
            sentiment_emoji = "🟢" if result.sentiment == "bullish" else "🔴" if result.sentiment == "bearish" else "⚪"
            
            return (
                f"{sentiment_emoji} <b>AI RADAR │ {result.sentiment.upper()}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{result.summary}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Gerald AI 🧠</i>"
            )
            
        except Exception as e:
            logger.error(f"AI Analyst error: {e}")
            return ""

analyst = GeraldSniperAnalyst()
