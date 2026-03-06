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

class SignalAnalysisOutput(BaseModel):
    conviction: int
    recommendation: str
    reasoning: str

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
                            "order": ["OpenAI"],
                            "ignore": ["Azure"],
                            "allow_fallbacks": True,
                        },
                        "route": "fallback",
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

            import asyncio
            try:
                result = await asyncio.wait_for(
                    self.router.generate_structured(
                        prompt=prompt,
                        response_model=MarketAnalysisOutput,
                        task_type="simple"
                    ),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning("LLM Analyst timed out (10s), using fallback text.")
                return "🤖 <b>AI RADAR │ NEUTRAL</b>\nРадар перегружен, ИИ-резюме недоступно ⏳"
            
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

    async def analyze_signal(self, symbol: str, level: dict, trigger: dict, score: int, risk_data: dict, btc_ctx: dict) -> dict | None:
        if not self.enabled:
            return None
            
        try:
            prompt = (
                f"Ты — Gerald Sniper, элитный крипто-трейдер.\n"
                f"Твоя задача — проанализировать сетап и дать четкую рекомендацию.\n"
                f"Монета: {symbol}\n"
                f"Направление (Тип): {trigger.get('direction', 'UNKNOWN')}\n"
                f"Паттерн: {trigger.get('pattern', 'UNKNOWN')} (score {score}/100)\n"
                f"Описание: {trigger.get('description', '')}\n"
                f"Уровень: {level.get('price')} (touches {level.get('touches')})\n"
                f"Дистанция до уровня: {level.get('distance_pct')}%\n"
                f"Stop Loss: {risk_data.get('stop_pct')}%\n"
                f"Контекст BTC: {btc_ctx.get('trend')} (1h: {btc_ctx.get('change_1h')}%, 4h: {btc_ctx.get('change_4h')}%)\n\n"
                "Выдай JSON в строгом формате:\n"
                "- conviction: целое число от 1 до 10 (твоя уверенность в сделке, где 10 - верняк)\n"
                "- recommendation: одно из значений: TAKE, WAIT, SKIP\n"
                "- reasoning: 1 очень короткое предложение почему (максимум 100 символов, без воды).\n"
            )

            import asyncio
            try:
                result = await asyncio.wait_for(
                    self.router.generate_structured(
                        prompt=prompt,
                        response_model=SignalAnalysisOutput,
                        task_type="simple"
                    ),
                    timeout=15.0
                )
                return {
                    "conviction": result.conviction,
                    "recommendation": result.recommendation.upper(),
                    "reasoning": result.reasoning
                }
            except asyncio.TimeoutError:
                logger.warning(f"LLM SignalAnalysis timed out for {symbol}.")
                return None
            
        except Exception as e:
            logger.error(f"AI Analyst signal error: {e}")
            return None

analyst = GeraldSniperAnalyst()
