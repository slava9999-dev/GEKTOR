import os
from loguru import logger
from pydantic import BaseModel
import sys

# We assume PROJECT_ROOT is in sys.path from main.py
try:
    from src.infrastructure.llm.router import SmartRouter
except ImportError:
    SmartRouter = None

import os
import sys

# Add skills dir to path so we can import gerald-super-analyst
skills_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if skills_dir not in sys.path:
    sys.path.append(skills_dir)
    
sys.path.append(os.path.join(skills_dir, "gerald-super-analyst"))

try:
    from schemas import SignalContext, LevelContext, BTCContext, RadarV2Context
    from analyst import SuperAnalyst
except ImportError as e:
    logger.error(f"Failed to import SuperAnalyst schemas: {e}")
    SuperAnalyst = None

class MarketAnalysisOutput(BaseModel):
    summary: str
    sentiment: str  # BULLISH, BEARISH, NEUTRAL

class GeraldSniperAnalyst:
    """
    Complex AI Market Analysis using Cloud Router.
    Analyzes the top hot coins from the Radar and provides a smart summary.
    """
    def __init__(self, router=None):
        self.enabled = False
        self.router = router
        
        if self.router is None:
            if SmartRouter is None:
                logger.warning("SmartRouter not found. AI Analyst is disabled.")
                return

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
                    "agent":  {"primary": "gpt4o_mini", "fallback": "gpt4o_mini", "emergency": "gpt4o_mini"},
                    "deep":   {"primary": "gpt4o_mini", "fallback": "gpt4o_mini", "emergency": "gpt4o_mini"}
                }
            }
            self.router = SmartRouter(llm_config)

        # Check if the router has the required LLM methods
        if hasattr(self.router, "generate_structured"):
            self.super_analyst = SuperAnalyst(self.router) if SuperAnalyst else None
            self.enabled = True
            logger.info("🧠 [AI Analyst] Smart Connection established.")
        else:
            logger.warning("⚠️ [AI Analyst] Router provided but lacks LLM methods. AI features limited.")
            self.enabled = False

    def configure(self, router):
        """Re-configures the analyst with a new router (e.g. from ExecutionEngine)."""
        self.router = router
        if hasattr(self.router, "generate_structured"):
            self.super_analyst = SuperAnalyst(self.router) if SuperAnalyst else None
            self.enabled = True
            logger.info("🧠 [AI Analyst] Re-configured with Smart Router.")
        else:
            logger.warning("⚠️ [AI Analyst] Configuration failed: Provided router lacks LLM capabilities.")

    async def analyze_hot_coins(self, top_coins: list) -> str:
        if not self.enabled or not hasattr(self.router, "generate_structured"):
            return ""
            
        try:
            # We construct a prompt analyzing the math stats of the top 5 coins
            prompt = (
                "Ты — Gerald Sniper, элитный ИИ-криптоаналитик.\n"
                "Вот топ монет, которые сейчас пульсируют на радаре по объему и ATR:\n"
            )
            for c in top_coins[:10]:
                m = c['metrics']
                # Radar v2 metrics
                prompt += (
                    f"- {m.symbol}: Score {m.final_score}, "
                    f"Spike {m.volume_spike:.1f}x, "
                    f"Velocity {m.velocity:.1f}t/m, "
                    f"Mom {m.momentum_pct:.1f}%, "
                    f"ATR Ratio {m.atr_ratio:.2f}\n"
                )
            
            prompt += (
                "\nНапиши ОЧЕНЬ КОРОТКОЕ (1-2 предложения) резюме для трейдера в Telegram.\n"
                "Оцени общее состояние рынка по этим аномалиям. "
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

    async def analyze_signal(self, symbol: str, level: dict, trigger: dict, score: int, direction: str, sym_data: dict, btc_ctx: dict, alert_id: int | None):
        if not self.enabled or not self.super_analyst:
            return None, None
            
        try:
            radar_v2 = sym_data.get('radar')  # This should be RadarV2Metrics
            atr_pct = 2.0  # Default fallback
            radar_ctx = None
            
            if radar_v2:
                # Calculate approximate ATR % for context if needed, though atr_ratio is better
                # We can use a default or try to get it from candles
                radar_ctx = RadarV2Context(
                    volume_spike=radar_v2.volume_spike,
                    velocity=radar_v2.velocity,
                    momentum_pct=radar_v2.momentum_pct,
                    atr_ratio=radar_v2.atr_ratio,
                    final_score=radar_v2.final_score
                )
                # For backward compatibility in SignalContext.atr_pct
                # We can estimate it or just use a fixed value if it's not critical
                # Actually, RadarV2Metrics doesn't have raw atr_pct anymore
                # But we can pass atr_ratio * normal_atr if we had it.
                # Let's keep it simple for now.
            
            context = SignalContext(
                symbol=symbol,
                direction=direction,
                trigger=trigger.get('pattern', 'UNKNOWN'),
                score=score,
                level=LevelContext(
                    price=level.get('price'),
                    type=level.get('type'),
                    source=level.get('source', 'UNKNOWN'),
                    touches=level.get('touches', 1),
                    distance_pct=level.get('distance_pct', 0.0)
                ),
                btc_context=BTCContext(
                    trend=btc_ctx.get('trend', 'FLAT'),
                    change_1h=btc_ctx.get('change_1h', 0.0),
                    change_4h=btc_ctx.get('change_4h', 0.0)
                ),
                radar=radar_ctx,
                candles_m5=sym_data.get('m5', []),
                candles_m15=sym_data.get('m15', []),
                atr_pct=atr_pct
            )
            
            plan = await self.super_analyst.analyze_signal(context)
            if not plan:
                return None, None
                
            reply_msg = self.super_analyst.format_telegram_alert(plan)
            
            markup = None
            if alert_id and plan.verdict in ("TAKE", "WAIT"):
                markup = {
                    "inline_keyboard": [
                        [
                            {"text": "✅ TAKE PLAN", "callback_data": f"/take_{alert_id}"},
                            {"text": "❌ SKIP", "callback_data": f"/skip_{alert_id}"}
                        ]
                    ]
                }
            return reply_msg, markup
            
        except Exception as e:
            logger.error(f"AI Analyst signal error: {e}")
            return None, None

analyst = GeraldSniperAnalyst()
