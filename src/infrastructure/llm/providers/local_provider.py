from pydantic import BaseModel
from typing import Type, Dict, Any
from src.infrastructure.llm.llama_engine import LlamaEngine
from .base_provider import LLMProvider

class LocalProvider(LLMProvider):
    """Fallback Local Provider wrapping LlamaEngine."""

    def __init__(self, config: dict):
        self.engine = LlamaEngine()
        # Ensure it loads safely if instantiated. LlamaEngine lazy-loads anyway.
    
    # Языковая директива для Qwen — без неё модель отвечает на китайском
    _LANG_DIRECTIVE = (
        "CRITICAL INSTRUCTION: You MUST respond ONLY in Russian language. "
        "NEVER use Chinese, Japanese, or any non-Russian language. "
        "All thoughts, analysis, and responses must be in Russian.\n\n"
    )

    async def generate_structured(
        self,
        messages: list[Dict[str, str]],
        response_model: Type[BaseModel],
        max_tokens: int = 1024,
        temperature: float = 0.1,
    ) -> BaseModel:
        # Ограничение токенов для локальной модели
        max_tokens = min(max_tokens, 512)

        # Конвертация messages в промпт с языковой директивой
        prompt_lines = []
        for msg in messages:
            content = msg['content']
            if msg['role'] == 'system':
                # Вставляем директиву русского языка в начало system prompt
                content = self._LANG_DIRECTIVE + content
                role = "SYSTEM_INSTRUCTIONS"
            else:
                role = msg['role'].upper()
            prompt_lines.append(f"### {role}:\n{content}\n")

        prompt_lines.append("\n### YOUR_TASK:\nReturn JSON matching AgentOutput. Answer in Russian.")
        prompt_lines.append("GERALD_JSON_OUTPUT:")

        prompt = "\n".join(prompt_lines)

        output = await self.engine.generate_structured(
            prompt=prompt,
            response_model=response_model,
            max_tokens=max_tokens
        )
        return output

    async def health_check(self) -> bool:
        return True # Always "healthy" if hardware exists
        
    def get_rate_limit_status(self) -> Dict[str, Any]:
        return {
            "rpm_limit": float('inf'),
            "requests_last_minute": 0,
            "remaining_rpm": float('inf')
        }
    
    def unload(self):
        """Frees VRAM when cloud providers recover."""
        self.engine.unload()
