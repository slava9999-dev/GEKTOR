from typing import List, Dict
from src.shared.logger import logger
from src.infrastructure.llm.llama_engine import LlamaEngine


class HistorySummarizer:
    """Consolidates long conversation history into a concise summary."""

    def __init__(self, llm_engine: LlamaEngine):
        self.llm = llm_engine

    async def summarize(self, history: List[Dict]) -> str:
        if not history:
            return ""

        # Prepare text for summarization
        history_text = ""
        for msg in history:
            role = "User" if msg["role"] == "user" else "Assistant"
            history_text += f"{role}: {msg['content']}\n"

        prompt = (
            "### ТВОЯ ЗАДАЧА:\n"
            "Ты — модуль сжатия памяти Gerald-SuperBrain. Прочитай историю диалога ниже и составь краткую, "
            "но емкую выжимку (summary) на русском языке. Укажи ключевые темы, принятые решения и факты о Славе.\n\n"
            f"### ИСТОРИЯ ДИАЛОГА:\n{history_text}\n\n"
            "### ВЫЖИМКА (SUMMARY):"
        )

        try:
            logger.info("Summarizing long conversation history...")
            from pydantic import BaseModel
            
            class SummaryOutput(BaseModel):
                summary: str

            summary_result = await self.llm.generate_structured(
                prompt=prompt, 
                response_model=SummaryOutput,
                max_tokens=300,
                task_type="simple"
            )
            return summary_result.summary.strip()
        except Exception as e:
            logger.error(f"Summarization failed: {e}")
            return "Ошибка при сжатии истории."
