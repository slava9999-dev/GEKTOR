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

    async def summarize_data(self, tool_name: str, raw_data: str) -> str:
        """
        Specialized quantitative summarization for tool outputs (SQL/Files).
        Prevents context window overflow in local models (7B range).
        """
        if not raw_data or len(raw_data) < 1500:
            return raw_data

        prompt = (
            "### TASK: QUANTITATIVE SUMMARY\n"
            f"You are the memory compression module for GERALD. Tool '{tool_name}' returned a large raw output. "
            "Compress this into a highly dense, analytical summary. Preserve all critical numbers, symbols, and anomalies. "
            "If it's an SQL result, list only the TOP 3 candidates and a summary of the rest.\n\n"
            f"### RAW_TOOL_DATA:\n{raw_data[:8000]}\n\n"
            "### ANALYTICAL_SUMMARY (Dense, Russian Language):"
        )

        try:
            logger.info(f"⚡ Compressing large tool output from '{tool_name}'...")
            from pydantic import BaseModel
            class DataSummary(BaseModel):
                summary: str

            result = await self.llm.generate_structured(
                prompt=prompt,
                response_model=DataSummary,
                max_tokens=256,
                task_type="simple"
            )
            summary = f"📑 [DENSE_SUMMARY of {tool_name}]: {result.summary}"
            return summary
        except Exception as e:
            logger.error(f"Data summarization failed: {e}")
            return raw_data[:1500] + "\n... [AUTO_TRUNCATED due to volume]"
