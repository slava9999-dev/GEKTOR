import os
import json
import time
from src.shared.logger import logger
from src.infrastructure.llm.llama_engine import LlamaEngine
from src.domain.entities.agent_output import AgentOutput, ToolResult
from src.infrastructure.database.vector_db import VectorDatabase
from src.infrastructure.database.session_db import SessionDatabase
from src.infrastructure.sandbox.python_executor import PythonExecutor
from src.infrastructure.cache.semantic_cache import SemanticCache
from src.infrastructure.llm.reranker import Reranker

from src.application.services.summarizer import HistorySummarizer
from src.shared.monitoring import PerformanceMonitor
from src.application.tools.gerald_tools import TOOL_REGISTRY, TOOL_DESCRIPTIONS


class GeraldAgent:
    """
    Core Orchestrator for Gerald-SuperBrain V2.0.
    """

    def __init__(self, router, session_id: str = "gerald-default"):
        self.llm = router
        self.session_id = session_id
        self.python_sandbox = PythonExecutor()
        self.vector_db = VectorDatabase()
        self.session_db = SessionDatabase()
        self.cache = SemanticCache()
        self.reranker = Reranker()
        self.summarizer = HistorySummarizer(self.llm)
        self.monitor = PerformanceMonitor()
        
        import asyncio
        self._chat_lock = asyncio.Lock()

        # Persistence: Load history from DB
        self.history = self.session_db.load_history(
            session_id, limit=20
        )  # Load more to allow summarization
        self.summary = ""  # Rolling summary of old messages
        self.max_history = 10

        # Safe context window for dual model interaction
        self.context_window = 128000  # Vastly increased context window thanks to Cloud

        self.system_prompt = (
            "### GERALD_SYSTEM_MODE: ELITE_QUANT_V2\n"
            "You are GERALD, a purely logical, highly aggressive Lead Quant Analyst operating an HFT crypto terminal. "
            "Your task is to analyze market anomalies using strict PostgreSQL queries via the `execute_sql` tool.\n\n"
            
            "CRITICAL DATABASE SCHEMA CONSTANTS:\n"
            "Table Name: `watchlist_history` (or `radar_metrics`)\n"
            "Columns:\n"
            "- `timestamp` (TIMESTAMPTZ): Order by this to get latest data\n"
            "- `symbol` (TEXT): e.g., 'BTCUSDT'\n"
            "- `score` (INTEGER): Alpha score 0-100 (NEVER use 'scoring')\n"
            "- `volume_spike` (FLOAT): Volume anomaly multiplier (> 1.5 is significant)\n"
            "- `velocity` (FLOAT): Price velocity (velocity of trend)\n"
            "- `momentum_pct` (FLOAT): Momentum percentage\n"
            "- `atr_ratio` (FLOAT): Market ATR Volatility ratio\n"
            "- `orderflow_imbalance` (FLOAT): Orderbook bid/ask ratio (> 1.3 is bullish imbalance)\n\n"

            "STRICT RULES FOR SQL GENERATION:\n"
            "1. NEVER hallucinate column names. Use ONLY the columns listed above.\n"
            "2. ALWAYS use `ORDER BY timestamp DESC` for timely analysis.\n"
            "3. ALWAYS append `LIMIT 20` to queries to preserve context.\n"
            "4. ONLY generate read-only `SELECT` statements.\n"
            "5. TRADING SIGNAL FILTER: Search for signals with `score >= 90 AND volume_spike > 1.5 AND orderflow_imbalance > 1.3`.\n\n"

            "EXAMPLE QUERY:\n"
            "SELECT symbol, score, volume_spike, orderflow_imbalance FROM watchlist_history WHERE score >= 90 ORDER BY timestamp DESC LIMIT 10;\n\n"

            "RESPONSE PROTOCOL:\n"
            "- Respond strictly with analytical logic. No conversational filler. No apologies.\n"
            "- If data is missing, request it using tools immediately.\n"
            "- Use 'final_answer(answer)' on Russian (РУССКОМ) language for final delivery."
        )

    def preload_models(self):
        """
        Preloads SentenceTransformers / Reranker into GPU memory asynchronously
        so the first message doesn't block the event loop for 15 seconds.
        """
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        def _warmup():
            self.cache._get_embedder()
            self.reranker._get_model()
            logger.info("🚀 RAG Memory Models preloaded into VRAM successfully.")
        
        loop.run_in_executor(None, _warmup)

    async def chat(self, user_input: str):
        async with self._chat_lock:
            start_time = time.time()
            logger.info(f"User > {user_input}")
    
            # 0. Semantic Cache Check
            cached_response = self.cache.get(user_input)
            if cached_response:
                self.history.append({"role": "user", "content": user_input})
                self.history.append({"role": "assistant", "content": cached_response})
                # We don't save cache hits to DB again to avoid duplication,
                # but we could if we wanted exact session logs.
                return cached_response
    
            # 1. RAG Retrieve
            context_docs = await self.vector_db.search(user_input, limit=10)
    
            seen_texts = set()
            unique_docs = []
            for d in context_docs:
                if d["text"] not in seen_texts:
                    seen_texts.add(d["text"])
                    unique_docs.append(d)
    
            top_docs = self.reranker.rerank(user_input, unique_docs, top_n=3)
            retrieved_context = "\n".join(
                [f"Source {d['filepath']}: {d['text']}" for d in top_docs]
            )
    
            # Save user message to history
            self.history.append({"role": "user", "content": user_input})
            self.session_db.save_message(self.session_id, "user", user_input)
    
            loop_count = 0
            while loop_count < 10:
                loop_count += 1
                prompt = self._build_prompt(retrieved_context)
    
                try:
                    output: AgentOutput = await self.llm.generate_structured(
                        prompt, response_model=AgentOutput
                    )
    
                    logger.info(f"Gerald Thought [{loop_count}]: {output.thought}")
    
                    # SELF-CRITIQUE / SECOND OPINION
                    # Enabled now that we have Cloud APIs without VRAM restrictions
                    if True and output.tool_name == "final_answer" and loop_count == 1:
                        critique_prompt = (
                            f"### ORIGINAL_PROMPT: {user_input}\n"
                            f"### PROPOSED_ANSWER: {output.tool_args.get('answer')}\n"
                            f"### TASK: Ты - старший архитектор (Система Критики). Проверь предложенный ответ на наличие ошибок, галлюцинаций или пропущенных деталей. "
                            f"Если ответ идеален, верни его без изменений. Если есть ошибки - исправь их и верни улучшенный ответ."
                        )
                        logger.info("⚡ Gerald: Cloud Self-Critique / Reflection initiated...")
                        output = await self.llm.generate_structured(
                            prompt=critique_prompt,
                            response_model=AgentOutput,
                            system_prompt=self.system_prompt,
                            task_type="deep"  # Routes to DeepSeek R1 or highest logic route
                        )
                        logger.info(f"Gerald Post-Critique Thought: {output.thought}")
    
                    if output.tool_name == "final_answer":
                        answer = output.tool_args.get("answer", "")
    
                        # ✨ SOUL CHECK: Reflect before delivery
                        from src.domain.entities.soul_engine import GeraldHeart
    
                        await GeraldHeart.reflect(user_input, answer)
    
                        if (
                            self.history
                            and self.history[-1]["role"] == "assistant"
                            and self.history[-1]["content"] == answer
                        ):
                            return answer
    
                        # Persistence & Cache Set
                        self.history.append({"role": "assistant", "content": answer})
                        self.session_db.save_message(self.session_id, "assistant", answer)
                        self.cache.set(user_input, answer)
    
                        # TRIGGER SUMMARIZATION
                        if len(self.history) > 15:
                            to_summarize = self.history[
                                :-5
                            ]  # Keep last 5 messages as raw history
                            self.summary = await self.summarizer.summarize(to_summarize)
                            self.history = self.history[-5:]  # Truncate active history
    
                        duration = time.time() - start_time
                        self.monitor.log_latency(duration)
    
                        return answer
    
                    # Tool Execution
                    result = await self._execute_tool(output.tool_name, output.tool_args)
                    status = "SUCCESS" if result.success else "FAILURE"
                    raw_feedback = result.output or result.error
                    
                    # Task 3.2: Recursive Data Compression for Local LLM
                    if len(raw_feedback) > 3000:
                        feedback = await self.summarizer.summarize_data(output.tool_name, raw_feedback)
                    else:
                        feedback = f"Tool '{output.tool_name}' result ({status}):\n{raw_feedback}"
    
                    # Feedback loop
                    self.history.append({"role": "system", "content": feedback})
    
                    if len(self.history) > 20:
                        return "⚠️ Слишком длинная цепочка размышлений. Попробуй упростить запрос."
    
                except Exception as e:
                    logger.error(f"Reasoning error: {e}")
                    return f"⚠️ Ошибка ядра: {str(e)}"

    def _build_prompt(self, context: str) -> str:
        prompt = f"### SYSTEM_INSTRUCTIONS:\n{self.system_prompt}\n\n"

        if self.summary:
            prompt += f"### SUMMARY_OF_PAST_CONVERSATION:\n{self.summary}\n\n"

        if context:
            prompt += f"### CONTEXT_FROM_FILES:\n{context}\n\n"

        prompt += "### RECENT_CONVERSATION_HISTORY:\n"
        for msg in self.history:
            role = (
                "USER"
                if msg["role"] == "user"
                else "ASSISTANT" if msg["role"] == "assistant" else "SYSTEM_LOG"
            )
            prompt += f"{role}: {msg['content']}\n"

        prompt += "\n### YOUR_TASK:\n"
        prompt += "Ответь пользователю, используя инструмент final_answer, или воспользуйся другими инструментами, если это необходимо.\n"
        prompt += "GERALD_JSON_OUTPUT:"
        return prompt

    async def _execute_tool(self, name: str, args: dict) -> ToolResult:
        # Check the centralized tool registry first
        if name in TOOL_REGISTRY:
            logger.info(f"Tool call: {name}({json.dumps(args, ensure_ascii=False)[:200]})")
            return await TOOL_REGISTRY[name](args)
        
        # Legacy: python sandbox (kept separate for security isolation)
        if name == "execute_python_code":
            return await self.python_sandbox.execute(args.get("code", ""))

        return ToolResult(success=False, output="", error=f"Unknown tool: {name}")
