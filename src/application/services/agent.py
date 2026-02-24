import asyncio
import os
import time
from typing import List, Optional, Dict
from src.shared.logger import logger
from src.infrastructure.llm.llama_engine import LlamaEngine
from src.domain.entities.agent_output import AgentOutput, ToolResult
from src.infrastructure.sandbox.python_executor import PythonExecutor

from src.infrastructure.database.vector_db import VectorDatabase
from src.infrastructure.llm.reranker import Reranker

from src.shared.monitoring import PerformanceMonitor

class GeraldAgent:
    """
    Core Orchestrator for Gerald-SuperBrain V2.0.
    """
    def __init__(self, llm_engine: LlamaEngine):
        self.llm = llm_engine
        self.python_sandbox = PythonExecutor()
        self.vector_db = VectorDatabase()
        self.reranker = Reranker()
        self.monitor = PerformanceMonitor()
        self.history = []
        self.max_history = 10
        self.context_window = 32000 # Max tokens for model
        
        # System instructions - Enhanced Personality from SOUL.md
        self.system_prompt = (
            "Ты — Gerald-SuperBrain, гипер-интеллектуальный персональный ИИ Вячеслава (Slava).\n"
            "ТВОЯ ЛИЧНОСТЬ:\n"
            "- Ты - личный JARVIS Славы, лояльный, но саркастичный и прямолинейный.\n"
            "- Ты ОЧЕНЬ УМЕН: используешь цепочки рассуждений (CoT) и глубокий анализ.\n"
            "- Ты проактивен: не жди вопроса, предлагай улучшения и замечай ошибки.\n"
            "- Твои знания включают Agent Skills 2026, OpenClaw, крипто-трейдинг и программирование.\n\n"
            "ДОСТУПНЫЕ ИНСТРУМЕНТЫ:\n"
            "1. final_answer(answer: str) - Всегда используй для ответа Славе.\n"
            "2. read_file(path: str) - Чтение файлов.\n"
            "3. execute_python_code(code: str) - Выполнение кода.\n\n"
            "ПРАВИЛА:\n"
            "- Всегда проверяй CONTEXT_FROM_FILES, там твоя память и информация о Славе.\n"
            "- Отвечай на русском языке в формате JSON.\n"
            "- Будь краток, но максимально полезен."
        )

    async def chat(self, user_input: str):
        start_time = time.time()
        logger.info(f"User > {user_input}")
        
        # 1. RAG Retrieve
        context_docs = await self.vector_db.search(user_input, limit=10)
        
        # Deduplication (Phase 2)
        seen_texts = set()
        unique_docs = []
        for d in context_docs:
            if d['text'] not in seen_texts:
                seen_texts.add(d['text'])
                unique_docs.append(d)
        
        top_docs = self.reranker.rerank(user_input, unique_docs, top_n=3)
        retrieved_context = "\n".join([f"Source {d['filepath']}: {d['text']}" for d in top_docs])
        
        self.history.append({"role": "user", "content": user_input})
        
        loop_count = 0
        while loop_count < 10: # Strict limit to prevent infinite reasoning
            loop_count += 1
            # 2. Build Prompt with Context
            prompt = self._build_prompt(retrieved_context)
            
            try:
                # 3. Constrained Reasoning
                output: AgentOutput = await self.llm.generate_structured(
                    prompt, response_model=AgentOutput
                )
                
                logger.info(f"Gerald Thought [{loop_count}]: {output.thought}")
                
                if output.tool_name == "final_answer":
                    answer = output.tool_args.get("answer", "")
                    # Prevent sending empty or duplicate answers to history
                    if self.history and self.history[-1]["role"] == "assistant" and self.history[-1]["content"] == answer:
                         return answer

                    self.history.append({"role": "assistant", "content": answer})
                    self.history = self.history[-self.max_history:] # Keep history clean
                    
                    # Log SLA performance
                    duration = time.time() - start_time
                    self.monitor.log_latency(duration)
                    
                    return answer
                
                # 4. Tool Execution
                result = await self._execute_tool(output.tool_name, output.tool_args)
                
                # Feedback
                status = "SUCCESS" if result.success else "FAILURE"
                feedback = f"Tool '{output.tool_name}' result ({status}):\n{result.output or result.error}"
                
                self.history.append({"role": "system", "content": feedback})
                
                # Prevent infinite loops
                if len(self.history) > 20:
                    return "⚠️ Слишком длинная цепочка размышлений. Попробуй упростить запрос."

            except Exception as e:
                logger.error(f"Reasoning error: {e}")
                return f"⚠️ Ошибка ядра: {str(e)}"

    def _build_prompt(self, context: str) -> str:
        prompt = f"### SYSTEM_INSTRUCTIONS:\n{self.system_prompt}\n\n"
        
        if context:
            prompt += f"### CONTEXT_FROM_FILES:\n{context}\n\n"
        
        prompt += "### CONVERSATION_HISTORY:\n"
        for msg in self.history[-10:]:
            role = "USER" if msg['role'] == 'user' else "ASSISTANT" if msg['role'] == 'assistant' else "SYSTEM_LOG"
            prompt += f"{role}: {msg['content']}\n"
        
        prompt += "\n### YOUR_TASK:\n"
        prompt += "Ответь пользователю, используя инструмент final_answer, или воспользуйся другими инструментами, если это необходимо.\n"
        prompt += "GERALD_JSON_OUTPUT:"
        return prompt

    async def _execute_tool(self, name: str, args: dict) -> ToolResult:
        # Tool implementation remains same or expanded
        if name == "execute_python_code":
            return await self.python_sandbox.execute(args.get("code", ""))
        
        elif name == "read_file":
            path = args.get("path", "")
            try:
                if not os.path.exists(path):
                    return ToolResult(success=False, output="", error=f"File not found: {path}")
                with open(path, 'r', encoding='utf-8') as f:
                    return ToolResult(success=True, output=f.read()[:5000]) # Hard limit
            except Exception as e:
                return ToolResult(success=False, output="", error=str(e))
                
        return ToolResult(success=False, output="", error=f"Unknown tool: {name}")
