import asyncio
import os
from typing import Any, Optional, Type
from pydantic import BaseModel
from llama_cpp import Llama
from src.shared.config import config
from src.shared.logger import logger


class LlamaEngine:
    """
    Core LLM Engine using llama-cpp-python.
    Handles hardware-optimized loading and constrained generation.
    """

    def __init__(self):
        self.model_path = config.models["primary"].path
        self.llm: Optional[Llama] = None
        self._lock = asyncio.Lock()

    def load_model(self):
        if self.llm is not None:
            return

        logger.info(f"Loading model: {self.model_path}")
        try:
            params = config.models["primary"]
            self.llm = Llama(
                model_path=self.model_path,
                n_ctx=params.n_ctx,
                n_gpu_layers=params.n_gpu_layers,
                verbose=False,
                n_threads=os.cpu_count() or 4,
            )
            logger.info("Model loaded successfully into VRAM/RAM")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise

    async def generate_structured(
        self,
        prompt: str,
        response_model: Type[BaseModel],
        model_name: Optional[str] = None,
        max_tokens: int = 2048,
    ) -> Any:
        """
        Generate output constrained by a Pydantic model's JSON schema.
        Supports both local llama-cpp and Ollama.
        """
        # If model_name is provided and looks like an Ollama model, use Ollama
        if model_name and not model_name.endswith(".gguf") and "/" not in model_name:
            return await self._generate_ollama(
                prompt, response_model, model_name, max_tokens
            )

        if self.llm is None:
            self.load_model()

        llm = self.llm
        if not llm:
            raise RuntimeError("LLM not loaded")
        async with self._lock:
            schema = response_model.model_json_schema()
            loop = asyncio.get_event_loop()
            try:
                response: Any = await loop.run_in_executor(
                    None,
                    lambda: llm.create_chat_completion(
                        messages=[{"role": "user", "content": prompt}],
                        response_format={
                            "type": "json_object",
                            "schema": schema,
                        },
                        max_tokens=max_tokens,
                        temperature=config.models["primary"].temperature,
                    ),
                )
                content = response["choices"][0]["message"]["content"]
                return response_model.model_validate_json(content)
            except Exception as e:
                logger.error(f"Llama-cpp failed: {e}. Trying Ollama fallback...")
                return await self._generate_ollama(
                    prompt, response_model, "qwen2.5:7b", max_tokens
                )

    async def _generate_ollama(self, prompt, response_model, model, max_tokens):
        import httpx

        # Ограничение токенов и языковая директива для локальных моделей
        max_tokens = min(max_tokens, 512)
        lang_prefix = (
            "CRITICAL: You MUST respond ONLY in Russian. "
            "NEVER use Chinese or any non-Russian language.\n\n"
        )

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    "http://localhost:11434/api/chat",
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": lang_prefix},
                            {"role": "user", "content": prompt},
                        ],
                        "stream": False,
                        "format": response_model.model_json_schema(),
                        "options": {"num_predict": max_tokens},
                    },
                )
                resp.raise_for_status()
                content = resp.json()["message"]["content"]
                return response_model.model_validate_json(content)
        except Exception as e:
            logger.error(f"Ollama error ({model}): {e}")
            raise

    async def generate(self, prompt: str, max_tokens: int = 2048) -> str:
        """Unstructured text generation."""
        if self.llm is None:
            self.load_model()
        llm = self.llm
        if not llm:
            raise RuntimeError("LLM not loaded")
            
        async with self._lock:
            loop = asyncio.get_event_loop()
            try:
                response: Any = await loop.run_in_executor(
                    None,
                    lambda: llm.create_chat_completion(
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=max_tokens,
                        temperature=config.models["primary"].temperature if config and config.models else 0.7,
                    ),
                )
                return response["choices"][0]["message"]["content"]
            except Exception as e:
                logger.error(f"Text generation failed: {e}")
                return ""

    def unload(self):
        if self.llm:
            logger.info("Unloading model from VRAM")
            del self.llm
            self.llm = None
            import gc

            gc.collect()
            try:
                import torch

                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except ImportError:
                pass
