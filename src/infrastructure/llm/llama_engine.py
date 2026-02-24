import asyncio
import os
from typing import Any, Dict, Optional, Type
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
                n_threads=os.cpu_count() or 4
            )
            logger.info("Model loaded successfully into VRAM/RAM")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise

    async def generate_structured(
        self, 
        prompt: str, 
        response_model: Type[BaseModel],
        max_tokens: int = 2048
    ) -> Any:
        """
        Generate output constrained by a Pydantic model's JSON schema.
        """
        if self.llm is None:
            self.load_model()

        async with self._lock:
            # llama-cpp-python supports response_format with type='json_object' 
            # and 'schema' for recent versions.
            schema = response_model.model_json_schema()
            
            logger.debug(f"Generating structured response for prompt (len={len(prompt)})")
            
            # Using run_in_executor to not block the event loop
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.llm.create_chat_completion(
                    messages=[{"role": "user", "content": prompt}],
                    response_format={
                        "type": "json_object",
                        "schema": schema,
                    },
                    max_tokens=max_tokens,
                    temperature=config.models["primary"].temperature,
                )
            )
            
            try:
                content = response["choices"][0]["message"]["content"]
                return response_model.model_validate_json(content)
            except Exception as e:
                logger.error(f"Failed to parse model output: {e}")
                logger.debug(f"Raw output: {content}")
                raise

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
