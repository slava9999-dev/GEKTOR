import os
import asyncio
from typing import Type, Optional, Dict
from pydantic import BaseModel
from loguru import logger

from .providers.base_provider import LLMProvider, ProviderError, RateLimitExceeded
from .providers.openai_compat import OpenAICompatProvider
from .providers.gemini_provider import GeminiProvider
try:
    from .providers.local_provider import LocalProvider
except ImportError:
    LocalProvider = None
from .security.sanitizer import SecureContextSanitizer

class SmartRouter:
    """
    Tiered routing architecture for LLMs.
    Prioritizes Gemini/DeepSeek for complex tasks, with local/openai fallbacks.
    """

    def __init__(self, config: dict):
        self.sanitizer = SecureContextSanitizer()
        self.sanitizer_enabled = config.get("sanitizer", True)
        self.providers: Dict[str, LLMProvider] = {}
        self.routing = config.get("routing", {})
        
        # [NERVE REPAIR v4.5] Local Inference Guard
        # Prevents CUDA Out of Memory (OOM) by ensuring only 1 local request runs at a time.
        self._local_semaphore = asyncio.Semaphore(1)
        self._local_active = False

        self._init_providers(config.get("providers", {}))

    def _init_providers(self, pconfs: dict):
        """Build provider instances based on config logic."""
        for name, pconf in pconfs.items():
            if not pconf.get("enabled", False):
                continue

            if name == "local":
                if LocalProvider:
                    self.providers[name] = LocalProvider(pconf)
                else:
                    logger.warning("LocalProvider cannot be enabled: dependencies (llama-cpp-python) missing.")

            elif name == "gemini":
                api_key_env = pconf.get("api_key_env", "GEMINI_API_KEY")
                actual_key = os.getenv(api_key_env)
                if actual_key:
                    self.providers[name] = GeminiProvider(
                        api_key=actual_key, config=pconf, name=name
                    )
                else:
                    logger.warning(f"Provider {name} enabled, but '{api_key_env}' not found in .env. Skipping.")

            else:
                api_key_env = pconf.get("api_key_env", "")
                actual_key = os.getenv(api_key_env)
                
                # Cloud provider requires API Key
                if actual_key:
                    self.providers[name] = OpenAICompatProvider(
                        api_key=actual_key, config=pconf, name=name
                    )
                else:
                    logger.warning(f"Provider {name} enabled, but '{api_key_env}' not found in .env. Skipping.")

    def _get_route(self, task_type: str) -> list[str]:
        route_config = self.routing.get(task_type, self.routing.get("simple", {}))
        return [
            provider_name
            for key in ["primary", "fallback", "emergency"]
            if (provider_name := route_config.get(key)) in self.providers
        ]

    def _classify_task(self, prompt: str, requires_tools: bool) -> str:
        prompt_lower = prompt.lower()
        deep_keywords = ["проанализируй", "сравни", "почему", "отрефактори", "что думаешь", "найди баг"]
        
        if any(kw in prompt_lower for kw in deep_keywords):
            return "deep"
        if requires_tools:
            return "agent"
        return "simple"

    async def generate_structured(
        self,
        prompt: str,
        response_model: Type[BaseModel],
        system_prompt: str = "Ты — Gerald, персональный ИИ-ассистент.",
        max_tokens: int = 2048,
        task_type: Optional[str] = None,
    ) -> BaseModel:

        requires_tools = response_model.__name__ == "AgentOutput"
        if task_type is None:
            task_type = self._classify_task(prompt, requires_tools)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]

        route = self._get_route(task_type)
        if not route:
            raise RuntimeError("CRITICAL: No active LLM providers configured in routes!")
            
        logger.debug(f"Router | Task: '{task_type}' | Route: {route}")

        last_error = None

        for idx, provider_name in enumerate(route):
            provider = self.providers[provider_name]
            is_local = LocalProvider and isinstance(provider, LocalProvider)
            cloud = not is_local

            # Only sanitize cloud payloads
            send_messages = messages
            if cloud and self.sanitizer_enabled:
                send_messages = self.sanitizer.sanitize_messages(messages)

            status = provider.get_rate_limit_status()
            if status.get("remaining_rpm", float('inf')) <= 0:
                logger.debug(f"Router | Skipped {provider_name} due to RPM exhaustion.")
                continue

            import time
            wait_start = time.time()
            try:
                # [NERVE REPAIR v4.5] VRAM Concurrency Isolation with Signal TTL (30s)
                if is_local:
                    async with self._local_semaphore:
                        wait_time = time.time() - wait_start
                        if wait_time > 30.0:
                            logger.error(f"⏳ [Router] Local Task DECAYED (Waited {wait_time:.1f}s > 30s). Rejecting.")
                            raise ProviderError(f"VRAM Queue Timeout: Signal is stale (Waited {wait_time:.1f}s)")

                        logger.debug(f"Router | VRAM Locked. Queue wait: {wait_time:.3f}s. Serving request locally...")
                        result = await provider.generate_structured(
                            messages=send_messages,
                            response_model=response_model,
                            max_tokens=min(max_tokens, 1024),
                        )
                else:
                    # Cloud calls are parallel
                    result = await provider.generate_structured(
                        messages=send_messages,
                        response_model=response_model,
                        max_tokens=max_tokens,
                    )

                # VRAM Memory Management
                if self._local_active and cloud:
                    if "local" in self.providers:
                        logger.info("Router | Cloud provider recovered. Unloading Local Fallback VRAM.")
                        self.providers["local"].unload()
                    self._local_active = False

                if is_local:
                    self._local_active = True

                logger.info(f"Router | Served by [{provider_name}] for task '{task_type}'")
                return result

            except RateLimitExceeded as e:
                logger.warning(f"Router | {provider_name} RateLimited: {e}")
                last_error = e
                continue
            except ProviderError as e:
                logger.error(f"Router | {provider_name} Error: {e}")
                last_error = e
                continue
            except Exception as e:
                logger.error(f"Router | Critical Exception on {provider_name}: {e}")
                last_error = e
                continue

        raise RuntimeError(f"All providers in route '{route}' failed. Last Error: {last_error}")
