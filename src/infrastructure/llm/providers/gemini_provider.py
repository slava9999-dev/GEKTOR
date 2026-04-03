import os
import json
import asyncio
from typing import Type, Dict, Optional, Any
from pydantic import BaseModel
import google.generativeai as genai
from loguru import logger

from .base_provider import LLMProvider, ProviderError, RateLimitExceeded

class GeminiProvider(LLMProvider):
    """
    Native Google Gemini provider using google-generativeai SDK.
    Supports system instructions, structured output, and native JSON mode.
    """

    def __init__(self, api_key: str, config: dict, name: str = "gemini"):
        self.name = name
        self.api_key = api_key
        self.config = config
        self.model_name = config.get("model", "gemini-2.0-flash")
        
        # Setup the SDK with PROXY support (Critical for regions with blocks)
        proxy_url = os.getenv("PROXY_URL")
        if proxy_url:
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["HTTPS_PROXY"] = proxy_url
            logger.info(f"Gemini | Proxy enabled: {proxy_url[:30]}...")
            
        genai.configure(api_key=self.api_key)
        
        self.rpm_limit = config.get("rpm", 15)
        self._request_count = 0
        self._last_reset = asyncio.get_event_loop().time()

    async def _handle_rate_limit(self):
        now = asyncio.get_event_loop().time()
        if now - self._last_reset > 60:
            self._request_count = 0
            self._last_reset = now
            
        if self._request_count >= self.rpm_limit:
            wait_time = 60 - (now - self._last_reset)
            if wait_time > 0:
                logger.warning(f"Gemini | RPM Limit hit. Pausing for {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
                self._request_count = 0
                self._last_reset = asyncio.get_event_loop().time()

    async def generate_structured(
        self,
        messages: list[Dict[str, str]],
        response_model: Type[BaseModel],
        max_tokens: int = 4096,
        temperature: float = 0.1,
    ) -> BaseModel:
        await self._handle_rate_limit()
        self._request_count += 1

        # Extract system instruction
        system_instruction = None
        contents = []
        for m in messages:
            if m["role"] == "system":
                system_instruction = m["content"]
            else:
                # Gemini roles: user, model (instead of assistant)
                role = "user" if m["role"] in ["user", "system_log"] else "model"
                contents.append({"role": role, "parts": [m["content"]]})

        raw_schema = response_model.model_json_schema()
        
        # PRO-ARCHITECTURE: Gemini is allergic to 'title' and 'additionalProperties' in JSON Schema
        def _clean_schema_for_gemini(schema_obj):
            if isinstance(schema_obj, dict):
                # Remove Gemini-incompatible keys (Audit 24.5)
                schema_obj.pop("title", None)
                schema_obj.pop("additionalProperties", None)
                
                for key in list(schema_obj.keys()):
                    _clean_schema_for_gemini(schema_obj[key])
            elif isinstance(schema_obj, list):
                for item in schema_obj:
                    _clean_schema_for_gemini(item)
            return schema_obj

        clean_schema = _clean_schema_for_gemini(raw_schema)

        generation_config = {
            "temperature": temperature,
            "max_output_tokens": max_tokens,
            "response_mime_type": "application/json",
            "response_schema": clean_schema
        }

        try:
            model = genai.GenerativeModel(
                model_name=self.model_name,
                system_instruction=system_instruction
            )
            
            # Use run_in_executor because the SDK might have blocking sync calls internally
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: model.generate_content(
                    contents,
                    generation_config=generation_config
                )
            )

            if not response.text:
                raise ProviderError("Gemini returned empty response.")

            # Validate with Pydantic
            try:
                parsed = response_model.model_validate_json(response.text)
                return parsed
            except Exception as e:
                logger.error(f"Gemini | JSON Validation failed: {e}\nRaw: {response.text}")
                # Fallback: try to clean it
                import re
                txt = response.text.replace("```json", "").replace("```", "").strip()
                return response_model.model_validate_json(txt)

        except Exception as e:
            if "429" in str(e):
                raise RateLimitExceeded(f"Gemini 429: {e}")
            raise ProviderError(f"Gemini Error: {e}")

    async def health_check(self) -> bool:
        try:
            model = genai.GenerativeModel(self.model_name)
            # Simple probe
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: model.generate_content("ping", generation_config={"max_output_tokens": 1})
            )
            return True
        except Exception:
            return False

    def get_rate_limit_status(self) -> Dict[str, Any]:
        return {
            "rpm_limit": self.rpm_limit,
            "remaining_rpm": max(0, self.rpm_limit - self._request_count)
        }
