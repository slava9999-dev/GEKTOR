import httpx
import json
import time
from typing import Type, Dict, Any
from pydantic import BaseModel, ValidationError
from loguru import logger
from .base_provider import LLMProvider, ProviderError, RateLimitExceeded

class OpenAICompatProvider(LLMProvider):
    """
    A unified LLM Provider for all OpenAI-compatible endpoints.
    Handles DeepSeek, Groq, OpenRouter, Together, and standard OpenAI.
    """

    def __init__(self, api_key: str, config: dict, name: str = "openai_compat"):
        self.name = name
        self.api_key = api_key
        self.config = config
        self.base_url = config.get("base_url", "https://api.openai.com/v1")
        self.model = config.get("model", "gpt-3.5-turbo")
        
        # OpenRouter/GPT-4o-mini supports strict json_schema output
        self.supports_strict_schema = config.get("supports_strict_schema", False)
        # DeepSeek/Groq supports json_object (requires system prompt tweak)
        self.supports_json_object = config.get("supports_json_object", True)

        self.rpm_limit = config.get("rpm", 30)
        self._request_timestamps = []

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        
        # Append extras (like OpenRouter HTTP-Referer)
        for key, val in config.get("extra_headers", {}).items():
            headers[key] = val

        # Setup proxy
        import os
        proxy_url = os.getenv("PROXY_URL")
        
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(45.0, connect=10.0),
            headers=headers,
            proxy=proxy_url if proxy_url else None
        )

    def _check_rate_limit(self):
        """Simplistic in-memory RPM checker."""
        now = time.monotonic()
        # Clean old
        self._request_timestamps = [t for t in self._request_timestamps if now - t < 60]
        if len(self._request_timestamps) >= self.rpm_limit:
            return False
        return True

    def _record_request(self):
        self._request_timestamps.append(time.monotonic())

    async def generate_structured(
        self,
        messages: list[Dict[str, str]],
        response_model: Type[BaseModel],
        max_tokens: int = 2048,
        temperature: float = 0.1,
    ) -> BaseModel:

        if not self._check_rate_limit():
            raise RateLimitExceeded(f"{self.name}: RPM limit exceeded locally.")

        schema = response_model.model_json_schema()

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        
        for key, val in self.config.get("extra_body", {}).items():
            payload[key] = val

        # Select JSON strategy
        if self.supports_strict_schema:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": response_model.__name__,
                    "strict": True,
                    "schema": schema,
                },
            }
        elif self.supports_json_object:
            payload["response_format"] = {"type": "json_object"}
            schema_str = json.dumps(schema, ensure_ascii=False, indent=2)
            self._inject_schema_hint(messages, schema_str)

        # Retry logic
        for attempt in range(2):
            try:
                self._record_request()
                t0 = time.monotonic()
                
                url = f"{self.base_url}/chat/completions"
                resp = await self.client.post(url, json=payload)
                latency = time.monotonic() - t0

                if resp.status_code == 429:
                    raise RateLimitExceeded(f"{self.name}: 429 Too Many Requests")

                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"]

                # Clean potential markdown wrapping around JSON
                content = self._clean_json(content)

                # Validate exactly
                parsed = response_model.model_validate_json(content)

                usage = data.get("usage", {})
                logger.info(f"[{self.name}] {self.model} | {latency:.2f}s | in:{usage.get('prompt_tokens','?')} out:{usage.get('completion_tokens','?')}")

                return parsed

            except ValidationError as e:
                logger.warning(f"{self.name} Schema failure (attempt {attempt + 1}): {str(e)[:150]}")
                if attempt == 0:
                    messages.append({"role": "assistant", "content": content})
                    messages.append({
                        "role": "user",
                        "content": f"Твой JSON невалиден. Ошибка Pydantic: {str(e)[:300]}. Верни ИСПРАВЛЕННЫЙ JSON. Только JSON."
                    })
                    continue
                raise ProviderError(f"{self.name}: Failed to return valid JSON after 2 attempts.")

            except httpx.TimeoutException:
                raise ProviderError(f"{self.name}: Request timeout")
            except httpx.HTTPStatusError as e:
                raise ProviderError(f"{self.name}: HTTP {e.response.status_code}: {e.response.text}")

    @staticmethod
    def _inject_schema_hint(messages: list[Dict[str, str]], schema_str: str):
        hint = (
            f"\n\n## ОБЯЗАТЕЛЬНЫЙ ФОРМАТ ОТВЕТА\n"
            f"Тебе запрещено писать обычный текст. Верни ТОЛЬКО ОДИН JSON объект, который строго валидируется по этой JSON Schema:\n"
            f"```json\n{schema_str}\n```\n"
            f"Внимание: Выше приведена СХЕМА. Твоя задача — вернуть реальный JSON с заполненными данными, а не копировать схему!"
        )
        for msg in messages:
            if msg["role"] == "system":
                # To prevent multiple injects upon retries or fallbacks
                if "ОБЯЗАТЕЛЬНЫЙ ФОРМАТ ОТВЕТА" not in msg["content"]:
                    msg["content"] += hint
                return
        messages.insert(0, {"role": "system", "content": hint})

    @staticmethod
    def _clean_json(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()
        return text

    async def health_check(self) -> bool:
        try:
            url = f"{self.base_url}/models"
            resp = await self.client.get(url, timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False

    def get_rate_limit_status(self) -> Dict[str, Any]:
        return {
            "rpm_limit": self.rpm_limit,
            "requests_last_minute": len(self._request_timestamps),
            "remaining_rpm": max(0, self.rpm_limit - len(self._request_timestamps))
        }
