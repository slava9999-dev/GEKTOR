import abc
from pydantic import BaseModel
from typing import Type, Dict, Optional, Any

class ProviderError(Exception):
    """Network, API or Parse error."""
    pass

class RateLimitExceeded(ProviderError):
    """429 Too Many Requests."""
    pass


class LLMProvider(abc.ABC):
    """Abstract base class for all LLM providers (Cloud & Local)."""

    @abc.abstractmethod
    async def generate_structured(
        self,
        messages: list[Dict[str, str]],
        response_model: Type[BaseModel],
        max_tokens: int = 2048,
        temperature: float = 0.1,
    ) -> BaseModel:
        """Call the API and return a validated Pydantic model."""
        pass

    @abc.abstractmethod
    async def health_check(self) -> bool:
        """Ping the API endpoints to verify connectivity."""
        pass

    @abc.abstractmethod
    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Return dict with remaining requests/tokens."""
        pass
