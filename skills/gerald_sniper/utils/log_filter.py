import re

# Паттерны для маскировки чувствительных данных
SENSITIVE_PATTERNS = [
    (re.compile(r'bot\d{8,}:[A-Za-z0-9_-]{30,}'), 'bot***:***TOKEN***'),
    (re.compile(r'Bearer [A-Za-z0-9._-]+'), 'Bearer ***MASKED***'),
    (re.compile(r'(?i)api[_-]?key["\s:=]+["\']?[A-Za-z0-9_-]{10,}'), 'api_key=***MASKED***'),
    (re.compile(r'sk-[A-Za-z0-9]{20,}'), 'sk-***MASKED***'),
]


def mask_sensitive(message: str) -> str:
    """Маскирует токены и API-ключи в строке."""
    result = str(message)
    for pattern, replacement in SENSITIVE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


class SensitiveFilter:
    """
    Loguru filter для автоматической маскировки секретов в логах.
    Применяется ко всем хэндлерам loguru.
    """
    def __call__(self, record):
        record["message"] = mask_sensitive(record["message"])
        return True
