import copy
import os
import re
from loguru import logger
from typing import List, Dict

class SecureContextSanitizer:
    """Очищает сообщения от чувствительных данных (API ключи, токены) перед отправкой в облако."""

    def __init__(self):
        # Регулярные выражения для поиска секретов
        self.patterns = [
            (re.compile(r'(?i)(telegram[_\-s]?bot[_\-s]?token\s*[:=]\s*["\']?)[a-zA-Z0-9:\-_]{20,}(["\']?)'), r'\1[REDACTED_TG_TOKEN]\2'),
            (re.compile(r'(?i)(api[_\-s]?key\s*[:=]\s*["\']?)[a-zA-Z0-9_\-]{20,}(["\']?)'), r'\1[REDACTED_API_KEY]\2'),
            (re.compile(r'(?i)([a-zA-Z0-9_-]*(?:secret|key|token)[a-zA-Z0-9_-]*\s*[:=]\s*["\']?)[a-zA-Z0-9_\-]{20,}(["\']?)'), r'\1[REDACTED_SECRET]\2'),
        ]
        
        # Дополнительно считываем ключи из .env, чтобы заменять их точные совпадения
        self.exact_secrets = []
        self._load_env_secrets()

    def _load_env_secrets(self):
        try:
            env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), ".env")
            if os.path.exists(env_path):
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            parts = line.split("=", 1)
                            if len(parts) == 2:
                                secret_value = parts[1].strip()
                                # Убираем кавычки
                                if secret_value.startswith('"') and secret_value.endswith('"'):
                                    secret_value = secret_value[1:-1]
                                elif secret_value.startswith("'") and secret_value.endswith("'"):
                                    secret_value = secret_value[1:-1]
                                    
                                # Добавляем только достаточно длинные строки (чтобы не зацензурить случайные слова)
                                if len(secret_value) > 15:
                                    self.exact_secrets.append(secret_value)
        except Exception as e:
            logger.warning(f"Sanitizer could not load .env for exact matching: {e}")

    def sanitize_text(self, text: str) -> str:
        """Очищает одну строку."""
        if not text:
            return text

        # 1. Замена по точным совпадениям из .env
        for secret in self.exact_secrets:
            if secret in text:
                text = text.replace(secret, "[REDACTED_SECRET]")

        # 2. Замена по регуляркам
        for pattern, replacement in self.patterns:
            text = pattern.sub(replacement, text)

        return text

    def sanitize_messages(self, messages: List[Dict]) -> List[Dict]:
        """Очищает список сообщений (клонирует перед мутацией)."""
        safe_messages = copy.deepcopy(messages)
        for msg in safe_messages:
            if 'content' in msg and isinstance(msg['content'], str):
                original_len = len(msg['content'])
                msg['content'] = self.sanitize_text(msg['content'])
        return safe_messages
