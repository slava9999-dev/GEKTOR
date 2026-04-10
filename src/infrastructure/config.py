from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional
import os

class Settings(BaseSettings):
    # API & Connection
    ASYNC_DATABASE_URL: str = "postgresql+asyncpg://tekton_admin:password@localhost:5433/tekton_alpha"
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6381
    REDIS_PASSWORD: Optional[str] = None
    
    # Telegram
    TG_BOT_TOKEN: Optional[str] = None
    TG_CHAT_ID: Optional[str] = None
    
    # Infrastructure
    PROXY_POOL: List[str] = []
    BYBIT_API_KEY: str = ""
    BYBIT_API_SECRET: str = ""
    BYBIT_TESTNET: bool = False
    
    # Logic Thresholds
    VPIN_BUCKET_VOLUME: float = 1_000_000.0 # Dollar Bars base
    SENTIMENT_MAX_WEIGHT: float = 0.15      # 15% Max for Hysteresis
    TG_QUEUE_MAX_SIZE: int = 100
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
