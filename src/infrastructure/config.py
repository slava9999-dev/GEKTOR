# src/infrastructure/config.py
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from loguru import logger

class Settings(BaseSettings):
    # Telegram (Авто-маппинг через Alias)
    TG_BOT_TOKEN: str = Field(alias="gerald_bot_token", default="")
    TG_CHAT_ID: str = Field(alias="telegram_chat_id", default="")
    
    # Infrastructure (Redis Alias)
    REDIS_HOST: str = Field(alias="redis_host", default="localhost")
    REDIS_PORT: int = Field(alias="redis_port", default=6379)
    REDIS_PASSWORD: Optional[str] = Field(alias="redis_password", default=None)
    
    # Database Alias
    ASYNC_DATABASE_URL: str = Field(alias="async_database_url", default="")
    
    # Proxy
    PROXY_URL: Optional[str] = Field(alias="proxy_url", default=None)
    
    # [GEKTOR APEX] MATH CALIBRATION (INTRADAY v4.1)
    VPIN_WINDOW: int = 50
    VPIN_THRESHOLD: float = 0.65 # Снижено до 0.65 для интрадей-чувствительности
    WARMUP_VOLUME: float = 5_000_000.0
    
    # [GEKTOR APEX] ADAPTIVE VOLUME CLOCKS (Target Vol per Symbol)
    VOLUME_BUCKETS: dict = {
        "BTCUSDT": 10_000_000.0,
        "ETHUSDT": 3_000_000.0,
        "DEFAULT": 1_000_000.0
    }

    # [GEKTOR APEX] EXIT PROTOCOL (PREMISE INVALIDATION)
    EXIT_VPIN_DECAY_FACTOR: float = 0.4  # Сигнал выхода если VPIN упал ниже 40% от входа
    EXIT_CUSUM_REVERSAL_SIGMA: float = 3.0 # Реверс потока в обратную сторону
    EXIT_TIME_MAX_BARS: int = 24  # Максимум 24 адаптивных бара (~2 часа при баре 5 мин)

    # [GEKTOR APEX] MICROSTRUCTURE CALIBRATION (INTRADAY v4.1)
    # Пороги смещены для захвата внутридневных институциональных импульсов
    MICRO_OFI_CONFIG: dict = {
        "MAJORS": {"threshold": 1500.0, "factor": 0.33}, # BTC/ETH
        "ALTS": {"threshold": 500.0, "factor": 0.4}      # SOL, XRP, RAVE
    }
    
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True
    )

    @property
    def bot_token(self) -> str: return self.TG_BOT_TOKEN
    @property
    def chat_id(self) -> str: return self.TG_CHAT_ID
    @property
    def VPIN_BUCKET_VOLUME(self) -> float: 
        return self.VOLUME_BUCKETS.get("DEFAULT", 1_000_000.0)

settings = Settings()
