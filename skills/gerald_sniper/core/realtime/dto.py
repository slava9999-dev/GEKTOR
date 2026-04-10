# skills/gerald_sniper/core/realtime/dto.py
from pydantic import BaseModel, Field, field_validator
from typing import Literal
from loguru import logger

class SignalDTO(BaseModel):
    """
    [GEKTOR v21.48] Строгий контракт данных сигнала.
    Исключает "мусорные" стейты с нулевым объемом или волатильностью.
    """
    symbol: str = Field(..., min_length=2)
    side: Literal["LONG", "SHORT"]
    vpin: float = Field(..., ge=0.0, le=1.0)
    volume_usd: float = Field(..., gt=0.0)  # Строго больше нуля
    sigma_pct: float = Field(..., ge=0.0)
    accuracy: float = Field(..., ge=0.0, le=1.0)
    price: float = Field(..., gt=0.0)

    @field_validator("volume_usd")
    @classmethod
    def prevent_zero_volume(cls, v):
        if v <= 0.01:
            raise ValueError(f"FATAL: Detected zero or toxic volume: {v}")
        return v

class TelegramFormatter:
    """Узел трансляции сигналов в понятный для оператора формат."""
    
    @staticmethod
    def build_alert(signal: SignalDTO) -> str:
        # Инвариант: сюда дойдет только валидный объект. Нули отсечены Pydantic'ом.
        
        # Элегантное форматирование. Защита от потери микро-сигмы.
        formatted_volatility = (
            f"{signal.sigma_pct:.4f}%" 
            if signal.sigma_pct >= 0.0001 
            else f"{signal.sigma_pct:.6f}% (MICRO)"
        )
        
        vpin_alert = "КРИТИЧЕСКИЙ" if signal.vpin > 0.9 else "ВЫСОКИЙ"
        side_icon = "🟢 LONG (ПОКУПКА)" if signal.side == "LONG" else "🔴 SHORT (ПРОДАЖА)"

        template = (
            f"{side_icon} │ {signal.symbol}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f" VPIN: {signal.vpin:.4f} ({vpin_alert})\n"
            f" Объем: ${signal.volume_usd:,.0f}\n"
            f" Цена: {signal.price:,.2f}\n"
            f" Волатильность: {formatted_volatility}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f" Точность: {signal.accuracy * 100:.1f}%\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"GEKTOR APEX v21.48 │ БОЕВОЙ РЕЖИМ"
        )
        return template
