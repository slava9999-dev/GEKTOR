from dataclasses import dataclass
from enum import StrEnum
from datetime import datetime
from typing import Dict, Any


class StructureType(StrEnum):
    COMPRESSION = "compression"
    SWEEP       = "sweep"
    BREAK       = "break"
    RETEST      = "retest"
    IMPULSE     = "impulse"


@dataclass
class StructureEvent:
    symbol: str
    timeframe: str
    type: StructureType
    level_id: str
    level_price: float
    candle_ts: datetime
    strength: float           # 0.0 – 1.0 confidence score
    extra: Dict[str, Any]     # Details for logging/debugging
