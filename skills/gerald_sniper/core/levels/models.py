from dataclasses import dataclass, field
from enum import StrEnum
from typing import List, Optional, Any
import hashlib


class LevelSide(StrEnum):
    SUPPORT    = "SUPPORT"
    RESISTANCE = "RESISTANCE"


class LevelSource(StrEnum):
    SWING      = "SWING"
    KDE        = "KDE"
    ROUND      = "ROUND_NUMBER"
    WEEK_EXT   = "WEEK_EXTREME"


@dataclass
class HTFLevel:
    symbol: str
    timeframe: str
    side: LevelSide
    price: float
    touches: int
    source: LevelSource
    strength: float        # touches * tf_weight
    raw_prices: List[float] = field(default_factory=list)

    @property
    def level_id(self) -> str:
        key = (
            f"{self.symbol}|{self.timeframe}|"
            f"{self.side}|{round(self.price, 6)}|"
            f"{self.source}"
        )
        return hashlib.blake2s(
            key.encode(), digest_size=8
        ).hexdigest()


@dataclass
class LevelCluster:
    symbol: str
    price: float               # median price of cluster
    side: LevelSide
    sources: List[str]         # ["SWING:1h", "KDE:4h"]
    touches_total: int
    confluence_score: int      # how many TFs confirmed
    strength: float
    tolerance_pct: float       
    level_id: str              # stable hash for the cluster

    @property
    def is_confluence(self) -> bool:
        return self.confluence_score >= 2


def get_level_price(level: Any) -> float:
    """Safe price accessor for any level representation (dict or object)."""
    if isinstance(level, dict):
        return float(level.get("price", 0.0) or 0.0)
    return float(getattr(level, "price", 0.0) or 0.0)


def get_level_side(level: Any) -> str:
    """Safe side accessor for any level representation (dict or object)."""
    if isinstance(level, dict):
        return str(level.get("side", "") or level.get("type", ""))
    return str(getattr(level, "side", getattr(level, "type", "")))


def get_level_id(level: Any) -> str:
    """Safe level_id accessor."""
    if isinstance(level, dict):
        return str(level.get("level_id", "") or "")
    return str(getattr(level, "level_id", ""))


def get_level_source(level: Any) -> str:
    """Safe source accessor."""
    if isinstance(level, dict):
        return str(level.get("source", "UNKNOWN"))
    return str(getattr(level, "source", "UNKNOWN"))


def get_level_touches(level: Any) -> int:
    """Safe touches accessor."""
    if isinstance(level, dict):
        return int(level.get("touches", 1) or 1)
    return int(getattr(level, "touches", 1) or 1)


def get_level_score(level: Any) -> float:
    """Safe score accessor."""
    if isinstance(level, dict):
        return float(level.get("score", 0.0) or level.get("strength", 0.0))
    return float(getattr(level, "score", getattr(level, "strength", 0.0)))
