from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional


class LevelState(Enum):
    FRESH = "fresh"
    ACTIVE = "active"
    STALE = "stale"
    DEAD = "dead"


@dataclass
class StoredLevel:
    symbol: str
    price: float
    source: str
    touches: int
    score: float
    level_id: str = ""
    age_candles: int = 0
    state: LevelState = LevelState.FRESH

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "source": self.source,
            "touches": self.touches,
            "score": self.score,
            "level_id": self.level_id,
            "age_candles": self.age_candles,
            "state": str(self.state.value if hasattr(self.state, 'value') else self.state)
        }


class LevelStore:
    """
    Persistent level storage across scans/candles.
    Levels evolve through lifecycle:
    FRESH -> ACTIVE -> STALE -> DEAD
    """

    def __init__(
        self,
        merge_tolerance_pct: float = 0.01,
        proximity_atr_mult: float = 0.8,
        stale_atr_mult: float = 2.5,
        dead_atr_mult: float = 5.0,
        fresh_candles_required: int = 3,
    ):
        self.levels: Dict[str, List[StoredLevel]] = {}
        self.merge_tolerance_pct = merge_tolerance_pct
        self.proximity_atr_mult = proximity_atr_mult
        self.stale_atr_mult = stale_atr_mult
        self.dead_atr_mult = dead_atr_mult
        self.fresh_candles_required = fresh_candles_required

    def add_or_merge_level(
        self,
        symbol: str,
        price: float,
        source: str,
        touches: int,
        score: float,
        level_id: str = "",
    ) -> None:
        if symbol not in self.levels:
            self.levels[symbol] = []

        existing = self._find_merge_candidate(symbol, price)
        if existing is not None:
            existing.touches = max(existing.touches, touches)
            existing.score = max(existing.score, score)
            if source not in existing.source:
                existing.source = f"{existing.source}|{source}"
            return

        self.levels[symbol].append(
            StoredLevel(
                symbol=symbol,
                price=float(price),
                source=str(source),
                touches=int(touches),
                score=float(score),
                level_id=level_id
            )
        )

    def _find_merge_candidate(self, symbol: str, price: float) -> Optional[StoredLevel]:
        for level in self.levels.get(symbol, []):
            dist_pct = abs(level.price - price) / max(abs(level.price), 1e-12)
            if dist_pct <= self.merge_tolerance_pct:
                return level
        return None

    def is_price_near_level(self, price: float, level_price: float, atr_abs: float) -> bool:
        return abs(price - level_price) <= atr_abs * self.proximity_atr_mult

    def update_symbol(self, symbol: str, current_price: float, atr_abs: float) -> None:
        if symbol not in self.levels:
            return

        if atr_abs is None or atr_abs <= 0:
            if symbol in self.levels:
                for level in self.levels[symbol]:
                    level.state = LevelState.STALE
            return

        survivors: List[StoredLevel] = []

        for level in self.levels[symbol]:
            level.age_candles += 1
            distance = abs(current_price - level.price)

            if atr_abs is None or atr_abs <= 0:
                level.state = LevelState.STALE
            elif distance > atr_abs * self.dead_atr_mult:
                level.state = LevelState.DEAD
            elif distance > atr_abs * self.stale_atr_mult:
                level.state = LevelState.STALE
            elif level.age_candles < self.fresh_candles_required:
                level.state = LevelState.FRESH
            elif self.is_price_near_level(current_price, level.price, atr_abs):
                level.state = LevelState.ACTIVE
            else:
                level.state = LevelState.STALE

            if level.state != LevelState.DEAD:
                survivors.append(level)

        self.levels[symbol] = survivors

    def get_all_levels(self, symbol: str) -> List[StoredLevel]:
        return list(self.levels.get(symbol, []))

    def get_triggerable_levels(self, symbol: str) -> List[StoredLevel]:
        return [
            level
            for level in self.levels.get(symbol, [])
            if level.state == LevelState.ACTIVE
        ]
