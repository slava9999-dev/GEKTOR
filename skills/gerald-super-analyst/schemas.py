from pydantic import BaseModel, Field
from typing import List, Literal, Optional, Dict

class LevelContext(BaseModel):
    price: float
    type: str  # e.g. "SUPPORT", "RESISTANCE"
    source: str
    touches: int
    distance_pct: float

class BTCContext(BaseModel):
    trend: str
    change_1h: float
    change_4h: float

class RadarV2Context(BaseModel):
    volume_spike: float
    velocity: float
    momentum_pct: float
    atr_ratio: float
    final_score: int

class SignalContext(BaseModel):
    symbol: str
    direction: Literal["LONG", "SHORT"]
    trigger: str
    score: float
    level: LevelContext
    btc_context: BTCContext
    radar: Optional[RadarV2Context] = None
    candles_m5: List[dict]
    candles_m15: List[dict]
    atr_pct: float

class BullResponse(BaseModel):
    pros: List[str]
    best_case: str
    confidence: int = Field(ge=1, le=10)

class BearResponse(BaseModel):
    risks: List[str]
    worst_case: str
    confidence: int = Field(ge=1, le=10)

class ArbiterResponse(BaseModel):
    verdict: Literal["TAKE", "WAIT", "SKIP"]
    conviction: int = Field(ge=1, le=10)
    primary_reason: str
    hidden_stop_hint: Optional[str] = None
    notes: Optional[str] = None

class TakeProfit(BaseModel):
    price: float
    size_pct: int

class ScenarioPlannerResponse(BaseModel):
    scenario_a: str = Field(description="IMPULSE: what to do when trade works")
    scenario_b: str = Field(description="PULLBACK: max acceptable drawdown before invalidation")
    scenario_c: str = Field(description="INVALIDATION: exact exit conditions")

class QuantCheckResult(BaseModel):
    quant_block: bool
    reason: Optional[str] = None

class EliteTradePlan(BaseModel):
    symbol: str
    verdict: Literal["TAKE", "WAIT", "SKIP", "BLOCKED"]
    conviction: int
    entry_mode: Optional[str] = None
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    stop_distance_pct: Optional[float] = None
    take_profits: Optional[List[dict]] = None
    scenarios: Optional[ScenarioPlannerResponse] = None
    risk_notes: Optional[str] = None
    bull_summary: Optional[BullResponse] = None
    bear_summary: Optional[BearResponse] = None

