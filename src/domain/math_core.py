# src/domain/math_core.py
import math
from enum import Enum, auto
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from loguru import logger

# 1. СТРУКТУРЫ ДАННЫХ
@dataclass(slots=True, frozen=True)
class MarketTick:
    symbol: str
    timestamp: int
    price: float
    volume: float
    side: str # 'B' or 'S'

class EngineState(Enum):
    WARMUP = auto()
    ACTIVE = auto()
    POISONED = auto()

# 2. МАТЕМАТИЧЕСКИЙ ОРКЕСТРАТОР (С поддержкой State Passing)
class MathCore:
    REQUIRED_WARMUP_VOLUME = 5_000_000.0

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.state = EngineState.WARMUP
        self._last_timestamp = None
        
        # SigmaGuard (Welford) State
        self.k: int = 0
        self.m: float = 0.0
        self.s: float = 0.0
        
        # Warmup State
        self.accumulated_warmup_vol = 0.0
        self.last_known_price = 0.0
        
        # VPIN / Bar Accumulators
        self.vpin_window = 50
        self.bucket_vol = 1_000_000.0
        self.current_vol = 0.0
        self.buy_vol = 0.0
        self.sell_vol = 0.0
        self.imbalance_history: List[float] = []

    def import_macro_state(self, state: Dict[str, Any]):
        """Восстановление состояния из сжатого словаря (O(1))."""
        if not state: return
        
        self.state = EngineState[state["state"]]
        self.k = state["k"]
        self.m = state["m"]
        self.s = state["s"]
        self.accumulated_warmup_vol = state["warmup_vol"]
        self.last_known_price = state["last_price"]
        self._last_timestamp = state["last_ts"]
        
        # VPIN State
        self.current_vol = state["current_vol"]
        self.buy_vol = state["buy_vol"]
        self.sell_vol = state["sell_vol"]
        self.imbalance_history = state["imbalance_history"]

    def export_macro_state(self) -> Dict[str, Any]:
        """Экспорт минимально необходимого набора данных для IPC."""
        return {
            "state": self.state.name,
            "k": self.k,
            "m": self.m,
            "s": self.s,
            "warmup_vol": self.accumulated_warmup_vol,
            "last_price": self.last_known_price,
            "last_ts": self._last_timestamp,
            "current_vol": self.current_vol,
            "buy_vol": self.buy_vol,
            "sell_vol": self.sell_vol,
            "imbalance_history": self.imbalance_history # Константное окно
        }

    def process_batch(self, batch: List[MarketTick]) -> List[dict]:
        batch.sort(key=lambda x: x.timestamp)
        
        # Gap Guard (Temporal Shield)
        if self._last_timestamp and (batch[0].timestamp - self._last_timestamp > 60_000):
            logger.warning(f"🧹 [MathCore] GAP DETECTED ({self.symbol}). Resetting.")
            self._trigger_hard_reset(batch[0].price)

        results = []
        for tick in batch:
            res = self._ingest_tick(tick)
            if res: results.append(res)
            
        self._last_timestamp = batch[-1].timestamp
        return results

    def _ingest_tick(self, tick: MarketTick) -> Optional[dict]:
        # 1. SigmaGuard (Anti-Glitch)
        if self.last_known_price > 0:
            if self._is_toxic_glitch(tick.price, tick.volume):
                return None
        
        self.last_known_price = tick.price
        dollar_volume = tick.price * tick.volume
        
        # 2. Warmup Logic
        if self.state == EngineState.WARMUP:
            self.accumulated_warmup_vol += dollar_volume
            if self.accumulated_warmup_vol >= self.REQUIRED_WARMUP_VOLUME:
                self.state = EngineState.ACTIVE
                logger.success(f"🟢 [MathCore] WARMUP COMPLETE. Engine ACTIVE ({self.symbol})")
            return None

        # 3. VPIN Logic (Active State)
        if self.state == EngineState.ACTIVE:
            self.current_vol += dollar_volume
            if tick.side == 'B': self.buy_vol += tick.volume
            else: self.sell_vol += tick.volume
            
            if self.current_vol >= self.bucket_vol:
                imbalance = abs(self.buy_vol - self.sell_vol)
                self.imbalance_history.append(imbalance)
                if len(self.imbalance_history) > self.vpin_window:
                    self.imbalance_history.pop(0)
                
                vpin = None
                if len(self.imbalance_history) == self.vpin_window:
                    vpin = sum(self.imbalance_history) / (self.vpin_window * self.bucket_vol)
                
                res = {
                    "vpin": vpin,
                    "price": tick.price,
                    "timestamp": tick.timestamp,
                    "bar_completed": True
                }
                self.current_vol -= self.bucket_vol
                self.buy_vol = 0.0
                self.sell_vol = 0.0
                return res
        return None

    def _is_toxic_glitch(self, price: float, volume: float) -> bool:
        # Welford update
        self.k += 1
        old_m = self.m
        self.m += (price - self.m) / self.k
        self.s += (price - old_m) * (price - self.m)
        
        if self.k < 100: return False
        
        variance = self.s / (self.k - 1)
        std_dev = math.sqrt(variance)
        delta = abs(price - self.last_known_price)
        
        if delta > (std_dev * 5.0) and (price * volume) < 5000.0:
            return True
        return False

    def _trigger_hard_reset(self, price: float):
        self.accumulated_warmup_vol = 0.0
        self.state = EngineState.WARMUP
        self.last_known_price = price
        self.imbalance_history.clear()
        self.current_vol = 0.0

# 3. WORKER SUBROUTINE (State Passing)
def process_ticks_subroutine(symbol: str, ticks_data: List[dict], bucket_vol: float, state: Optional[dict]) -> dict:
    """[GEKTOR v2.0] Stateless Worker Subroutine with State Rehydration."""
    engine = MathCore(symbol)
    engine.bucket_vol = bucket_vol
    
    # 1. Загружаем память
    if state:
        engine.import_macro_state(state)
    
    # 2. Считаем
    ticks = [MarketTick(**t) for t in ticks_data]
    results = engine.process_batch(ticks)
    
    # 3. Возвращаем результаты и ОБНОВЛЕННУЮ память
    # Инъекция символа в результаты
    for r in results: r["symbol"] = symbol
    
    return {
        "results": results,
        "new_state": engine.export_macro_state()
    }
