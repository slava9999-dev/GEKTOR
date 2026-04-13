# src/domain/math_core.py
import math
import time
from enum import Enum, auto
from typing import List, Optional, Dict, Any, Deque
from dataclasses import dataclass, field
from collections import deque
from loguru import logger
from src.infrastructure.config import settings

@dataclass(slots=True, frozen=True)
class MarketTick:
    symbol: str
    timestamp: int
    price: float
    volume: float
    side: str # 'Buy' or 'Sell'
    conflated: bool = False

class EngineState(Enum):
    WARMUP = auto()
    ACTIVE = auto()
    POISONED = auto()

class PremiseDirection(Enum):
    LONG = auto()
    SHORT = auto()

@dataclass(slots=True)
class PremiseTracker:
    symbol: str
    direction: PremiseDirection
    entry_vpin: float
    entry_price: float
    entry_ts: int
    bars_observed: int = 0
    max_vpin_since_entry: float = 0.0

class MathCore:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.state = EngineState.WARMUP
        self._last_timestamp = None
        
        # [SIGMAGUARD] - Welford stats for price volatility
        self.k: int = 0
        self.m: float = 0.0
        self.s: float = 0.0
        
        # [WARMUP]
        self.accumulated_warmup_vol = 0.0
        self.last_known_price = 0.0
        
        # [DYNAMIC BUCKET SIZING v4.1]
        # Инициализация объема из настроек для конкретного символа
        self.bucket_vol = settings.VOLUME_BUCKETS.get(symbol, settings.VOLUME_BUCKETS["DEFAULT"])
        self.target_bars_per_day = 288 # Цель: бары по ~5 минут в среднем
        self.ema_alpha = 2.0 / (self.target_bars_per_day + 1)
        self.last_bar_ts: Optional[int] = None
        
        # [VPIN ACCUMULATORS]
        self.vpin_window = settings.VPIN_WINDOW
        self.current_vol = 0.0
        self.buy_vol_usd = 0.0
        self.sell_vol_usd = 0.0
        self.imbalance_history: List[float] = []
        
        # [CUSUM/STAT ПАМЯТИ]
        self.vpin_k = 0
        self.vpin_m = 0.0
        self.vpin_s = 0.0
        
        # [PHASE 5: MONITORING CONTEXT]
        self.active_premise: Optional[PremiseTracker] = None

    def import_macro_state(self, state: Dict[str, Any]):
        if not state: return
        try:
            self.state = EngineState[state["state"]]
            self.k, self.m, self.s = state["k"], state["m"], state["s"]
            self.accumulated_warmup_vol = state["warmup_vol"]
            self.last_known_price = state["last_price"]
            self._last_timestamp = state["last_ts"]
            
            self.current_vol = state.get("current_vol", 0.0)
            self.buy_vol_usd = state.get("buy_vol_usd", 0.0)
            self.sell_vol_usd = state.get("sell_vol_usd", 0.0)
            self.imbalance_history = state.get("imbalance_history", [])
            
            self.vpin_k = state.get("vpin_k", 0)
            self.vpin_m = state.get("vpin_m", 0.0)
            self.vpin_s = state.get("vpin_s", 0.0)
            
            # ВОССТАНОВЛЕНИЕ ДИНАМИЧЕСКОГО ОБЪЕМА
            default_vol = settings.VOLUME_BUCKETS.get(self.symbol, settings.VOLUME_BUCKETS["DEFAULT"])
            self.bucket_vol = state.get("bucket_vol", default_vol)
            self.last_bar_ts = state.get("last_bar_ts")
            
            # [PHASE 5] HYDRATION
            p = state.get("active_premise")
            if p:
                self.active_premise = PremiseTracker(
                    symbol=self.symbol,
                    direction=PremiseDirection[p["direction"]],
                    entry_vpin=p["entry_vpin"],
                    entry_price=p["entry_price"],
                    entry_ts=p["entry_ts"],
                    bars_observed=p.get("bars_observed", 0),
                    max_vpin_since_entry=p.get("max_vpin_since_entry", 0.0)
                )
        except Exception as e:
            logger.error(f"⚠️ [MathCore] State corruption for {self.symbol}: {e}")

    def export_macro_state(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "state": self.state.name,
            "k": self.k, "m": self.m, "s": self.s,
            "warmup_vol": self.accumulated_warmup_vol,
            "last_price": self.last_known_price,
            "last_ts": self._last_timestamp,
            "current_vol": self.current_vol,
            "buy_vol_usd": self.buy_vol_usd,
            "sell_vol_usd": self.sell_vol_usd,
            "imbalance_history": list(self.imbalance_history),
            "vpin_k": self.vpin_k,
            "vpin_m": self.vpin_m,
            "vpin_s": self.vpin_s,
            "bucket_vol": self.bucket_vol,
            "last_bar_ts": self.last_bar_ts,
            "active_premise": {
                "direction": self.active_premise.direction.name,
                "entry_vpin": self.active_premise.entry_vpin,
                "entry_price": self.active_premise.entry_price,
                "entry_ts": self.active_premise.entry_ts,
                "bars_observed": self.active_premise.bars_observed,
                "max_vpin_since_entry": self.active_premise.max_vpin_since_entry
            } if self.active_premise else None
        }

    def process_batch(self, batch: List[MarketTick]) -> List[dict]:
        batch.sort(key=lambda x: x.timestamp)
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
        if self.last_known_price > 0:
            if self._is_toxic_glitch(tick.price, tick.volume): return None
        
        self.last_known_price = tick.price
        dollar_volume = tick.price * tick.volume
        
        if self.state == EngineState.WARMUP:
            self.accumulated_warmup_vol += dollar_volume
            if self.accumulated_warmup_vol >= settings.WARMUP_VOLUME:
                self.state = EngineState.ACTIVE
                self.last_bar_ts = tick.timestamp
                logger.success(f"🟢 [MathCore] WARMUP COMPLETE. Engine ACTIVE ({self.symbol})")
            return None

        if self.state == EngineState.ACTIVE:
            self.current_vol += dollar_volume
            if tick.side == 'Buy': self.buy_vol_usd += dollar_volume
            else: self.sell_vol_usd += dollar_volume
            
            if self.current_vol >= self.bucket_vol:
                # 1. ТЕЛЕМЕТРИЯ И РАСЧЕТ VPIN
                imbalance = abs(self.buy_vol_usd - self.sell_vol_usd)
                self.imbalance_history.append(imbalance)
                if len(self.imbalance_history) > self.vpin_window:
                    self.imbalance_history.pop(0)
                
                vpin = 0.0
                history_len = len(self.imbalance_history)
                if history_len == self.vpin_window:
                    vpin = sum(self.imbalance_history) / (self.vpin_window * self.bucket_vol)
                
                # ADAPTIVE THRESHOLD (CUSUM)
                self.vpin_k += 1
                old_v_m = self.vpin_m
                self.vpin_m += (vpin - self.vpin_m) / self.vpin_k
                self.vpin_s += (vpin - old_v_m) * (vpin - self.vpin_m)
                
                # Use epsilon to prevent sqrt(0) or div by zero
                vpin_var = self.vpin_s / (self.vpin_k - 1) if self.vpin_k > 1 else 0.0
                vpin_std = math.sqrt(max(0.0, vpin_var))
                
                # Ensure threshold has a sane minimum to prevent hair-trigger alerts
                dynamic_threshold = max(settings.VPIN_THRESHOLD, self.vpin_m + 3.0 * vpin_std) if self.vpin_k > 50 else settings.VPIN_THRESHOLD
                score = vpin / dynamic_threshold if dynamic_threshold > 0 else 0
                
                # 2. [DYNAMIC EMA BAR SIZING] - Адаптация линзы
                duration_ms = tick.timestamp - self.last_bar_ts if self.last_bar_ts else 300_000
                self._update_bucket_size(self.bucket_vol, duration_ms)
                self.last_bar_ts = tick.timestamp

                # Лог пульса с новым размером бара (в млн $)
                mode = f"SCORE: {score:.2f} | THR: {dynamic_threshold:.3f}" if history_len == self.vpin_window else f"CALIB: {history_len}/{self.vpin_window}"
                logger.info(f"📊 [Pulse] {self.symbol} | VPIN: {vpin:.4f} | {mode} | Next Bar: ${self.bucket_vol/1_000_000:.2f}M")

                res = {
                    "vpin": vpin,
                    "price": tick.price,
                    "timestamp": tick.timestamp,
                    "bar_completed": True,
                    "is_anomaly": vpin >= dynamic_threshold and self.vpin_k > 50
                }
                
                self.current_vol -= self.bucket_vol
                self.buy_vol_usd = 0.0
                self.sell_vol_usd = 0.0
                
                # [PHASE 5] СОПРОВОЖДЕНИЕ ГИПОТЕЗЫ
                if self.active_premise:
                    abort_reason = self._check_premise_invalidation(vpin, tick.price)
                    if abort_reason:
                        res["abort_mission"] = True
                        res["abort_reason"] = abort_reason
                        self.active_premise = None # Сброс мониторинга
                    else:
                        self.active_premise.bars_observed += 1
                        self.active_premise.max_vpin_since_entry = max(self.active_premise.max_vpin_since_entry, vpin)

                # Авто-активация мониторинга при аномалии (если еще нет активной)
                if res["is_anomaly"] and not self.active_premise:
                    direction = PremiseDirection.LONG if tick.side == 'Buy' else PremiseDirection.SHORT
                    self.active_premise = PremiseTracker(
                        symbol=self.symbol,
                        direction=direction,
                        entry_vpin=vpin,
                        entry_price=tick.price,
                        entry_ts=tick.timestamp
                    )
                    logger.info(f"🔍 [Monitor] Started tracking premise for {self.symbol} ({direction.name})")

                return res
        return None

    def _update_bucket_size(self, actual_vol: float, duration_ms: int):
        """EMA адаптация размера барсетки под поток ликвидности."""
        if duration_ms <= 0: return
        # Volume Rate = Доллары в мс
        vol_rate = actual_vol / duration_ms
        # Целевая длительность бара (5 минут = 300,000 мс)
        target_ms = (24 * 60 * 60 * 1000) / self.target_bars_per_day
        projected_vol = vol_rate * target_ms
        
        # EMA сглаживание
        self.bucket_vol = (self.ema_alpha * projected_vol) + ((1 - self.ema_alpha) * self.bucket_vol)
        # Защитные лимиты ($100k - $50M)
        self.bucket_vol = max(100_000.0, min(self.bucket_vol, 50_000_000.0))

    def _is_toxic_glitch(self, price: float, volume: float) -> bool:
        self.k += 1
        old_m = self.m
        self.m += (price - self.m) / self.k
        self.s += (price - old_m) * (price - self.m)
        if self.k < 100: return False
        
        # [Welford Guard] Use epsilon to prevent div by zero
        variance = self.s / (self.k - 1) if self.k > 1 else 0.0
        std_dev = math.sqrt(max(0.0, variance))
        
        delta = abs(price - self.last_known_price)
        # Apply a noise floor (std_dev baseline) to prevent false flags on constant prices
        effective_std = max(std_dev, price * 1e-6) # 1bps noise floor
        return delta > (effective_std * 5.0) and (price * volume) < 5000.0

    def _trigger_hard_reset(self, price: float):
        self.accumulated_warmup_vol = 0.0
        self.state = EngineState.WARMUP
        self.last_known_price = price
        self.imbalance_history.clear()
        self.current_vol = 0.0
        self.buy_vol_usd = 0.0
        self.sell_vol_usd = 0.0
        self.vpin_k, self.vpin_m, self.vpin_s = 0, 0.0, 0.0
        self.bucket_vol = settings.VOLUME_BUCKETS.get(self.symbol, settings.VOLUME_BUCKETS["DEFAULT"])
        self.active_premise = None

    def _check_premise_invalidation(self, current_vpin: float, current_price: float) -> Optional[str]:
        """Проверка целостности торговой гипотезы."""
        if not self.active_premise: return None
        
        p = self.active_premise
        
        # 1. VPIN DECAY (Flow Exhaustion)
        # Если поток информированных денег упал на 60% от пикового после входа
        decay_threshold = p.max_vpin_since_entry * settings.EXIT_VPIN_DECAY_FACTOR
        if current_vpin < decay_threshold and p.bars_observed > 3:
            return f"VPIN_DECAY: flow exhausted ({current_vpin:.4f} < {decay_threshold:.4f})"

        # 2. TEMPORAL DECAY (Time-Stop)
        if p.bars_observed >= settings.EXIT_TIME_MAX_BARS:
            return f"TIME_EXPIRED: anomaly failed to realize impulse in {settings.EXIT_TIME_MAX_BARS} bars"

        # 3. MICROSTRUCTURAL REVERSAL (Counter-Breach)
        # (Упрощенно: если цена ушла слишком далеко против нас на высоком VPIN)
        price_diff_pct = (current_price - p.entry_price) / p.entry_price
        if p.direction == PremiseDirection.LONG and price_diff_pct < -0.05: # -5% стоп по микроструктуре
             return f"PRICE_REVERSAL: structural breach against LONG"
        if p.direction == PremiseDirection.SHORT and price_diff_pct > 0.05: # +5% стоп
             return f"PRICE_REVERSAL: structural breach against SHORT"

        return None

def process_ticks_subroutine(symbol: str, ticks_data: List[dict], bucket_vol: float, state: Optional[dict]) -> dict:
    engine = MathCore(symbol)
    if state: engine.import_macro_state(state)
    ticks = [MarketTick(**t) for t in ticks_data]
    results = engine.process_batch(ticks)
    for r in results: r["symbol"] = symbol
    return {
        "results": results,
        "new_state": engine.export_macro_state()
    }
