# skills/gerald_sniper/core/quant/bar_engine.py
import numpy as np
from loguru import logger
from typing import List, Optional, Dict
from dataclasses import dataclass
import collections
from .quarantine import MarketQuarantineGuard, StateInvalidationError, ClockSynchronizer

@dataclass
class DollarBar:
    symbol: str
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    ticks: int
    volume: float
    buy_volume: float
    sell_volume: float
    vpin: float
    sigma: float
    max_latency_ms: float = 0.0

class SymbolState:
    """Внутреннее состояние для конкретного инструмента."""
    def __init__(self):
        self.last_price = 0.0
        self.last_side = 0 # 1 for buy, -1 for sell
        self.reset()

    def reset(self):
        self.current_cum_value = 0.0
        self.buy_vol = 0.0
        self.sell_vol = 0.0
        self.ohlc = {'open': None, 'high': -np.inf, 'low': np.inf, 'close': None}
        self.prices = []
        self.max_latency_ms = 0.0

class AdaptiveDollarBarEngine:
    """
    [GEKTOR v21.58] Multi-Symbol Precision Bar Engine.
    """
    def __init__(self, clock_sync: ClockSynchronizer, target_dollar_value: float = 1_000_000, conflator=None, grid: Optional[MarketQuarantineGuard] = None):
        self.target_value = target_dollar_value
        self.states: Dict[str, SymbolState] = collections.defaultdict(SymbolState)
        # Shared or local quarantine controller
        self.grid = grid or MarketQuarantineGuard(clock_sync)
        self.conflator = conflator 

    async def process_tick(self, symbol: str, ts: float, price: float, volume: float, is_buy: Optional[bool] = None, trade_id: str = ""):
        """
        Hot Path: Агрегация тика. 
        Защита: Если лаг критический, стейт сбрасывается для предотвращения отравления.
        """
        # 0. QUARANTINE INSPECTION (Фильтр Каузальности)
        try:
            # Превращаем в ms для карантина
            self.grid.inspect_tick(int(ts * 1000))
        except StateInvalidationError:
            # Не логируем каждый тик, чтобы не забивать цикл
            return None

        state = self.states[symbol]
        
        # 1. SIDE DETERMINATION (Tick-Rule fallback)
        if is_buy is not None:
            side = 1 if is_buy else -1
        else:
            if state.last_price > 0:
                if price > state.last_price:
                    side = 1
                elif price < state.last_price:
                    side = -1
                else:
                    side = state.last_side
            else:
                side = 1 
        
        state.last_price = price
        state.last_side = side

        # 2. ACCUMULATION (OHLC & Value)
        if state.ohlc['open'] is None:
            state.ohlc['open'] = price
        
        state.ohlc['high'] = max(state.ohlc['high'], price)
        state.ohlc['low'] = min(state.ohlc['low'], price)
        state.ohlc['close'] = price
        state.prices.append(price)

        if side == 1:
            state.buy_vol += volume
        else:
            state.sell_vol += volume

        tick_value = price * volume
        state.current_cum_value += tick_value

        # 3. Threshold Check
        if state.current_cum_value >= self.target_value:
            bar = self._finalize_bar(symbol, ts)
            state.reset()
            return bar
            
        return None

    def _finalize_bar(self, symbol: str, ts: float) -> DollarBar:
        state = self.states[symbol]
        total_vol = state.buy_vol + state.sell_vol
        ticks_count = len(state.prices)
        
        # VPIN (Volume Toxicity)
        vpin = abs(state.buy_vol - state.sell_vol) / total_vol if total_vol > 0 else 0.5
        
        # SIGMA (Log-Returns Standard Deviation)
        if ticks_count > 1:
            try:
                prices_arr = np.array(state.prices)
                if np.any(prices_arr <= 0):
                    sigma = 0.0
                else:
                    log_returns = np.diff(np.log(prices_arr))
                    sigma = np.std(log_returns) * 100 
            except Exception as e:
                logger.error(f"❌ [Quant] Sigma Calc Error: {e}")
                sigma = 0.0
        else:
            sigma = 0.0
        
        logger.info(f"📊 [Quant] Bar {symbol} | Price: {state.ohlc['close']:.2f} | VPIN: {vpin:.4f} | Sigma: {sigma:.4f}% | Ticks: {ticks_count}")
        
        # ZERO FRICTION TELEMETRY: Отправляем в агрегатор, а не напрямую в Prometheus.
        if self.conflator:
            self.conflator.push_vpin(symbol, vpin)
        
        return DollarBar(
            symbol=symbol,
            timestamp=ts,
            open=state.ohlc['open'],
            high=state.ohlc['high'],
            low=state.ohlc['low'],
            close=state.ohlc['close'],
            ticks=ticks_count,
            volume=total_vol,
            buy_volume=state.buy_vol,
            sell_volume=state.sell_vol,
            vpin=vpin,
            sigma=sigma,
            max_latency_ms=state.max_latency_ms
        )

    def reset_math_state(self, symbol: Optional[str] = None):
        """[GEKTOR v21.60.2] Hard zeroing for data integrity recovery."""
        if symbol:
            if symbol in self.states:
                self.states[symbol].reset()
        else:
            for s in self.states.values():
                s.reset()
        logger.warning(f"🧹 [Quant] Math state ZEROED out (target: {symbol or 'ALL'}).")

    def reset_current_accumulation(self, symbol: Optional[str] = None):
        self.reset_math_state(symbol)
