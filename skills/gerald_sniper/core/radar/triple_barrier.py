# core/radar/triple_barrier.py
"""
[GEKTOR v21.15] Dynamic Triple Barrier Method (López de Prado).

Core Architecture:
    Three barriers that govern position exit:
    1. UPPER (Take Profit) = entry + pt_sigma * σ_db  — Dynamic TP
    2. LOWER (Stop Loss)   = entry - sl_sigma * σ_db  — Dynamic SL
    3. VERTICAL (Time Decay) = max_macro_bars Dollar Bars — Alpha Expiration

    Barriers EXPAND/CONTRACT with volatility (σ_db) on each Dollar Bar.
    This is NOT a fixed TP/SL — it "breathes" with the market.

Integration Point:
    MonitoringOrchestrator.process_macro_bar() — evaluated on each new Dollar Bar.
    
    BarrierState is persisted atomically alongside MonitoringContext.
    On trigger: Outbox event with Priority 5 (higher than standard Entry Alert at Priority 10).

Manifesto Compliance:
    - GEKTOR does NOT close positions. Barrier touch generates advisory alert.
    - Operator closes position manually.
    - VPIN Inversion remains as the GLOBAL abort (macro structure collapse).
    - TBM is the LOCAL, precision exit engine.
"""
import math
import time
from dataclasses import dataclass, field
from typing import Optional, Dict
from loguru import logger


@dataclass(slots=True)
class PenetrationState:
    """[GEKTOR v21.15.1] Anti-Whipsaw State."""
    is_armed: bool = False
    accumulated_volume_usd: float = 0.0
    required_volume_usd: float = 10000.0  # Default $10k barrier confirmation

    def to_dict(self) -> Dict:
        return {
            "is_armed": self.is_armed,
            "accumulated_volume_usd": self.accumulated_volume_usd,
            "required_volume_usd": self.required_volume_usd
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "PenetrationState":
        return cls(
            is_armed=data.get("is_armed", False),
            accumulated_volume_usd=data.get("accumulated_volume_usd", 0.0),
            required_volume_usd=data.get("required_volume_usd", 10000.0)
        )


@dataclass(slots=True)
class BarrierState:
    """Mutable barrier state — updated on each Dollar Bar."""
    upper_target: float       # Take Profit price
    lower_stop: float         # Hard Stop Loss price
    bars_elapsed: int         # Dollar Bars since entry
    max_bars: int             # Vertical barrier (alpha expiration)
    
    # [GEKTOR v21.15.1] Volume-Confirmed Penetration
    penetration: PenetrationState = field(default_factory=PenetrationState)
    
    def to_dict(self) -> Dict:
        return {
            "upper_target": self.upper_target,
            "lower_stop": self.lower_stop,
            "bars_elapsed": self.bars_elapsed,
            "max_bars": self.max_bars,
            "entry_price": self.entry_price,
            "peak_price": self.peak_price,
            "trough_price": self.trough_price,
            "is_long": self.is_long,
            "pt_sigma": self.pt_sigma_multiplier,
            "sl_sigma": self.sl_sigma_multiplier
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "BarrierState":
        return cls(
            upper_target=data.get("upper_target", 0.0),
            lower_stop=data.get("lower_stop", 0.0),
            bars_elapsed=data.get("bars_elapsed", 0),
            max_bars=data.get("max_bars", 15),
            entry_price=data.get("entry_price", 0.0),
            peak_price=data.get("peak_price", 0.0),
            trough_price=data.get("trough_price", 0.0),
            is_long=data.get("is_long", True),
            pt_sigma_multiplier=data.get("pt_sigma", 2.0),
            sl_sigma_multiplier=data.get("sl_sigma", 2.0)
        )


@dataclass(slots=True, frozen=True)
class BarrierEvent:
    """Immutable result when a barrier is touched."""
    event_type: str     # UPPER_BARRIER_TOUCH | LOWER_BARRIER_TOUCH | TIME_BARRIER_EXPIRATION
    symbol: str
    current_price: float
    barrier_price: float
    entry_price: float
    bars_elapsed: int
    pnl_pct: float      # Unrealized P&L at touch
    
    @property
    def is_profit(self) -> bool:
        return self.event_type == "UPPER_BARRIER_TOUCH"

    @property
    def priority(self) -> int:
        """Outbox priority: lower = higher urgency."""
        if self.event_type == "LOWER_BARRIER_TOUCH":
            return 3   # Stop Loss = CRITICAL
        if self.event_type == "UPPER_BARRIER_TOUCH":
            return 5   # Take Profit = HIGH
        return 7       # Time Expiry = MEDIUM


class TripleBarrierEngine:
    """
    [GEKTOR v21.15] Dynamic Triple Barrier (López de Prado, Ch. 3).
    
    Barriers "breathe" with σ_db — they expand in volatile markets 
    and contract in calm markets, adapting to regime changes.
    """
    def __init__(
        self,
        pt_sigma: float = 2.0,   # Take Profit at 2.0σ above entry
        sl_sigma: float = 2.0,   # Stop Loss at 2.0σ below entry
        max_macro_bars: int = 15  # Alpha expires after 15 Dollar Bars
    ):
        self.pt_sigma = pt_sigma
        self.sl_sigma = sl_sigma
        self.max_macro_bars = max_macro_bars

    def init_barriers(
        self, 
        entry_price: float, 
        current_sigma: float, 
        is_long: bool = True
    ) -> BarrierState:
        """
        Called once when position opens. Calculates initial barrier coordinates.
        These will be recalculated on each Dollar Bar based on fresh σ_db.
        """
        sigma = max(current_sigma, 0.005)  # Floor at 0.5% to prevent degenerate barriers
        
        if is_long:
            upper = entry_price * (1 + self.pt_sigma * sigma)
            lower = entry_price * (1 - self.sl_sigma * sigma)
        else:
            upper = entry_price * (1 + self.sl_sigma * sigma)  # SL for short
            lower = entry_price * (1 - self.pt_sigma * sigma)  # TP for short
        
        state = BarrierState(
            upper_target=round(upper, 6),
            lower_stop=round(lower, 6),
            bars_elapsed=0,
            max_bars=self.max_macro_bars,
            entry_price=entry_price,
            peak_price=entry_price,
            trough_price=entry_price,
            is_long=is_long,
            pt_sigma_multiplier=self.pt_sigma,
            sl_sigma_multiplier=self.sl_sigma
        )
        
        logger.info(
            f"📊 [TBM] Barriers initialized: "
            f"TP=${state.upper_target:,.4f} | SL=${state.lower_stop:,.4f} | "
            f"MaxBars={state.max_bars} | σ={sigma:.4f}"
        )
        
        return state

    def recalculate_barriers(
        self, 
        state: BarrierState, 
        current_sigma: float, 
        current_price: float
    ) -> None:
        """
        Called on each Dollar Bar: barriers BREATHE with σ_db.
        Entry price stays fixed — only the σ multiplier boundary shifts.
        
        Mutates state in-place (atomic with DB persist in MonitoringOrchestrator).
        """
        sigma = max(current_sigma, 0.005)
        
        if state.is_long:
            state.upper_target = round(state.entry_price * (1 + state.pt_sigma_multiplier * sigma), 6)
            state.lower_stop = round(state.entry_price * (1 - state.sl_sigma_multiplier * sigma), 6)
        else:
            state.upper_target = round(state.entry_price * (1 + state.sl_sigma_multiplier * sigma), 6)
            state.lower_stop = round(state.entry_price * (1 - state.pt_sigma_multiplier * sigma), 6)
        
        # Track peak/trough for trailing analysis
        state.peak_price = max(state.peak_price, current_price)
        state.trough_price = min(state.trough_price, current_price)
        
        state.bars_elapsed += 1

    def evaluate(
        self, 
        symbol: str, 
        current_price: float, 
        state: BarrierState,
        volume_usd: float = 0.0
    ) -> Optional[BarrierEvent]:
        """
        [GEKTOR v21.15.1] Anti-Whipsaw Evaluation.
        
        Evaluates barrier conditions with Volume Confirmation (VCP).
        - Price touch -> ARMED (start counting volume)
        - Accumulated Volume > Threshold -> CONFIRMED (trigger exit)
        - Price returns to Safe Zone -> SAFE (reset accumulation)
        """
        pnl_pct = (current_price - state.entry_price) / state.entry_price
        if not state.is_long:
            pnl_pct = -pnl_pct
        
        pen = state.penetration
        
        # 1. UPPER Barrier (TP for long, SL for short)
        if current_price >= state.upper_target:
            pen.is_armed = True
            pen.accumulated_volume_usd += volume_usd
            
            if pen.accumulated_volume_usd >= pen.required_volume_usd:
                event_type = "UPPER_BARRIER_CONFIRMED" if state.is_long else "LOWER_BARRIER_CONFIRMED"
                return BarrierEvent(
                    event_type=event_type,
                    symbol=symbol,
                    current_price=current_price,
                    barrier_price=state.upper_target,
                    entry_price=state.entry_price,
                    bars_elapsed=state.bars_elapsed,
                    pnl_pct=round(pnl_pct, 6)
                )
            return None # Armed but not yet confirmed
        
        # 2. LOWER Barrier (SL for long, TP for short)
        if current_price <= state.lower_stop:
            pen.is_armed = True
            pen.accumulated_volume_usd += volume_usd
            
            if pen.accumulated_volume_usd >= pen.required_volume_usd:
                event_type = "LOWER_BARRIER_CONFIRMED" if state.is_long else "UPPER_BARRIER_CONFIRMED"
                return BarrierEvent(
                    event_type=event_type,
                    symbol=symbol,
                    current_price=current_price,
                    barrier_price=state.lower_stop,
                    entry_price=state.entry_price,
                    bars_elapsed=state.bars_elapsed,
                    pnl_pct=round(pnl_pct, 6)
                )
            return None # Armed but not yet confirmed
            
        # 3. WHIPSAW RESET: Price returned to SAFE zone
        if pen.is_armed and state.lower_stop < current_price < state.upper_target:
            if pen.accumulated_volume_usd > 0:
                logger.debug(f"🧹 [TBM] [{symbol}] Whipsaw Rejected. V_acc=${pen.accumulated_volume_usd:,.0f} rejected.")
            pen.is_armed = False
            pen.accumulated_volume_usd = 0.0
        
        # 4. VERTICAL Barrier (Time Decay)
        # Evaluated independently of whipsaws
        if state.bars_elapsed >= state.max_bars:
            return BarrierEvent(
                event_type="TIME_BARRIER_EXPIRATION",
                symbol=symbol,
                current_price=current_price,
                barrier_price=0.0,
                entry_price=state.entry_price,
                bars_elapsed=state.bars_elapsed,
                pnl_pct=round(pnl_pct, 6)
            )
        
        return None

    @staticmethod
    def format_barrier_alert(event: BarrierEvent) -> str:
        """Formats barrier touch for Telegram."""
        emoji = {"UPPER_BARRIER_TOUCH": "🎯", "LOWER_BARRIER_TOUCH": "🛑", "TIME_BARRIER_EXPIRATION": "⏳"}
        e = emoji.get(event.event_type, "📊")
        
        return (
            f"{e} *TRIPLE BARRIER: #{event.symbol}*\n"
            f"Event: `{event.event_type}`\n"
            f"Price: `${event.current_price:,.4f}`\n"
            f"Entry: `${event.entry_price:,.4f}`\n"
            f"P&L: `{event.pnl_pct:+.2%}`\n"
            f"Bars: `{event.bars_elapsed}`\n"
            f"⚠️ _Close position manually NOW._"
        )

    @staticmethod
    def format_barriers_for_entry(state: BarrierState) -> str:
        """Embeds barrier coordinates into the Entry Alert."""
        return (
            f"\n📊 *TRIPLE BARRIER (Dynamic):*\n"
            f"  🎯 TP: `${state.upper_target:,.4f}` ({state.pt_sigma_multiplier}σ)\n"
            f"  🛑 SL: `${state.lower_stop:,.4f}` ({state.sl_sigma_multiplier}σ)\n"
            f"  ⏳ Expiry: `{state.max_bars}` Dollar Bars\n"
            f"  _Barriers breathe with σ\\_db — recalculated each bar._"
        )


# Global singleton
triple_barrier = TripleBarrierEngine()
