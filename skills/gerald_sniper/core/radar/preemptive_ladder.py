# core/radar/preemptive_ladder.py
"""
[GEKTOR v21.15] PreEmptive Sigma-Ladder Engine.

Core Principle (López de Prado):
    Protection orders are placed BEFORE a crisis, not during.
    The Sigma-Ladder is computed at ENTRY and monitored for volatility drift.
    
Architecture:
    1. generate_initial_ladder() → Called by ScoringEngine when signal is APPROVED.
       Outputs ladder coordinates for Operator to manually place as Resting Limit Orders.
    
    2. check_ladder_drift() → Called by MonitoringOrchestrator.process_macro_bar().
       If σ_db drifted >5%, alerts Operator to rebalance the ladder.
       
    3. format_telegram_alert() → Formats ladder for Telegram message embedding.

Manifesto Compliance:
    - GEKTOR does NOT place orders. Ladder is advisory only.
    - Operator places Resting Limit Orders manually on exchange.
    - Drift alerts are informational, not actionable by system.
"""
import math
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from loguru import logger


@dataclass(slots=True)
class LadderLevel:
    """Single level of the Sigma-Ladder evacuation grid."""
    sigma_multiplier: float  # 2.5σ, 3.5σ, 5.0σ
    volume_pct: float        # Fraction of total position to exit at this level
    price: float = 0.0       # Exact limit order price
    
    @property
    def volume_label(self) -> str:
        return f"{int(self.volume_pct * 100)}%"


@dataclass(slots=True)
class LadderSnapshot:
    """Full ladder state for persistence and drift monitoring."""
    levels: List[LadderLevel]
    sigma_at_creation: float   # σ_db when ladder was computed
    entry_price: float
    is_long: bool
    created_at: float = 0.0    # Unix timestamp
    
    def to_dict(self) -> Dict:
        return {
            "levels": [
                {"sigma": l.sigma_multiplier, "vol_pct": l.volume_pct, "price": l.price}
                for l in self.levels
            ],
            "sigma_db": self.sigma_at_creation,
            "entry_price": self.entry_price,
            "is_long": self.is_long,
            "created_at": self.created_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "LadderSnapshot":
        levels = [
            LadderLevel(l["sigma"], l["vol_pct"], l["price"])
            for l in data.get("levels", [])
        ]
        return cls(
            levels=levels,
            sigma_at_creation=data.get("sigma_db", 0.02),
            entry_price=data.get("entry_price", 0.0),
            is_long=data.get("is_long", True),
            created_at=data.get("created_at", 0.0)
        )


class PreEmptiveLadderEngine:
    """
    [GEKTOR v21.15] Preventive Evacuation Engine.
    
    Computes survival grids BEFORE crisis, not during.
    Monitors σ_db drift for proactive rebalancing alerts.
    """
    def __init__(self, drift_threshold_pct: float = 0.05):
        """
        Args:
            drift_threshold_pct: Percent change in sigma that triggers rebalance alert.
                                 Default 5%: if σ moved >5%, ladder is stale.
        """
        # Hard-coded Sigma-Ladder structure (Manifesto v21.15)
        self._structure = [
            LadderLevel(2.5, 0.40),  # Tier 1: 40% at 2.5σ
            LadderLevel(3.5, 0.40),  # Tier 2: 40% at 3.5σ
            LadderLevel(5.0, 0.20),  # Tier 3: 20% at 5.0σ (deep survival)
        ]
        self._drift_threshold = drift_threshold_pct
        
        # Active ladders for monitoring (symbol → LadderSnapshot)
        self._active_ladders: Dict[str, LadderSnapshot] = {}

    def generate_initial_ladder(
        self, 
        entry_price: float, 
        current_sigma: float,
        is_long: bool = True
    ) -> LadderSnapshot:
        """
        Generates the Sigma-Ladder at position entry.
        Called by ScoringEngine when signal is APPROVED.
        
        Args:
            entry_price: The entry price of the position.
            current_sigma: σ_db (Dollar Bar volatility) at entry time.
            is_long: True for long positions (ladder below), False for short (ladder above).
            
        Returns:
            LadderSnapshot with exact prices for each tier.
        """
        import time
        
        # Floor sigma at 2% for safety (prevent division artifacts on ultra-low vol assets)
        sigma = max(current_sigma, 0.02)
        
        levels = []
        for template in self._structure:
            if is_long:
                # Long position: safety net BELOW entry
                safe_price = entry_price * (1 - (template.sigma_multiplier * sigma))
            else:
                # Short position: safety net ABOVE entry
                safe_price = entry_price * (1 + (template.sigma_multiplier * sigma))
            
            levels.append(LadderLevel(
                sigma_multiplier=template.sigma_multiplier,
                volume_pct=template.volume_pct,
                price=round(safe_price, 6)
            ))
        
        snapshot = LadderSnapshot(
            levels=levels,
            sigma_at_creation=sigma,
            entry_price=entry_price,
            is_long=is_long,
            created_at=time.time()
        )
        
        logger.info(
            f"🛡️ [LADDER] Generated {len(levels)}-tier Sigma-Ladder @ entry={entry_price:.4f}, "
            f"σ={sigma:.4f}: "
            + " | ".join(f"L{i+1}({l.volume_label})@{l.price:.4f}" for i, l in enumerate(levels))
        )
        
        return snapshot

    def register_active_ladder(self, symbol: str, snapshot: LadderSnapshot) -> None:
        """Registers a ladder for drift monitoring."""
        self._active_ladders[symbol] = snapshot

    def unregister_ladder(self, symbol: str) -> None:
        """Removes ladder on position close/abort."""
        self._active_ladders.pop(symbol, None)

    def check_ladder_drift(
        self, 
        symbol: str, 
        current_mark_price: float, 
        current_sigma: float
    ) -> Optional[LadderSnapshot]:
        """
        Checks if active ladder is stale due to volatility drift.
        Called by MonitoringOrchestrator.process_macro_bar() on each new Dollar Bar.
        
        Returns:
            New LadderSnapshot if rebalance needed, None if ladder is still valid.
        """
        active = self._active_ladders.get(symbol)
        if not active:
            return None
        
        # Compute theoretical deep-tier price with CURRENT sigma
        deepest_template = self._structure[-1]  # 5.0σ tier
        
        if active.is_long:
            theoretical_deep = current_mark_price * (1 - (deepest_template.sigma_multiplier * current_sigma))
        else:
            theoretical_deep = current_mark_price * (1 + (deepest_template.sigma_multiplier * current_sigma))
        
        actual_deep = active.levels[-1].price
        
        if actual_deep <= 0:
            return None
        
        drift = abs(theoretical_deep - actual_deep) / actual_deep
        
        if drift > self._drift_threshold:
            logger.info(
                f"🔄 [LADDER] [{symbol}] VOLATILITY DRIFT DETECTED: {drift:.2%} "
                f"(threshold: {self._drift_threshold:.2%}). "
                f"σ_old={active.sigma_at_creation:.4f} → σ_new={current_sigma:.4f}"
            )
            
            # Generate fresh ladder centered on current price
            new_snapshot = self.generate_initial_ladder(
                entry_price=current_mark_price,
                current_sigma=current_sigma,
                is_long=active.is_long
            )
            
            self._active_ladders[symbol] = new_snapshot
            return new_snapshot
        
        return None

    @staticmethod
    def format_telegram_ladder(snapshot: LadderSnapshot, position_size_usd: float) -> str:
        """
        Formats the Sigma-Ladder for embedding in Telegram Entry Alert.
        
        Output:
            🛡️ PRE-PLACE SIGMA LADDER NOW:
            L1 (40%) → $29,250.00 | Size: $4,000
            L2 (40%) → $28,500.00 | Size: $4,000  
            L3 (20%) → $27,500.00 | Size: $2,000
            σ_db: 0.0350
        """
        direction = "⬇️ BELOW" if snapshot.is_long else "⬆️ ABOVE"
        
        lines = [f"\n🛡️ *PRE-PLACE SIGMA LADDER NOW* ({direction} entry):"]
        
        for i, level in enumerate(snapshot.levels):
            tier_size = position_size_usd * level.volume_pct
            lines.append(
                f"  L{i+1} ({level.volume_label}) → `${level.price:,.2f}` "
                f"| Size: `${tier_size:,.0f}`"
            )
        
        lines.append(f"  σ\\_db: `{snapshot.sigma_at_creation:.4f}`")
        lines.append("  ⚠️ _Place as Resting Limit Orders immediately._")
        
        return "\n".join(lines)

    @staticmethod
    def format_drift_alert(symbol: str, old_snapshot: LadderSnapshot, new_snapshot: LadderSnapshot) -> str:
        """Formats drift rebalance notification for Telegram."""
        lines = [
            f"🔄 *LADDER DRIFT ALERT: #{symbol}*",
            f"σ shifted: `{old_snapshot.sigma_at_creation:.4f}` → `{new_snapshot.sigma_at_creation:.4f}`",
            "",
            "🔁 *REBALANCE TO:*"
        ]
        
        for i, level in enumerate(new_snapshot.levels):
            old_price = old_snapshot.levels[i].price if i < len(old_snapshot.levels) else 0
            delta = level.price - old_price
            direction = "↑" if delta > 0 else "↓"
            lines.append(
                f"  L{i+1} ({level.volume_label}): "
                f"`${old_price:,.2f}` → `${level.price:,.2f}` {direction}"
            )
        
        lines.append("  ⚠️ _Cancel old orders. Re-place at new levels._")
        
        return "\n".join(lines)


# Global singleton
preemptive_ladder = PreEmptiveLadderEngine()
