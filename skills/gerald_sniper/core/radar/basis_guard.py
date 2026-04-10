# core/radar/basis_guard.py
"""
[GEKTOR v21.15] Spot-Premium Guard (Basis Collapse Filter).

Critical Vulnerability Addressed:
    Perpetual Futures price can deviate from Spot (Index Price) during
    localized derivative Flash Crashes caused by whale liquidations.
    The Sigma-Ladder sits in the futures orderbook and can be harvested
    by these phantom dislocations while the underlying asset (Spot) 
    remains perfectly stable.

Architecture:
    1. IndexPriceTracker — Ingests Index Price from Bybit ticker WS stream.
       Bybit V5 tickers provide 'indexPrice' alongside 'lastPrice' and 'markPrice'.
    
    2. SpotPremiumGuard — Computes Basis = (Futures - Spot) / Spot.
       When |Basis| exceeds a dynamic threshold during a crash,
       it emits a DERIVATIVE_DISLOCATION alert to the Operator.

    3. Integration:
       - MonitoringOrchestrator.process_macro_bar() checks basis before confirming ladder fill.
       - bybit_ws._handle_ticker() feeds indexPrice into the tracker.

Mathematical Foundation:
    Basis(t) = [P_futures(t) - P_index(t)] / P_index(t)
    
    Under normal conditions:
        |Basis| ∈ [0, 0.001]  (0 to 0.1% — maintained by arbitrageurs)
    
    Dislocation threshold:
        |Basis| > σ_basis * k  (where σ_basis is rolling std of Basis, k = 3.0)
    
    During legitimate crash (Spot ALSO falls):
        Basis stays near 0 → Ladder fills are JUSTIFIED.
    
    During phantom dislocation (futures-only crash):
        |Basis| explodes → Ladder fills are TOXIC → CANCEL LADDER.

References:
    Hasbrouck (2003): "Intraday Price Formation in U.S. Equity Index Markets"
    CME Group: "Understanding Basis Risk in Futures Trading"
"""
import time
import math
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from loguru import logger


@dataclass(slots=True)
class BasisSnapshot:
    """Single observation of Spot-Futures basis."""
    futures_price: float
    index_price: float    # Spot aggregate from exchange
    basis_pct: float      # (futures - index) / index
    timestamp: float


class IndexPriceTracker:
    """
    [GEKTOR v21.15] Ring-Buffer Index (Spot) Price Tracker.
    
    Ingests Bybit V5 'indexPrice' from ticker WebSocket stream.
    Provides the Spot reference price for Basis computation.
    
    Data Source: Bybit V5 tickers topic → 'indexPrice' field.
    Update Cadence: Same as ticker stream (~100ms for subscribed symbols).
    """
    __slots__ = ('_prices', '_max_history')

    def __init__(self, max_history: int = 120):
        """
        Args:
            max_history: Max price observations per symbol (default ~2 min at 1/s throttle).
        """
        self._prices: Dict[str, deque] = {}
        self._max_history = max_history

    def record(self, symbol: str, index_price: float, futures_price: float) -> None:
        """Records an index/futures price pair from ticker data."""
        if index_price <= 0 or futures_price <= 0:
            return

        if symbol not in self._prices:
            self._prices[symbol] = deque(maxlen=self._max_history)

        basis_pct = (futures_price - index_price) / index_price

        self._prices[symbol].append(BasisSnapshot(
            futures_price=futures_price,
            index_price=index_price,
            basis_pct=basis_pct,
            timestamp=time.time()
        ))

    def get_current_index_price(self, symbol: str) -> float:
        """Returns most recent index (spot) price."""
        buf = self._prices.get(symbol)
        if not buf:
            return 0.0
        return buf[-1].index_price

    def get_current_basis(self, symbol: str) -> float:
        """Returns most recent basis value."""
        buf = self._prices.get(symbol)
        if not buf:
            return 0.0
        return buf[-1].basis_pct

    def get_basis_stats(self, symbol: str, lookback: int = 60) -> Tuple[float, float]:
        """
        Returns (mean_basis, std_basis) over recent history.
        Used by SpotPremiumGuard for dynamic threshold computation.
        """
        buf = self._prices.get(symbol)
        if not buf or len(buf) < 5:
            return (0.0, 0.0001)  # Default: assume tight basis with minimal std

        n = min(lookback, len(buf))
        recent = list(buf)[-n:]
        values = [s.basis_pct for s in recent]

        mean_b = sum(values) / len(values)
        variance = sum((v - mean_b) ** 2 for v in values) / len(values)
        std_b = math.sqrt(variance) if variance > 0 else 0.0001

        return (mean_b, std_b)


# Global singleton
index_tracker = IndexPriceTracker()


class SpotPremiumGuard:
    """
    [GEKTOR v21.15] Derivative Dislocation Detector.
    
    Monitors Basis = (Futures - Spot) / Spot.
    Triggers DERIVATIVE_DISLOCATION alarm when basis exceeds dynamic Z-score threshold,
    indicating that the futures crash is NOT reflected in the underlying asset.
    
    Integration Points:
        - MonitoringOrchestrator.process_macro_bar() → checks before confirming ladder execution
        - PreEmptiveLadderEngine → basis guard can pause/cancel ladder
    """
    # Thresholds
    Z_SCORE_CRITICAL = 3.0          # Basis Z-score above this = DISLOCATION
    ABSOLUTE_BASIS_CRITICAL = 0.005  # 0.5% absolute basis floor (even without history)
    MIN_INDEX_FRESHNESS_S = 30.0    # Index price must be <30s old

    def __init__(
        self,
        tracker: Optional[IndexPriceTracker] = None,
        z_threshold: float = 3.0,
        abs_threshold: float = 0.005
    ):
        self._tracker = tracker or index_tracker
        self.Z_SCORE_CRITICAL = z_threshold
        self.ABSOLUTE_BASIS_CRITICAL = abs_threshold

    def evaluate(self, symbol: str, futures_price: float) -> "DislocationVerdict":
        """
        Core evaluation: Is the current futures price move a phantom dislocation
        or a real crash affecting the underlying?
        
        Args:
            symbol: Trading pair
            futures_price: Current futures mark/last price
            
        Returns:
            DislocationVerdict with safety recommendation.
        """
        index_price = self._tracker.get_current_index_price(symbol)
        
        # Safety: If no index data, we can't determine dislocation → assume safe
        if index_price <= 0:
            return DislocationVerdict(
                is_dislocated=False,
                basis_pct=0.0,
                basis_z_score=0.0,
                index_price=0.0,
                futures_price=futures_price,
                reason="NO_INDEX_DATA"
            )

        # Check index freshness — stale index is unreliable
        buf = self._tracker._prices.get(symbol)
        if buf:
            age = time.time() - buf[-1].timestamp
            if age > self.MIN_INDEX_FRESHNESS_S:
                return DislocationVerdict(
                    is_dislocated=False,
                    basis_pct=0.0,
                    basis_z_score=0.0,
                    index_price=index_price,
                    futures_price=futures_price,
                    reason=f"INDEX_STALE({age:.0f}s)"
                )

        # Compute current basis
        current_basis = (futures_price - index_price) / index_price
        
        # Dynamic Z-score threshold
        mean_basis, std_basis = self._tracker.get_basis_stats(symbol)
        basis_z = abs(current_basis - mean_basis) / max(std_basis, 1e-6)
        
        # Dual-trigger: BOTH Z-score AND absolute threshold must fire
        # This prevents false alarms in assets with naturally wider basis
        is_dislocated = (
            basis_z > self.Z_SCORE_CRITICAL and 
            abs(current_basis) > self.ABSOLUTE_BASIS_CRITICAL
        )
        
        reason = "NORMAL"
        if is_dislocated:
            reason = f"DERIVATIVE_DISLOCATION(basis={current_basis:+.3%}, z={basis_z:.1f}σ)"
            logger.critical(
                f"🌊 [BASIS GUARD] [{symbol}] {reason} | "
                f"Futures=${futures_price:,.2f} vs Spot=${index_price:,.2f} | "
                f"Δ={abs(futures_price - index_price):,.2f}"
            )
        
        return DislocationVerdict(
            is_dislocated=is_dislocated,
            basis_pct=round(current_basis, 6),
            basis_z_score=round(basis_z, 2),
            index_price=index_price,
            futures_price=futures_price,
            reason=reason
        )

    @staticmethod
    def format_dislocation_alert(symbol: str, verdict: "DislocationVerdict") -> str:
        """Formats dislocation alert for Telegram."""
        return (
            f"🌊 *DERIVATIVE DISLOCATION: #{symbol}*\n"
            f"Futures: `${verdict.futures_price:,.2f}`\n"
            f"Spot (Index): `${verdict.index_price:,.2f}`\n"
            f"Basis: `{verdict.basis_pct:+.3%}` (Z={verdict.basis_z_score:.1f}σ)\n"
            f"⚠️ _Futures crash NOT confirmed by Spot._\n"
            f"🛑 *CANCEL RESTING LADDER ORDERS IMMEDIATELY.*\n"
            f"_This is a phantom dislocation — do NOT let fills execute._"
        )


@dataclass(slots=True, frozen=True)
class DislocationVerdict:
    """Immutable result of basis dislocation analysis."""
    is_dislocated: bool
    basis_pct: float
    basis_z_score: float
    index_price: float
    futures_price: float
    reason: str


# Global singleton
basis_guard = SpotPremiumGuard()
