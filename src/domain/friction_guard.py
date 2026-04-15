# src/domain/friction_guard.py
"""
[GEKTOR APEX v4.3] Execution Friction Guard (Peter Brown's Razor).

Mathematical filter for transaction costs. Suppresses signals
where the expected Alpha is consumed by spread + taker fees.

Philosophy: "If you don't account for transaction costs,
they will account for your capital." — Peter Brown, RenTech.
"""
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from loguru import logger


@dataclass(slots=True)
class L1Quote:
    """Atomic L1 quote snapshot for spread calculation."""
    bid: float
    ask: float
    ts: float  # monotonic timestamp of last update


class ExecutionFrictionGuard:
    """
    [GEKTOR v4.3] Transaction Cost Aware Signal Filter.

    Before any signal reaches the Operator, this guard verifies that
    the expected Alpha (derived from VPIN Z-Score magnitude) exceeds
    the total round-trip friction (spread cost + 2x taker fee).

    If friction >= alpha => signal is SUPPRESSED (not worth executing).
    """
    __slots__ = ['taker_fee_bps', 'min_alpha_bps', '_quotes', '_stale_threshold_sec']

    def __init__(self, taker_fee_bps: float = 6.0, min_alpha_bps: float = 5.0):
        """
        Args:
            taker_fee_bps: Bybit taker fee in basis points (default 6.0 = 0.06%).
            min_alpha_bps: Minimum net alpha after friction to pass filter.
        """
        self.taker_fee_bps = taker_fee_bps
        self.min_alpha_bps = min_alpha_bps
        self._quotes: Dict[str, L1Quote] = {}
        self._stale_threshold_sec = 30.0  # L1 older than 30s is unreliable

    def update_quote(self, symbol: str, bid: float, ask: float) -> None:
        """Ingest latest L1 BBO from orderbook snapshots (called from ingest_snapshot)."""
        self._quotes[symbol] = L1Quote(bid=bid, ask=ask, ts=time.monotonic())

    def is_tradable(self, symbol: str, vpin: float, dynamic_threshold: float) -> bool:
        """
        Determines if the signal has enough Alpha to survive friction.

        Expected Alpha is estimated from the VPIN excess above the dynamic threshold.
        A VPIN of 0.85 against a threshold of 0.65 means ~30% excess, which translates
        to roughly 30 bps of expected directional edge (heuristic scaling).

        Args:
            symbol: Trading pair.
            vpin: Current VPIN value.
            dynamic_threshold: Current adaptive threshold.

        Returns:
            True if signal is worth executing, False if friction kills it.
        """
        quote = self._quotes.get(symbol)

        # [FAILSAFE] If no L1 data available, let the signal through
        # (conservative approach: don't suppress valid signals due to data gaps)
        if not quote:
            logger.debug(f"⚠️ [FrictionGuard] No L1 data for {symbol}. Passing signal through.")
            return True

        # [STALE CHECK] Old quotes are unreliable
        age_sec = time.monotonic() - quote.ts
        if age_sec > self._stale_threshold_sec:
            logger.debug(f"⚠️ [FrictionGuard] L1 stale for {symbol} ({age_sec:.1f}s). Passing signal through.")
            return True

        # 1. Spread in basis points
        mid_price = (quote.ask + quote.bid) / 2.0
        if mid_price <= 0:
            return True  # Prevent division by zero

        spread_bps = ((quote.ask - quote.bid) / mid_price) * 10_000

        # 2. Total round-trip friction: spread crossing + 2x taker fee (entry + exit)
        friction_bps = spread_bps + (self.taker_fee_bps * 2)

        # 3. Expected Alpha (heuristic): excess VPIN as % of threshold, scaled to bps
        # If VPIN = 0.85, threshold = 0.65 => excess_ratio = 0.308 => ~30.8 bps expected move
        if dynamic_threshold > 0:
            excess_ratio = (vpin - dynamic_threshold) / dynamic_threshold
        else:
            excess_ratio = 0.0

        expected_alpha_bps = excess_ratio * 100.0  # Scale to basis points

        # 4. Net Alpha after friction
        net_alpha_bps = expected_alpha_bps - friction_bps

        if net_alpha_bps < self.min_alpha_bps:
            logger.warning(
                f"🛡️ [FRICTION GUARD] {symbol} Signal SUPPRESSED. "
                f"Alpha: {expected_alpha_bps:.1f} bps | "
                f"Spread: {spread_bps:.1f} bps | "
                f"Fees: {self.taker_fee_bps * 2:.1f} bps | "
                f"Friction: {friction_bps:.1f} bps | "
                f"Net: {net_alpha_bps:.1f} bps (min: {self.min_alpha_bps:.1f})"
            )
            return False

        logger.info(
            f"✅ [FRICTION GUARD] {symbol} Signal CLEARED. "
            f"Net Alpha: {net_alpha_bps:.1f} bps > {self.min_alpha_bps:.1f} bps min."
        )
        return True
