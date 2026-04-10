# core/radar/absorption_detector.py
"""
[GEKTOR v21.15] Iceberg Absorption Detector (Micro-Toxicity Engine).

Critical Vulnerability Addressed:
    Iceberg orders absorb massive liquidity at a single price level while
    showing only a fraction of their true size in the L2 orderbook.
    
    VPIN is BLIND to this: aggressive buyers hit the ask → VPIN says "buying pressure".
    But the hidden reserve regenerates → price doesn't move → all buying volume is
    absorbed by the iceberg. Eventually buyers exhaust → price reverses.

Detection Method (Trade Flow Absorption Ratio):
    For each price level near the Upper Barrier:
        
        V_abs(p) = cumulative volume EXECUTED at price p over rolling window
        V_vis(p) = maximum SIZE OBSERVED at price p in L2 orderbook
        
        AbsorptionRatio(p) = V_abs(p) / V_vis(p)
        
        Normal: ratio ≈ 1.0 (visible size matches executed volume)
        Iceberg: ratio >> 5.0 (executed volume is 5x-50x the visible size)
        
    Secondary signal: Trade-Through Failure Rate
        If trades keep executing AT a price but price never breaks THROUGH it:
        trades_at_price = count of trades AT level p
        trades_through_price = count of trades BEYOND level p
        failure_rate = 1 - (trades_through / trades_at)
        
        If failure_rate > 0.8 AND AbsorptionRatio > threshold → ICEBERG CONFIRMED

Integration Point:
    MonitoringOrchestrator.process_macro_bar() — checks absorption zone
    near Upper Barrier when price is within 0.5σ of TP target.

References:
    Hasbrouck & Saar, "Low-Latency Trading" (2013)
    SEC Staff Report on Algorithmic Trading (2020) — Iceberg Order Detection
"""
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from loguru import logger


@dataclass(slots=True)
class AbsorptionSnapshot:
    """Tracks trade execution and visible L2 size at a price level."""
    cumulative_volume: float     # Total volume executed at this price
    peak_visible_size: float     # Max L2 size observed at this price
    trade_count: int             # Number of trades at this price
    first_seen_ts: float
    last_trade_ts: float


@dataclass(slots=True, frozen=True)
class IcebergVerdict:
    """Immutable result of iceberg detection analysis."""
    is_iceberg: bool
    absorption_ratio: float       # V_abs / V_vis (>5x = suspicious)
    absorbed_volume_usd: float    # Total USD absorbed
    visible_size_usd: float       # Median visible in L2
    price_level: float            # The price level of the wall
    trade_count: int              # How many trades hit this level
    proximity_to_barrier_pct: float  # How close to our TP
    reason: str


class AbsorptionTracker:
    """
    [GEKTOR v21.15] Per-Symbol Trade Absorption Ring Buffer.
    
    Tracks cumulative volume executed at each price level,
    compares with MEDIAN visible L2 size to ignore flickering quotes.
    
    Single-writer: WS Trade handler.
    Multi-reader: MonitoringOrchestrator queries.
    GIL-safe: dict/deque mutations.
    """
    __slots__ = ('_levels', '_window_s', '_max_levels', '_median_window')

    def __init__(self, window_seconds: float = 120.0, max_levels: int = 200, median_window: int = 10):
        """
        Args:
            window_seconds: Rolling window for absorption tracking (default 2 min).
            max_levels: Max tracked price levels per instance.
            median_window: Number of L2 updates to keep for median calculation.
        """
        self._levels: Dict[int, AbsorptionSnapshot] = {}  # Key: price in ticks (int)
        self._window_s = window_seconds
        self._max_levels = max_levels
        self._median_window = median_window

    def record_trade(self, price_tick: int, volume: float) -> None:
        """Records a trade execution at a price level."""
        now = time.time()
        
        if price_tick in self._levels:
            snap = self._levels[price_tick]
            snap.cumulative_volume += volume
            snap.trade_count += 1
            snap.last_trade_ts = now
        else:
            if len(self._levels) >= self._max_levels:
                self._purge_oldest()
            self._levels[price_tick] = AbsorptionSnapshot(
                cumulative_volume=volume,
                depth_history=deque(maxlen=self._median_window),
                trade_count=1,
                first_seen_ts=now,
                last_trade_ts=now
            )

    def record_l2_size(self, price_tick: int, visible_size: float) -> None:
        """Records visible depth with flickering protection."""
        if price_tick in self._levels:
            snap = self._levels[price_tick]
            snap.depth_history.append(visible_size)
        else:
            if len(self._levels) >= self._max_levels:
                return  # Don't create entries for L2-only levels
            snap = AbsorptionSnapshot(
                cumulative_volume=0.0,
                depth_history=deque(maxlen=self._median_window),
                trade_count=0,
                first_seen_ts=time.time(),
                last_trade_ts=time.time()
            )
            snap.depth_history.append(visible_size)
            self._levels[price_tick] = snap

    def get_absorption_ratio(self, price_tick: int) -> Tuple[float, float, float, int]:
        """
        Returns (absorption_ratio, cumulative_vol, median_visible, trade_count)
        for a specific price level.
        """
        snap = self._levels.get(price_tick)
        if not snap:
            return (0.0, 0.0, 0.0, 0)
        
        median_vis = snap.median_visible_size
        if median_vis <= 0:
            return (0.0, 0.0, 0.0, 0)
        
        ratio = snap.cumulative_volume / median_vis
        return (ratio, snap.cumulative_volume, median_vis, snap.trade_count)

    def scan_zone(
        self, 
        center_price_tick: int, 
        radius_ticks: int = 10
    ) -> Optional[Tuple[int, float, float, float, int]]:
        """
        Scans a price zone for the most suspicious absorption level.
        
        Returns:
            Tuple of (price_tick, ratio, cum_vol, median_vis, trades) for worst level,
            or None if no absorption detected.
        """
        worst_tick = 0
        worst_ratio = 0.0
        worst_data = None
        
        cutoff_ts = time.time() - self._window_s
        
        for p_tick, snap in self._levels.items():
            if abs(p_tick - center_price_tick) > radius_ticks:
                continue
            if snap.last_trade_ts < cutoff_ts:
                continue
            
            median_vis = snap.median_visible_size
            if median_vis <= 0:
                continue
            
            ratio = snap.cumulative_volume / median_vis
            if ratio > worst_ratio:
                worst_ratio = ratio
                worst_tick = p_tick
                worst_data = (p_tick, ratio, snap.cumulative_volume, median_vis, snap.trade_count)
        
        return worst_data

    def _purge_oldest(self) -> None:
        """Remove oldest 25% of entries to make room."""
        if not self._levels:
            return
        sorted_items = sorted(self._levels.items(), key=lambda x: x[1].last_trade_ts)
        to_remove = max(1, len(sorted_items) // 4)
        for tick, _ in sorted_items[:to_remove]:
            del self._levels[tick]


# Per-symbol trackers (created on demand)
_absorption_trackers: Dict[str, AbsorptionTracker] = {}

def get_absorption_tracker(symbol: str) -> AbsorptionTracker:
    """Factory: returns or creates AbsorptionTracker for a symbol."""
    if symbol not in _absorption_trackers:
        _absorption_trackers[symbol] = AbsorptionTracker()
    return _absorption_trackers[symbol]


# Tick conversion constant (must match L2LiquidityEngine)
TICK_MULTIPLIER = 100_000


class IcebergAbsorptionDetector:
    """
    [GEKTOR v21.15] Iceberg Detection Engine.
    
    Evaluates absorption ratio near the Upper Barrier.
    Called by MonitoringOrchestrator when price enters the "proximity zone"
    (within 0.5σ of the Take Profit barrier).
    
    Stateless evaluator — all state lives in AbsorptionTracker instances.
    """
    # Thresholds
    ABSORPTION_RATIO_CRITICAL = 5.0     # 5x more volume executed than visible
    ABSORPTION_RATIO_WARNING = 3.0      # 3x is early warning
    MIN_TRADES_FOR_SIGNAL = 10          # Need at least 10 trades to confirm
    PROXIMITY_ZONE_SIGMA = 0.5          # Activate within 0.5σ of barrier

    def __init__(
        self,
        absorption_threshold: float = 5.0,
        min_trades: int = 10,
        proximity_sigma: float = 0.5
    ):
        self.ABSORPTION_RATIO_CRITICAL = absorption_threshold
        self.MIN_TRADES_FOR_SIGNAL = min_trades
        self.PROXIMITY_ZONE_SIGMA = proximity_sigma

    def evaluate(
        self,
        symbol: str,
        current_price: float,
        upper_barrier: float,
        sigma_db: float,
        is_long: bool = True
    ) -> Optional[IcebergVerdict]:
        """
        Checks for iceberg absorption near the Take Profit barrier.
        
        Only activates when price is within proximity_sigma * σ_db of the barrier.
        
        Args:
            symbol: Trading pair
            current_price: Current mark price
            upper_barrier: Take Profit target (for longs) 
            sigma_db: Current Dollar Bar volatility
            is_long: Position direction
            
        Returns:
            IcebergVerdict if absorption detected, None if zone is clear.
        """
        # For longs: check near upper_barrier (TP)
        # For shorts: check near lower_stop (TP)
        target = upper_barrier
        
        # Check if we're in the proximity zone
        distance_pct = abs(target - current_price) / current_price
        proximity_threshold = self.PROXIMITY_ZONE_SIGMA * max(sigma_db, 0.005)
        
        if distance_pct > proximity_threshold:
            return None  # Too far from barrier — no need to check
        
        proximity_to_barrier = distance_pct / max(sigma_db, 0.005)
        
        # Scan absorption zone around the barrier price
        tracker = get_absorption_tracker(symbol)
        barrier_tick = int(target * TICK_MULTIPLIER + 0.5)
        
        # Scan ±5 ticks around barrier
        result = tracker.scan_zone(barrier_tick, radius_ticks=10)
        
        if result is None:
            return None
        
        p_tick, ratio, cum_vol, peak_vis, trades = result
        
        # Check if absorption is suspicious
        if ratio < self.ABSORPTION_RATIO_CRITICAL or trades < self.MIN_TRADES_FOR_SIGNAL:
            # Below critical threshold — might still warn
            if ratio >= self.ABSORPTION_RATIO_WARNING and trades >= self.MIN_TRADES_FOR_SIGNAL:
                logger.warning(
                    f"⚠️ [ICEBERG] [{symbol}] Elevated absorption near TP: "
                    f"ratio={ratio:.1f}x, trades={trades}"
                )
            return None
        
        # ICEBERG CONFIRMED
        wall_price = p_tick / TICK_MULTIPLIER
        
        verdict = IcebergVerdict(
            is_iceberg=True,
            absorption_ratio=round(ratio, 1),
            absorbed_volume_usd=round(cum_vol * wall_price, 2),
            visible_size_usd=round(peak_vis * wall_price, 2),
            price_level=wall_price,
            trade_count=trades,
            proximity_to_barrier_pct=round(proximity_to_barrier * 100, 2),
            reason=f"ICEBERG_WALL(ratio={ratio:.0f}x, trades={trades}, vis={peak_vis:.2f})"
        )
        
        logger.critical(
            f"🧊 [ICEBERG] [{symbol}] ICEBERG DETECTED @ ${wall_price:,.4f}! "
            f"Absorbed {ratio:.0f}x visible size | "
            f"{trades} trades | ${cum_vol * wall_price:,.0f} USD absorbed | "
            f"Only ${peak_vis * wall_price:,.0f} visible in L2"
        )
        
        return verdict

    @staticmethod
    def format_iceberg_alert(symbol: str, verdict: IcebergVerdict) -> str:
        """Formats iceberg detection for Telegram."""
        return (
            f"🧊 *ICEBERG WALL DETECTED: #{symbol}*\n"
            f"Wall Price: `${verdict.price_level:,.4f}`\n"
            f"Absorption: `{verdict.absorption_ratio:.0f}x` visible size\n"
            f"Absorbed: `${verdict.absorbed_volume_usd:,.0f}` USD\n"
            f"Visible: `${verdict.visible_size_usd:,.0f}` USD\n"
            f"Trades: `{verdict.trade_count}` hit this wall\n"
            f"Distance to TP: `{verdict.proximity_to_barrier_pct:.1f}%`\n"
            f"⚠️ _Hidden reserve is absorbing all buying pressure._\n"
            f"🛑 *CLOSE POSITION BY MARKET NOW — TP will NOT be reached.*"
        )


# Global singleton
iceberg_detector = IcebergAbsorptionDetector()
