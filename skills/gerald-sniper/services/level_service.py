from loguru import logger
import asyncio
from typing import List, Dict
from core.levels.htf_detector import detect_swings, detect_week_extremes, detect_round_numbers, detect_kde
from core.levels.cluster import cluster_levels
from core.levels.models import HTFLevel, LevelCluster
from data.bybit_rest import BybitREST


class LevelService:
    """
    Orchestrates Multi-TF level detection and clustering.
    Matches Digash Phase 2 (Engineering Standard).
    """

    def __init__(self, rest: BybitREST):
        self.rest = rest
        self._last_scan_ts: Dict[str, float] = {}

    def get_warmup_remaining(self, symbol: str) -> float:
        """Returns seconds remaining in level warmup for symbol."""
        now = asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else __import__('time').time()
        last = self._last_scan_ts.get(symbol, 0)
        remaining = 45 - (now - last)
        return max(0, remaining)

    async def scan_symbol_levels(
        self,
        symbol: str,
        current_price: float
    ) -> List[LevelCluster]:
        """
        1. Fetch 1H, 4H, 1D Klines.
        2. Detect Swings on each TF.
        3. Cluster into unified zones.
        """
        if current_price <= 0:
            logger.warning(f"⚠️ Invalid price for {symbol}: {current_price}, returning empty levels")
            return []
            
        logger.debug(f"📐 LevelService: Scanning {symbol}...")
        
        # 1. Pipeline: 1H(100), 4H(100), D1(50)
        tasks = [
            self.rest.get_klines(symbol, "60", limit=100),
            self.rest.get_klines(symbol, "240", limit=100),
            self.rest.get_klines(symbol, "D", limit=50)
        ]
        
        klines_1h, klines_4h, klines_d1 = await asyncio.gather(*tasks)
        
        all_swings: List[HTFLevel] = []
        
        # Helper to detect and add
        def process_tf(tf_name: str, klines: List[List]):
            if not klines: return
            highs = [float(k[2]) for k in klines]
            lows  = [float(k[3]) for k in klines]
            swings = detect_swings(highs, lows, symbol, tf_name)
            all_swings.extend(swings)
            
            # Density/KDE factor for each TF
            closes = [float(k[4]) for k in klines]
            kde_levels = detect_kde(closes, symbol, tf_name)
            all_swings.extend(kde_levels)

        process_tf("60", klines_1h)
        process_tf("240", klines_4h)
        process_tf("D", klines_d1)
        
        # 3. Dynamic Factors (Psychological Rounds + Weekly Extremes)
        highs_1h = [float(k[2]) for k in klines_1h] if klines_1h else []
        lows_1h  = [float(k[3]) for k in klines_1h] if klines_1h else []
        
        dynamic_levels = []
        if highs_1h and lows_1h:
            dynamic_levels.extend(detect_week_extremes(highs_1h, lows_1h, symbol))
        
        dynamic_levels.extend(detect_round_numbers(current_price, symbol))
        all_swings.extend(dynamic_levels)

        if not all_swings:
            logger.warning(
                f"📐 {symbol}: 0 raw levels detected "
                f"(1H:{len(klines_1h) if klines_1h else 0} "
                f"4H:{len(klines_4h) if klines_4h else 0} "
                f"D1:{len(klines_d1) if klines_d1 else 0} candles). "
                f"Possible data issue."
            )
            return []

        # 2. Cluster with adaptive tolerance (0.8% base for cleaner zones)
        clusters = cluster_levels(all_swings, tolerance_pct=0.008)
        
        # 3. Filter Pipeline
        final_clusters = self._filter_levels(clusters, current_price)
        
        # v4: Fallback when standard filter returns 0 — relax thresholds
        if not final_clusters and clusters:
            logger.info(
                f"📐 {symbol}: 0 zones after standard filter "
                f"(had {len(clusters)} raw clusters). Applying fallback filters..."
            )
            final_clusters = self._filter_levels_fallback(clusters, current_price)
            for c in final_clusters:
                c.confluence_score = max(0, c.confluence_score - 1)  # Confidence penalty

        self._last_scan_ts[symbol] = asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else __import__('time').time()
        logger.info(f"📐 {symbol}: Found {len(final_clusters)} zones (price={current_price})")
        return final_clusters

    def _filter_levels(self, clusters: List[LevelCluster], current_price: float) -> List[LevelCluster]:
        """
        Filters clusters using Digash Engineering criteria:
        1. Strength >= 2.0
        2. Touches >= 2
        3. Distance within 15%
        4. Apply Proximity Penalty to score
        5. Sort by calibrated strength, take Top 8
        """
        MIN_STRENGTH = 2.0
        MIN_TOUCHES = 2
        MAX_DIST_PCT = 15.0
        MAX_LEVELS = 8

        filtered = []
        for c in clusters:
            # Filter 1: Raw Strength
            if c.strength < MIN_STRENGTH:
                continue
            
            # Filter 2: Touches
            if c.touches_total < MIN_TOUCHES:
                continue

            # Filter 3: Distance
            dist_pct = 0.0
            if current_price > 0:
                dist_pct = abs(c.price - current_price) / current_price * 100
                if dist_pct > MAX_DIST_PCT:
                    continue
            
            # Filter 4: Valid price
            if c.price <= 0:
                continue

            # BUG-NEW-3: Calibrate score with proximity penalty
            c.strength = self.compute_level_score(c, current_price, dist_pct)
            filtered.append(c)

        # Sort by calibrated strength (descending)
        filtered.sort(key=lambda x: x.strength, reverse=True)

        return filtered[:MAX_LEVELS]

    @staticmethod
    def compute_level_score(cluster: LevelCluster, current_price: float, dist_pct: float) -> float:
        """
        Score уровня с учётом proximity.
        Компоненты:
        1. base_score      = strength * touches
        2. confluence_bonus= confluence * 5
        3. proximity_mult  = 1.0 (0-1%), 0.8 (1-3%), 0.6 (3-5%), 0.4 (5-10%), 0.2 (>10%)
        """
        # Base uses sum of strengths * total touches
        base = cluster.strength * cluster.touches_total
        confluence_bonus = cluster.confluence_score * 5.0

        if current_price > 0:
            if dist_pct < 1.0:
                proximity_mult = 1.0
            elif dist_pct < 3.0:
                proximity_mult = 0.8
            elif dist_pct < 5.0:
                proximity_mult = 0.6
            elif dist_pct < 10.0:
                proximity_mult = 0.4
            else:
                proximity_mult = 0.2
        else:
            proximity_mult = 1.0

        score = (base + confluence_bonus) * proximity_mult
        return round(score, 1)

    def _filter_levels_fallback(
        self, clusters: List[LevelCluster], current_price: float
    ) -> List[LevelCluster]:
        """
        v4 Fallback filter — relaxed thresholds when standard filter yields 0.
        Produces levels with lower confidence.
        """
        MIN_STRENGTH = 1.5   # Relaxed from 2.0
        MIN_TOUCHES = 1      # Relaxed from 2
        MAX_DIST_PCT = 20.0  # Relaxed from 15.0
        MAX_LEVELS = 5

        filtered = []
        for c in clusters:
            if c.strength < MIN_STRENGTH:
                continue
            if c.touches_total < MIN_TOUCHES:
                continue

            dist_pct = 0.0
            if current_price > 0:
                dist_pct = abs(c.price - current_price) / current_price * 100
                if dist_pct > MAX_DIST_PCT:
                    continue

            if c.price <= 0:
                continue

            c.strength = self.compute_level_score(c, current_price, dist_pct)
            filtered.append(c)

        filtered.sort(key=lambda x: x.strength, reverse=True)
        return filtered[:MAX_LEVELS]

