# core/radar/macro_radar.py
"""
[GEKTOR v8.1] Macro Anomaly Radar — Signal Purity Engine.

Architecture: Two-Phase Scan + Cold Start Hydration + One-Click Entry
  Phase 0 (Hydration):   50 REST calls at startup → /v5/market/open-interest
                          Pre-fills OI ring buffer so radar is ready at t=0

  Phase 1 (Broad Scan):  1 REST call → GET /v5/market/tickers → ALL 300 tickers
                          Compare OI + Volume snapshots against previous cycle
                          Filter: turnover > $1M absolute + OI Delta > 5% + Price Move > 2%
                          Output: 3-7 candidates (Shortlist)

  Phase 2 (Deep Scan):   3-7 REST calls → GET /v5/market/kline (5m candles)
                          Validate RVOL on 5m turnover (not 24h average)
                          Confirm wick structure (no full rejection candles)
                          Output: 0-3 confirmed anomalies → Telegram with Inline Keyboard

  One-Click Entry:       Telegram Inline Keyboard (⚡ LONG / ⚡ SHORT / 🚫 ПАС)
                          Callback → OMS Limit IOC order → 0.5s reaction time

Budget: 8-10 REST requests per minute (vs 120+ in old RadarV2)
Proxy-Safe: Single-connection sequential flow, no parallel storms.
"""

import asyncio
import hashlib
import json
import time
from typing import Dict, List, Optional, Any, Set
from pydantic import BaseModel, Field
from loguru import logger

from data.bybit_rest import BybitREST
from utils.math_utils import log_throttler


def _generate_signal_id(symbol: str, direction: str) -> str:
    """Deterministic short ID for callback_data (max 64 bytes in TG)."""
    raw = f"{symbol}:{direction}:{int(time.time())}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]


class MacroSignal(BaseModel):
    """
    [GEKTOR v11.9] Macro Anomaly Signal.
    Focus on Volume-Flow alignment + Predictive Projections.
    """
    symbol: str
    direction: int        # 1 for LONG, -1 for SHORT
    rvol: float           # Relative Volume
    imbalance: float      # Buy/Sell Ratio
    z_score: float        # Deviation
    cvd_alignment: bool   # Does the impulse align with global CVD?
    price: float = 0.0
    timestamp: float = Field(default_factory=time.time)
    
    # [GEKTOR v11.7] Analysis Fields
    signal_type: Optional[str] = None # ICEBERG_BUY, EXHAUSTION, etc.
    regime: str = "NORMAL"            # EXPANSION, COMPRESSION
    vr: float = 1.0                   # Volatility Ratio
    atr: float = 0.0
    targets: Dict[str, float] = Field(default_factory=dict) # {"sl": 10.5, "tp": 12.0}
    liquidity_tier: str = "C"
    tick_size: float = 0.001

    # [GEKTOR v11.7] Multi-Vector Historical primitives (for IPC)
    hist_p: List[float] = Field(default_factory=list)
    hist_c: List[float] = Field(default_factory=list)
    hist_v: List[float] = Field(default_factory=list)

    @property
    def direction_str(self) -> str:
        return "LONG" if self.direction == 1 else "SHORT"

    @property
    def is_structural_break(self) -> bool:
        """Structural Break = High Z-Score + CVD Alignment."""
        return abs(self.z_score) > 4.0 and self.cvd_alignment


class OISnapshotStore:
    """
    In-memory ring buffer of OI snapshots for Delta calculation.
    
    Stores last N snapshots (default 10 = 10 minutes at 1 scan/min).
    Delta is calculated against the oldest available snapshot,
    giving us a ~10 minute OI Delta window.
    """
    
    def __init__(self, lookback_cycles: int = 10):
        self._lookback = lookback_cycles
        # {symbol: deque([{oi, turnover, price, ts}, ...])}
        self._snapshots: Dict[str, list] = {}
    
    def record(self, symbol: str, oi: float, turnover_24h: float, price: float):
        """Record current OI and turnover snapshot."""
        if symbol not in self._snapshots:
            self._snapshots[symbol] = []
        
        history = self._snapshots[symbol]
        history.append({
            "oi": oi,
            "turnover": turnover_24h,
            "price": price,
            "ts": time.time()
        })
        
        # Keep only last N entries
        if len(history) > self._lookback:
            self._snapshots[symbol] = history[-self._lookback:]
    
    def get_oi_delta_pct(self, symbol: str) -> Optional[float]:
        """
        Returns OI change (%) between oldest and current snapshot.
        Returns None if not enough history (< 2 snapshots).
        """
        history = self._snapshots.get(symbol)
        if not history or len(history) < 2:
            return None
        
        old_oi = history[0]["oi"]
        new_oi = history[-1]["oi"]
        
        if old_oi <= 0:
            return None
        
        return ((new_oi - old_oi) / old_oi) * 100.0
    
    def get_turnover_baseline(self, symbol: str) -> Optional[float]:
        """
        Returns average 24h turnover across stored snapshots.
        Used as baseline for RVOL comparison.
        """
        history = self._snapshots.get(symbol)
        if not history or len(history) < 3:
            return None
        
        turnovers = [s["turnover"] for s in history[:-1]]  # Exclude current
        return sum(turnovers) / len(turnovers) if turnovers else None
    
    @property
    def symbols_tracked(self) -> int:
        return len(self._snapshots)
    
    @property
    def is_warmed_up(self) -> bool:
        """Need at least 3 cycles to have meaningful OI deltas."""
        if not self._snapshots:
            return False
        # Check if at least some symbols have 3+ snapshots
        ready_count = sum(1 for h in self._snapshots.values() if len(h) >= 3)
        return ready_count > 10  # At least 10 symbols with history


import numpy as np
import scipy.stats as stats
from concurrent.futures import ProcessPoolExecutor
from core.events.events import AlertEvent
from core.events.nerve_center import bus
from collections import deque

# [GEKTOR v11.7] Top-level functions for OPTIMIZED IPC (No Pydantic/Objects)
def _cpu_bound_heavy_analysis(p: List[float], c: List[float], v: List[float], 
                               tick: float, lookback: int) -> dict:
    """
    [GEKTOR v11.7] Raw-Primitive IPC Math (Zero-Object Serialization).
    Uses Dynamic Noise Floor based on Tick Size to defeat the Relativity Trap.
    """
    res = {"div": None, "regime": "NORMAL", "vr": 1.0}
    if len(p) < lookback: return res
    
    p_arr = np.array(p, dtype=np.float32)
    c_arr = np.array(c, dtype=np.float32)
    v_arr = np.array(v, dtype=np.float32)
    
    # 1. DYNAMIC NOISE ANCHOR (v11.7)
    # Anchor = (Min Step / Current Price)^2. Absolute floor for variance.
    curr_p = p_arr[-1]
    noise_floor = (tick / curr_p) ** 2
    
    # 2. VOLATILITY CLUSTERING (Fractal Anchor)
    rets = np.diff(p_arr) / (p_arr[:-1] + 1e-12)
    s_win, l_win = 20, 100
    if len(rets) >= l_win:
        s_var = np.var(rets[-s_win:]) + noise_floor
        l_var = np.var(rets[-l_win:]) + noise_floor
        vr = (s_var) / (l_var)
        res["vr"] = float(vr)
        if vr > 2.0: res["regime"] = "EXPANSION"
        elif vr < 0.5: res["regime"] = "COMPRESSION"

    # 3. ROBUST DIVERGENCE (Iceberg/Exhaustion)
    p_min, p_max = np.percentile(p_arr, [5, 95])
    c_min, c_max = np.percentile(c_arr, [5, 95])
    norm_p = (p_arr - p_min) / (p_max - p_min + 1e-9)
    norm_c = (c_arr - c_min) / (c_max - c_min + 1e-9)
    
    x = np.arange(len(p_arr))
    p_slope = np.polyfit(x, norm_p, 1)[0]
    c_slope = np.polyfit(x, norm_c, 1)[0]
    
    s_diff = c_slope - p_slope
    mean_v = np.mean(v_arr[-5:])
    
    if s_diff > 0.08 and p_slope <= 0.02:
        res["div"] = "ICEBERG_SELL" if mean_v > 2.5 else "EXHAUSTION_SHORT"
    elif s_diff < -0.08 and p_slope >= -0.02:
        res["div"] = "ICEBERG_BUY" if mean_v > 2.5 else "EXHAUSTION_LONG"
        
    return res

class AdvisoryTargetCalculator:
    """[GEKTOR v11.7] Predictive SL/TP Generator for Radar Projections."""
    @staticmethod
    def calculate(price: float, atr: float, regime: str, direction: int) -> dict:
        mult = 2.0 if regime == "EXPANSION" else 1.2
        stop = atr * mult
        take = atr * (mult * 1.5)
        if direction == 1:
            return {"sl": price - stop, "tp": price + take}
        return {"sl": price + stop, "tp": price - take}

class MathOrchestrator:
    """[GEKTOR v11.7] IPC Math Controller. Passes only primitives."""
    def __init__(self, max_workers: int = 4):
        self._pool = ProcessPoolExecutor(max_workers=max_workers)
        
    async def analyze_multivector(self, symbol: str, prices: List[float], 
                                  cvds: List[float], rvols: List[float], 
                                  tick: float, lookback: int):
        loop = asyncio.get_event_loop()
        try:
            # GEKTOR v11.7: Deep-pickling prevention (Lists of primitives only)
            return await loop.run_in_executor(
                self._pool, _cpu_bound_heavy_analysis, prices, cvds, rvols, tick, lookback
            )
        except Exception as e:
            logger.error(f"❌ [MathOrchestrator] IPC Error for {symbol}: {e}")
            return {"div": None, "regime": "ERROR", "vr": 1.0}

class RobustMath:
    """[GEKTOR v11.5] Outlier-Resistant Statistical Engine."""
    @staticmethod
    def get_z_score_robust(value: float, window: np.ndarray) -> float:
        """MAD-based Z-Score. Immune to poisoning."""
        if len(window) < 10: return 0.0
        median = np.median(window)
        mad = np.median(np.abs(window - median))
        if mad < 1e-9: return 0.0
        return (value - median) / (mad * 1.4826)

class RollingSystemicShockDetector:
    """
    [GEKTOR v11.3] Sliding Window Systemic Shock Detector.
    Tracks 'Universe Coverage %' instead of magic numbers.
    """
    def __init__(self, universe_size: int, impact_threshold_pct: float = 0.15, window_sec: float = 3.0):
        self._universe_size = universe_size
        self._threshold = max(3, int(universe_size * impact_threshold_pct))
        self._window = window_sec
        # (monotonic, asset, direction)
        self._signal_window: deque[Tuple[float, str, int]] = deque()
        self._lock = asyncio.Lock()

    async def check(self, signal: MacroSignal) -> bool:
        """Registers a signal and returns True if universe impact threshold is crossed."""
        now = time.monotonic()
        async with self._lock:
            self._signal_window.append((now, signal.symbol, signal.direction))
            
            # Flush expired signals
            while self._signal_window and now - self._signal_window[0][0] > self._window:
                self._signal_window.popleft()
            
            # Directional coverage
            unique_assets = {s[1] for s in self._signal_window if s[2] == signal.direction}
            
            if len(unique_assets) >= self._threshold:
                logger.critical(f"⚠️ [SystemicShock] {len(unique_assets)} signals in {self._window}s! Direction: {signal.direction}")
                self._signal_window.clear() # Reset after trigger
                return True
            return False

class GlobalDirectionalMute:
    """[GEKTOR v11.3] Intelligent Directional Guard (Allows V-Reversals)."""
    def __init__(self, redis: Any, ttl: int = 900):
        self.redis = redis
        self.ttl = ttl

    async def is_muted(self, direction: int) -> bool:
        mute = await self.redis.get("macro:global_mute_dir")
        if not mute: return False
        # Block only if signal matches the shock direction
        return int(mute) == direction

    async def apply_shock(self, direction: int):
        # Overwrites any existing mute (allows fast V-reversal filters)
        await self.redis.set("macro:global_mute_dir", str(direction), ex=self.ttl)

class CVDAggregator:
    """[GEKTOR v11.1] Cumulative Volume Delta Engine — Pre-filtering candidate."""
    def __init__(self):
        # symbol -> {cumulative_delta, last_update}
        self._states: Dict[str, dict] = {}

    def get_alignment(self, symbol: str, direction: int) -> bool:
        """Returns True if the current impulse direction matches global CVD trend."""
        # TODO: Implement real-time CVD tracking
        return True # Default to True until CVD-pipe is wired

class MacroRadar:
    """
    [GEKTOR v8.0] Two-Phase Macro Anomaly Scanner.
    
    Designed for Hostile Environment (laggy proxy, REST-only).
    Uses MINIMAL API calls to detect institutional-grade anomalies.
    
    Budget: ~8 REST requests per scan cycle (vs 120+ in RadarV2).
    """
    
    def __init__(
        self,
        rest: BybitREST,
        config: dict,
        scan_interval_sec: float = 60.0,
        dispatcher: Optional[Any] = None
    ):
        self.rest = rest
        self.config = config
        self.scan_interval = scan_interval_sec
        self.guard = AtomicDirectionalGuard(bus.redis, lock_ttl_sec=config.get("lock_ttl_sec", 14400))
        self.cvd = CVDAggregator()
        self.orchestrator = MathOrchestrator(max_workers=config.get("math_processes", 4))
        self.advisory = AdvisoryTargetCalculator()
        
        # [GEKTOR v11.7] Dynamic Risk & Robust Statistics
        self.robust_math = RobustMath()
        self.universe_size = config.get("universe_size", 50)
        self.shock_detector = RollingSystemicShockDetector(universe_size=self.universe_size)
        self.global_mute = GlobalDirectionalMute(bus.redis, ttl=900)
        
        # Simplified thresholds
        self.min_turnover_24h = config.get("min_turnover_24h_usd", 10_000_000)
        self.min_z_score = config.get("min_z_score", 3.0)
        self.base_z_score = self.min_z_score
        self.min_imbalance = config.get("min_imbalance", 2.5)

        # [GEKTOR v11.9] Adaptive Sensitivity Engine
        self._last_alert_time = time.time()
        self._sensitivity_timer = time.time()

        # Stats
        self.stats = {"scans": 0, "anomalies": 0, "alerts": 0}
    
    async def start(self):
        """Main scan loop. Runs until stopped."""
        self._running = True
        logger.info(
            f"📡 [MacroRadar] ONLINE — Scan every {self.scan_interval}s | "
            f"Min Turnover: ${self.min_turnover_24h:,.0f} | "
            f"RVOL: >{self.min_rvol} | OI Δ: >{self.min_oi_delta_pct}%"
        )
        
        # ── [GEKTOR v8.1] Cold Start Hydration ────────────────────────
        # Pre-fill OI ring buffer from historical data so radar is
        # ready at t=0 instead of waiting 10 minutes for warmup.
        await self._hydrate_oi_store()
        
        while self._running:
            try:
                cycle_start = time.monotonic()
                
                signals = await self._scan_cycle()
                
                if signals:
                    await self._dispatch_signals(signals)
                
                elapsed = time.monotonic() - cycle_start
                sleep_time = max(1.0, self.scan_interval - elapsed)
                
                # Periodic status log
                if self._scan_count % 10 == 0 and self._scan_count > 0:
                    logger.info(
                        f"📡 [MacroRadar] Cycle #{self._scan_count} | "
                        f"Elapsed: {elapsed:.1f}s | "
                        f"OI Store: {self.oi_store.symbols_tracked} symbols | "
                        f"Warmed: {'✅' if self.oi_store.is_warmed_up else '⏳'} | "
                        f"Total Anomalies: {self.stats['anomalies_found']}"
                    )
                
                await asyncio.sleep(sleep_time)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [MacroRadar] Scan cycle error: {e}")
                await asyncio.sleep(10)  # Back off on error
    
    def stop(self):
        self._running = False
        logger.info("🛑 [MacroRadar] Stopped.")
    
    # =========================================================================
    # PHASE 0: COLD START HYDRATION (eliminates 10-min blindness on restart)
    # =========================================================================
    
    async def _hydrate_oi_store(self):
        """
        [GEKTOR v8.1] Cold Start Hydration.
        
        On startup, fetches historical OI data for top-50 liquid symbols
        using /v5/market/open-interest endpoint (5min intervals, 3 points = 15 min).
        This pre-fills the ring buffer so OI Delta is available immediately.
        
        Budget: ~50 REST calls (one-time at startup, sequential with 100ms gaps).
        """
        logger.info("🔄 [MacroRadar] Cold Start Hydration — pre-loading OI history...")
        t0 = time.monotonic()
        
        try:
            # Step 1: Get current tickers to identify top-50 by turnover
            tickers = await self.rest.get_tickers()
            self.stats["api_calls"] += 1
            
            if not tickers:
                logger.warning("⚠️ [Hydration] Tickers empty. Radar starts cold.")
                return
            
            # Filter and sort by turnover
            liquid = [
                t for t in tickers
                if t.get("symbol", "").endswith("USDT")
                and float(t.get("turnover24h", 0)) >= self.min_turnover_24h
                and t.get("symbol") not in self.blacklist
            ]
            liquid.sort(key=lambda t: float(t.get("turnover24h", 0)), reverse=True)
            top_symbols = liquid[:50]  # Top-50 by liquidity
            
            # Step 2: Also record current snapshot from tickers (free data)
            for t in liquid:
                sym = t.get("symbol", "")
                oi = float(t.get("openInterest", 0))
                turnover = float(t.get("turnover24h", 0))
                price = float(t.get("lastPrice", 0))
                if oi > 0:
                    self.oi_store.record(sym, oi, turnover, price)
            
            # Step 3: Fetch historical OI for top-50 (sequential, not parallel)
            hydrated = 0
            for t in top_symbols:
                sym = t["symbol"]
                try:
                    resp = await self.rest._request(
                        "GET", "/v5/market/open-interest",
                        params={
                            "category": "linear",
                            "symbol": sym,
                            "intervalTime": "5min",
                            "limit": 3  # 3 points = 15 minutes of history
                        }
                    )
                    self.stats["api_calls"] += 1
                    
                    oi_list = resp.get("list", []) if isinstance(resp, dict) else []
                    
                    # Bybit returns newest first, we need chronological order
                    for item in reversed(oi_list):
                        historical_oi = float(item.get("openInterest", 0))
                        if historical_oi > 0:
                            # Record with current turnover/price as approximation
                            turnover = float(t.get("turnover24h", 0))
                            price = float(t.get("lastPrice", 0))
                            self.oi_store.record(sym, historical_oi, turnover, price)
                    
                    hydrated += 1
                    
                except Exception as e:
                    logger.debug(f"⚠️ [Hydration] OI fetch failed for {sym}: {e}")
                
                # Gentle pacing: 100ms between requests to avoid rate limits
                await asyncio.sleep(0.1)
            
            elapsed = time.monotonic() - t0
            logger.success(
                f"✅ [MacroRadar] Hydration DONE — {hydrated}/{len(top_symbols)} symbols | "
                f"{elapsed:.1f}s | OI store: {self.oi_store.symbols_tracked} symbols | "
                f"Warmed: {'✅' if self.oi_store.is_warmed_up else '⏳'}"
            )
            
        except Exception as e:
            logger.error(f"❌ [Hydration] Failed: {e}. Radar starts cold (10 min warmup).")
    
    # =========================================================================
    # PHASE 1: BROAD SCAN (1 API call)
    # =========================================================================
    
    async def _scan_cycle(self) -> List[MacroSignal]:
        """
        Complete two-phase scan cycle.
        
        Phase 1: 1 REST call → filter candidates
        Phase 2: N REST calls → deep validate candidates
        
        Returns list of confirmed MacroSignal objects.
        """
        self._scan_count += 1
        self.stats["scans"] += 1
        
        # ── PHASE 1: Broad Scan ──────────────────────────────────────────────
        t0 = time.monotonic()
        
        tickers = await self.rest.get_tickers()
        self.stats["api_calls"] += 1
        
        if not tickers:
            logger.warning("⚠️ [MacroRadar] Empty tickers response. Skipping cycle.")
            return []
        
        t_fetch = time.monotonic() - t0
        
        # ── [GEKTOR v11.9] Self-Correcting Threshold ──────────────────
        # Calibration: Every 15 minutes of silence, lower Z-Score by 0.1
        # Upon any alert, reset to base value.
        now = time.time()
        silence_duration = now - self._last_alert_time
        if silence_duration > 1800 and (now - self._sensitivity_timer) > 900:
            self.min_z_score = max(1.5, self.min_z_score - 0.2)
            self._sensitivity_timer = now
            logger.info(f"⚡ [MacroRadar] Sensitivity Boost: Z-Score dropped to {self.min_z_score:.1f}σ (Silence: {silence_duration/60:.0f}m)")
        
        # Record ALL tickers into OI store (building history for Delta calc)
        candidates = []
        universe_size = 0
        
        for t in tickers:
            symbol = t.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            if symbol in self.blacklist:
                continue
            
            turnover_24h = float(t.get("turnover24h", 0))
            oi = float(t.get("openInterest", 0))
            price = float(t.get("lastPrice", 0))
            
            # Filter: minimum liquidity floor
            if turnover_24h < self.min_turnover_24h or price <= 0:
                continue
            
            universe_size += 1
            
            # Record snapshot for OI Delta tracking
            self.oi_store.record(symbol, oi, turnover_24h, price)
            
            # Skip scoring until OI store is warmed up (need history)
            if not self.oi_store.is_warmed_up:
                continue
            
            # ── Phase 1 Filters ──────────────────────────────────────────
            
            # 1. OI Delta check (vs oldest stored snapshot)
            oi_delta = self.oi_store.get_oi_delta_pct(symbol)
            if oi_delta is None or abs(oi_delta) < self.min_oi_delta_pct:
                continue
            
            # 2. Price movement check (24h % change from tickers endpoint)
            price_24h_pct = float(t.get("price24hPcnt", 0)) * 100  # Convert to %
            if abs(price_24h_pct) < self.min_price_move_pct:
                continue
            
            # 3. Turnover anomaly (current 24h vs stored baseline)
            baseline_turnover = self.oi_store.get_turnover_baseline(symbol)
            if baseline_turnover and baseline_turnover > 0:
                turnover_spike = turnover_24h / baseline_turnover
                # 24h turnover is relatively stable, a 1.5x jump is significant
                if turnover_spike < 1.3:  # Minimum 30% above baseline
                    continue
            
            # ── CANDIDATE FOUND ──────────────────────────────────────────
            funding = float(t.get("fundingRate", 0))
            
            # Liquidity Tier
            tier = "D"
            if turnover_24h > 100_000_000: tier = "A"
            elif turnover_24h > 30_000_000: tier = "B"
            elif turnover_24h > 10_000_000: tier = "C"
            
            candidates.append({
                "symbol": symbol,
                "price": price,
                "turnover_24h": turnover_24h,
                "oi_delta_pct": oi_delta,
                "price_24h_pct": price_24h_pct,
                "funding_rate": funding,
                "tier": tier,
            })
        
        if not self.oi_store.is_warmed_up:
            if self._scan_count % 3 == 0:
                logger.info(
                    f"⏳ [MacroRadar] Warming up OI store... "
                    f"Cycle #{self._scan_count} | Universe: {universe_size} | "
                    f"Fetch: {t_fetch:.1f}s"
                )
            return []
        
        if not candidates:
            return []
        
        logger.info(
            f"🔍 [MacroRadar P1] {len(candidates)} candidates from {universe_size} universe | "
            f"Fetch: {t_fetch:.1f}s"
        )
        
        # ── PHASE 2: Deep Scan (N API calls for N candidates) ────────────
        confirmed = []
        for cand in candidates[:10]:  # Hard cap: max 10 deep scans per cycle
            signal = await self._deep_validate(cand)
            if signal:
                confirmed.append(signal)
        
        return confirmed
    
    # =========================================================================
    # PHASE 2: DEEP SCAN (1 API call per candidate)
    # =========================================================================
    
    async def _deep_validate(self, candidate: dict) -> Optional[MacroSignal]:
        """
        Phase 2: Fetch 5-minute klines for a candidate to confirm anomaly.
        
        Validates:
        1. RVOL on 5m candle turnover (not 24h average)
        2. Absolute turnover floor ($500K+ per candle)
        3. Candle body structure (reject full-wick rejections)
        
        Costs: 1 API call.
        """
        symbol = candidate["symbol"]
        
        # Cooldown check: don't alert same symbol within 5 minutes
        last_alert_ts = self._last_alerts.get(symbol, 0)
        if time.time() - last_alert_ts < self._alert_cooldown_sec:
            return None
        
        try:
            # Fetch last 12 five-minute candles (1 hour of data)
            # Format: [startTime, open, high, low, close, volume, turnover]
            klines = await self.rest.get_klines(symbol, "5", limit=12)
            self.stats["api_calls"] += 1
            
            if not klines or len(klines) < 5:
                return None
            
            # Current candle = last element (klines are oldest→newest after reverse in get_klines)
            current = klines[-1]
            candle_turnover = float(current[6])  # turnover in USDT
            
            # ── Filter 1: Absolute Turnover Floor ────────────────────────
            if candle_turnover < self.min_candle_turnover:
                return None  # Not enough real money in this candle
            
            # ── Filter 2: RVOL on 5m Candle Turnover ────────────────────
            # Compare current candle turnover vs average of previous candles
            past_turnovers = [float(k[6]) for k in klines[:-1] if float(k[6]) > 0]
            if len(past_turnovers) < 3:
                return None
            
            avg_turnover = sum(past_turnovers) / len(past_turnovers)
            if avg_turnover <= 0:
                return None
            
            rvol = candle_turnover / avg_turnover
            
            if rvol < self.min_rvol:
                return None  # Not anomalous enough
            
            # ── Filter 3: Candle Body Structure ──────────────────────────
            open_p = float(current[1])
            high_p = float(current[2])
            low_p = float(current[3])
            close_p = float(current[4])
            
            if open_p <= 0:
                return None
            
            price_delta_pct = ((close_p - open_p) / open_p) * 100.0
            candle_range = high_p - low_p
            body = abs(close_p - open_p)
            
            # Reject full-wick rejection candles (body < 30% of range)
            # These are market makers pulling liquidity, not institutions entering.
            if candle_range > 0 and (body / candle_range) < 0.30:
                logger.debug(
                    f"🕯️ [MacroRadar] {symbol} rejected: "
                    f"Wick candle (body {body/candle_range:.0%} of range)"
                )
                return None
            
            # ── Filter 4: Price Move Confirmation ────────────────────────
            if abs(price_delta_pct) < self.min_price_move_pct:
                return None  # Move too small to be actionable
            
            # ══ SIGNAL CONFIRMED ═════════════════════════════════════════
            direction_val = 1 if price_delta_pct > 0 else -1
            direction_name = "LONG_IMPULSE" if direction_val == 1 else "SHORT_IMPULSE"
            sig_id = _generate_signal_id(symbol, direction_name)
            
            # [GEKTOR v11.7] Populate Vectors for IPC Math
            # hist_p: closes, hist_c: cvd (approximated from volume/side), hist_v: rvols
            hist_p = [float(k[4]) for k in klines]
            hist_v = [float(k[6]) for k in klines]
            
            # Approximated CVD history for the scan
            hist_c = []
            curr_c = 0.0
            for k in klines:
                k_open, k_close = float(k[1]), float(k[4])
                k_side = 1 if k_close >= k_open else -1
                curr_c += (float(k[6]) * k_side)
                hist_c.append(curr_c)

            signal = MacroSignal(
                symbol=symbol,
                direction=direction_val,
                rvol=round(rvol, 2),
                z_score=self.robust_math.get_z_score_robust(price_delta_pct, np.array([((float(k[4])-float(k[1]))/float(k[1])*100) for k in klines[:-1]])),
                imbalance=self.min_imbalance, # Placeholder for micro-imbalance
                cvd_alignment=self.cvd.get_alignment(symbol, direction_val),
                price=close_p,
                hist_p=hist_p,
                hist_c=hist_c,
                hist_v=hist_v,
                liquidity_tier=candidate["tier"],
                atr=np.mean([abs(float(k[2])-float(k[3])) for k in klines[-5:]]) # Simple ATR
            )
            
            self.stats["anomalies_found"] += 1
            self._last_alerts[symbol] = time.time()
            
            logger.info(
                f"🎯 [ANOMALY] {symbol} | {direction} | "
                f"RVOL: {rvol:.1f}x | OI Δ: {candidate['oi_delta_pct']:+.1f}% | "
                f"Price: {price_delta_pct:+.1f}% | "
                f"Turnover: ${candle_turnover:,.0f} | Tier: {candidate['tier']}"
            )
            
            return signal
            
        except Exception as e:
            logger.error(f"❌ [MacroRadar] Deep scan failed for {symbol}: {e}")
            return None
    
    # =========================================================================
    # DISPATCH: Send confirmed signals to Telegram
    # =========================================================================
    
    async def _dispatch_signals(self, signals: List[MacroSignal]):
        """[GEKTOR v11.5] Multi-Type Broadcast with Robust Filtering."""
        if not signals: return

        active_signals = []
        for sig in signals:
            # 1. Dynamic Risk Adaptive Trigger 
            # (Math implemented in scanner to filter candidates before dispatch)
            
            # 2. Heavy Multi-Vector Analysis (Exhaustion, Iceberg, Regime)
            # GEKTOR v11.7: Extract raw lists to minimize IPC/Pickling lac
            raw_p = [float(x) for x in sig.hist_p]
            raw_c = [float(x) for x in sig.hist_c]
            raw_v = [float(x) for x in sig.hist_v]
            
            analysis = await self.orchestrator.analyze_multivector(
                sig.symbol, raw_p, raw_c, raw_v, sig.tick_size, 15
            )
            
            sig.signal_type = analysis.get("div")
            sig.regime = analysis.get("regime")
            sig.vr = analysis.get("vr")
            
            # 3. Advisory Range Calculation
            sig.targets = self.advisory.calculate(sig.price, sig.atr, sig.regime, sig.direction)
            
            # 4. Global Resilience
            is_shock = await self.shock_detector.check(sig)
            if is_shock:
                await self._handle_systemic_shock(sig.direction, active_signals + [sig])
                return 

            if await self.global_mute.is_muted(sig.direction): continue

            # 4. Individual Guard
            status = await self.guard.acquire_or_override(sig)
            if status in ("ALLOWED_NEW", "ALLOWED_OVERRIDE"):
                active_signals.append(sig)
                self._send_individual_alert(sig, status)
                # RESET ADAPTIVE SENSITIVITY
                self._last_alert_time = time.time()
                self.min_z_score = self.base_z_score

    def _send_individual_alert(self, sig: MacroSignal, status: str):
        """[GEKTOR v11.9] Advanced Alert Formatter — Institutional Report."""
        emoji = "🚀" if sig.direction == 1 else "☄️"
        prefix = "STRUCTURAL BREAK" if status == "ALLOWED_OVERRIDE" else "MACRO ANOMALY"
        
        # 1. Advisory Projections
        sl = sig.targets.get("sl", 0.0)
        tp = sig.targets.get("tp", 0.0)
        rr = abs(tp - sig.price) / abs(sig.price - sl) if abs(sig.price - sl) > 0 else 0
        
        # 2. Institutional Metadata
        div_str = sig.signal_type.replace("_", " ") if sig.signal_type else "MOMENTUM"
        tier_emoji = {"A": "💎", "B": "🥇", "C": "🥈", "D": "🥉"}.get(sig.liquidity_tier, "⚪")
        
        message = (
            f"🚨 <b>{prefix}</b> {emoji}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>{sig.symbol}</b> | {sig.direction_str}\n"
            f"💰 Price: <code>${sig.price:,.4f}</code>\n"
            f"🧬 Signal: <b>{div_str}</b>\n"
            f"📈 Z-Score: <b>{sig.z_score:+.2f}σ</b> | RVOL: <b>{sig.rvol:.1f}x</b>\n"
            f"⚖️ CVD: <b>{'✅ MATCH' if sig.cvd_alignment else '❌ NO'}</b> | Tier: {tier_emoji} <b>{sig.liquidity_tier}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>ADVISORY PROJECTION</b>\n"
            f"🟢 Take Profit: <code>${tp:,.4f}</code>\n"
            f"🔴 Stop Loss:   <code>${sl:,.4f}</code>\n"
            f"⚖️ Risk/Reward: <b>{rr:.2f}</b> | Regime: <b>{sig.regime}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🔐 <i>Guard: {status} | Noise Floor: Dynamic</i>"
        )
        
        bus.publish(AlertEvent(title=f"{emoji} {prefix}: {sig.symbol}", message=message))
        self.stats["alerts"] += 1

    async def _handle_systemic_shock(self, direction: int, signals: List[MacroSignal]):
        """[GEKTOR v11.3] Universe Impact Meta-Alert and Global Mute."""
        dir_str = "MARKET-WIDE PUMP" if direction == 1 else "MARKET-WIDE DUMP"
        emoji = "☄️" if direction == -1 else "🚀"
        
        # Sort by impact
        top_impact = sorted(signals, key=lambda s: abs(s.z_score), reverse=True)[:5]
        symbols_str = ", ".join([f"<b>{s.symbol}</b>" for s in top_impact])
        
        title = f"{emoji} {dir_str} DETECTED"
        message = (
            f"🔥 <b>{dir_str} (SYSTEMIC SHOCK)</b> 🔥\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📈 Сработка: <b>Universe Impact Crossed</b>\n"
            f"📢 Текущие лидеры: {symbols_str}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🛡️ <b>[DEFENSE ACTIVATED]</b>\n"
            f"Шорт-сигналы заглушены. Откупы (LONG) РАЗРЕШЕНЫ.\n"
            f"<i>Режим: Направленная фильтрация шока.</i>"
        )
        
        bus.publish(AlertEvent(title=title, message=message))
        
        # Apply Directional Mute (e.g. if Market Dumps (-1), we mute -1)
        await self.global_mute.apply_shock(direction)
        logger.warning(f"☄️ [SystemicShock] {dir_str} detected. Set directional guard.")

    async def _handle_systemic_shock(self, signals: List[MacroSignal]):
        """[GEKTOR v11.2] Coalesce multiple signals into a Systemic Shock Meta-Alert."""
        direction = 1 if sum(s.direction for s in signals) > 0 else -1
        dir_str = "MARKET-WIDE PUMP" if direction == 1 else "MARKET-WIDE DUMP"
        emoji = "☄️" if direction == -1 else "🚀"
        
        # Sort by impact
        top_impact = sorted(signals, key=lambda s: abs(s.z_score), reverse=True)[:5]
        symbols_str = ", ".join([f"<b>{s.symbol}</b>" for s in top_impact])
        
        title = f"{emoji} {dir_str} DETECTED"
        message = (
            f"🔥 <b>{dir_str} (SYSTEMIC SHOCK)</b> 🔥\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📈 Сработка: <b>{len(signals)} активов</b> одновременно!\n"
                f"📢 Текущие лидеры: {symbols_str}\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🛡️ <b>[DEFENSE ACTIVATED]</b>\n"
            f"Индивидуальные алерты заглушены на 15 мин.\n"
            f"<i>Режим: Фильтрация системного шума.</i>"
        )
        
        # 1. Send the Meta-Alert
        bus.publish(AlertEvent(title=title, message=message))
        
        # 2. Put 'Global Guard' in Redis (Mutes all individual macro signals for 15m)
        await bus.redis.set("macro:global_mute", "1", ex=900)
        logger.warning(f"☄️ [SystemicShock] {len(signals)} signals coalesced. Global Mute set (15m).")

    def _cache_signal_for_callback(self, sig: MacroSignal):
        """[Stub for v11] Cache remained for telemetry purposes."""
        pass
    
    def get_cached_signal(self, signal_id: str) -> Optional[MacroSignal]:
        """Retrieve cached signal for callback handler."""
        if not hasattr(self, '_signal_cache'):
            return None
        return self._signal_cache.get(signal_id)
    
    def get_stats(self) -> dict:
        """Returns radar metrics for /stats command."""
        return {
            **self.stats,
            "scan_interval_sec": self.scan_interval,
            "oi_store_symbols": self.oi_store.symbols_tracked,
            "oi_store_warmed": self.oi_store.is_warmed_up,
            "scan_count": self._scan_count,
            "active_cooldowns": len(self._last_alerts),
            "cached_signals": len(getattr(self, '_signal_cache', {})),
        }
