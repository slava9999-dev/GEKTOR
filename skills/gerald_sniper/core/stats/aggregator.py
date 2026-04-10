import numpy as np
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple
from loguru import logger
from numba import njit

@njit(fastmath=True)
def robust_slope_theil_sen(y: np.ndarray) -> float:
    """
    [GEKTOR v14.5.2] Outlier-Resistant Trend Estimator.
    Calculates the median of all pairwise slopes.
    Resistant to ~29% outliers (Flash Crashes).
    """
    n = len(y)
    if n < 2: return 0.0
    
    num_slopes = n * (n - 1) // 2
    slopes = np.empty(num_slopes, dtype=np.float64)
    k = 0
    for i in range(n):
        for j in range(i + 1, n):
            # Slope = (y_j - y_i) / (t_j - t_i) where t is index difference
            slopes[k] = (y[j] - y[i]) / (j - i)
            k += 1
    return np.median(slopes)

class MarketHealthSHMBridge:
    """
    [GEKTOR v14.5.5] Multi-Dimensional Health & Macro Bridge.
    Fields (48 bytes):
    [0-7] Seq: int (Counter)
    [8-15] Skew: double (Spot OFI)
    [16-23] Momentum: double (Spot Price Change)
    [24-31] PerpSkew: double (Perp OFI)
    [32-39] OISlope: double (Open Interest Trend)
    [40-47] MacroMuteUntil: double (Unix TS)
    """
    __slots__ = ('_shm', '_buf', '_is_writer')

    def __init__(self, is_writer: bool = False):
        self._is_writer = is_writer
        try:
            self._shm = shared_memory.SharedMemory(name='gektor_health_v5', create=is_writer, size=48)
            self._buf = self._shm.buf
        except FileNotFoundError:
            # Fallback for process initialization
            self._shm = None

    def read(self) -> tuple:
        if not self._shm: return (0, 0.0, 0.0, 0.0, 0.0, 0.0)
        # Structured read: Seq, Skew, Momentum, PerpSkew, OISlope, MacroMute
        return struct.unpack('qddddd', self._buf[:48])

    def write(self, skew: float, mom: float, perp_skew: float, oi_slope: float, mute_until: float = 0.0):
        if not self._is_writer: return
        # Atomic Update (Seq++ for torn-read detection)
        curr_seq = struct.unpack('q', self._buf[:8])[0]
        struct.pack_into('qddddd', self._buf, 0, curr_seq + 1, skew, mom, perp_skew, oi_slope, mute_until)

class ZeroGCRingBuffer:
    """
    Pre-allocated computational memory for sliding windows.
    Zero object allocation after __init__.
    """
    __slots__ = ('_capacity', '_closes', '_cvds', '_idx', '_count')

    def __init__(self, capacity: int = 50):
        self._capacity = capacity
        self._closes = np.zeros(capacity, dtype=np.float64)
        self._cvds = np.zeros(capacity, dtype=np.float64)
        self._idx = 0
        self._count = 0

    def append(self, close: float, cvd: float):
        self._closes[self._idx] = close
        self._cvds[self._idx] = cvd
        self._idx = (self._idx + 1) % self._capacity
        if self._count < self._capacity:
            self._count += 1

    def hard_reset(self) -> None:
        """[GEKTOR v14.7.3] Logical reset without GC overhead."""
        self._idx = 0
        self._count = 0
        self._closes.fill(np.nan)
        self._cvds.fill(0.0)

    def get_ordered_data(self) -> Tuple[np.ndarray, np.ndarray]:
        """Returns views of the data in chronological order."""
        if self._count < self._capacity:
            return self._closes[:self._count], self._cvds[:self._count]
        # Real-time rotation using np.roll (optimized C-loop inside numpy)
        return np.roll(self._closes, -self._idx), np.roll(self._cvds, -self._idx)

@dataclass(slots=True)
class DollarBar:
    """[GEKTOR v14.5.2] Seasonality-Aware Information Bar."""
    open_p: float
    high_p: float
    low_p: float
    close_p: float
    volume_usd: float
    cvd: float
    start_ts: float
    end_ts: float

@njit(fastmath=True)
def update_ewma(current_ewma: float, new_value: float, alpha: float) -> float:
    """Zero-datetime EWMA for real-time threshold scaling."""
    return (new_value * alpha) + (current_ewma * (1.0 - alpha))

class TrueDynamicDollarBarEngine:
    """
    [GEKTOR v14.5.3] Self-Regulating Dollar Bar Engine.
    Threshold scales with real-time volume flow (EWMA), not clocks.
    """
    __slots__ = ('_base_threshold', '_current_threshold', '_ewma_volume', 
                 '_alpha', '_rolling_bars', '_current_bar', '_current_cvd', 
                 '_current_usd_vol')

    def __init__(self, initial_threshold: float = 1_000_000.0, capacity: int = 50):
        self._base_threshold = initial_threshold
        self._current_threshold = initial_threshold
        self._ewma_volume = initial_threshold
        # EMA over ~100 bars for mid-term smoothing
        self._alpha = 2.0 / (100.0 + 1.0) 
        
        self._rolling_bars = ZeroGCRingBuffer(capacity=capacity)
        self._current_bar: Optional[DollarBar] = None
        self._current_cvd = 0.0
        self._current_usd_vol = 0.0

    def ingest_trade(self, price: float, qty: float, is_buy: bool) -> bool:
        usd_value = price * qty
        self._current_usd_vol += usd_value
        self._current_cvd += (qty if is_buy else -qty)

        if self._current_usd_vol >= self._current_threshold:
            # Fix bar in Zero-GC memory
            self._rolling_bars.append(price, self._current_cvd)
            
            # Dynamic EWMA Adjustment: follow actual liquidity flow
            self._ewma_volume = update_ewma(self._ewma_volume, self._current_usd_vol, self._alpha)
            
            # Bound threshold to base to avoid 'dust-bars'
            self._current_threshold = max(self._base_threshold, self._ewma_volume)
            
            self._current_usd_vol = 0.0
            return True # Bar complete
        return False

    def hard_reset(self) -> None:
        """[GEKTOR v14.7.3] Full state flush for Amnesia Protocol."""
        self._rolling_bars.hard_reset()
        self._current_threshold = self._base_threshold
        self._ewma_volume = self._base_threshold
        self._current_usd_vol = 0.0
        self._current_cvd = 0.0

    def get_robust_slopes(self, market_momentum: float) -> Tuple[float, float, float]:
        """Calculates Robust Trends for Entry/Exit logic."""
        closes, cvds = self._rolling_bars.get_ordered_data()
        if len(closes) < 50: return 0.0, 0.0, 0.0
        
        # JIT-optimized median slopes
        p_slope = robust_slope_theil_sen(closes)
        c_slope = robust_slope_theil_sen(cvds)
        # Residual Strength (Idiosyncratic Alpha)
        idiosyncratic = c_slope - (abs(market_momentum) * 0.5)
        
        return p_slope / (closes[0] + 1e-9), c_slope, idiosyncratic

@dataclass(slots=True)
class VirtualPosition:
    """[GEKTOR v14.5.4] Per-Symbol Tracking State."""
    symbol: str
    entry_price: float
    whale_cost_basis_vwap: float
    status: str = 'PENDING' # 'PENDING', 'ACTIVE', 'INVALIDATED'
    timestamp: float = 0.0

class CrossMarketValidator:
    """
    [GEKTOR v14.5.5] Arbitrage Disqualification Engine.
    Detects Delta-Neutral 'Basis Trades' (Cash & Carry) vs Directed Alpha.
    """
    __slots__ = ('_min_divergence',)

    def __init__(self):
        self._min_divergence = 0.4

    def validate_directed_alpha(self, spot_slope: float, perp_slope: float, oi_slope: float) -> bool:
        """
        Spot Up + Perp Down + OI Up = Cash & Carry Arbitrage (SKIP).
        Exogenous macro-noise causes neutral validation.
        """
        # [THE ARB TRAP]: Whale buys Spot, sells Perp, hedges OI
        if spot_slope > 0.4 and perp_slope < -0.3 and oi_slope > 0.15:
             logger.warning("🚫 [CROSS-MARKET] Delta-Neutral Arbitrage detected. Discarding fake Spot accumulation.")
             return False
             
        # [THE DIRECTIONAL WHALE]: Buyers on Spot, No aggressive Perp dumping, OI Stable/Rising
        if spot_slope > 0.6 and perp_slope > -0.2:
             return True
             
        return False

class AdvisoryLifecycleTracker:
    """
    [GEKTOR v14.5.4] Heavy-Duty Idea Monitoring.
    Resistant to context amnesia (Redis) and whipsaws (L2 Absorption).
    """
    __slots__ = ('_active_positions', '_feedback_queue', '_redis_client')

    def __init__(self, feedback_queue: Optional[Queue] = None, redis_client=None):
        self._active_positions: dict[str, VirtualPosition] = {}
        self._feedback_queue = feedback_queue
        self._redis_client = redis_client

    def invalidate_all(self, reason: str):
        self._active_positions.clear()
        logger.info(f"🧹 [LIFECYCLE] All positions cleared: {reason}")

    def restore_state(self):
        """State Recovery: Re-loads active positions from Redis after process crash."""
        if not self._redis_client: return
        try:
             import json
             raw_state = self._redis_client.hgetall("tekton:positions")
             for sym, data in raw_state.items():
                 p = json.loads(data)
                 if p['status'] == 'ACTIVE':
                      symbol = sym.decode() if isinstance(sym, bytes) else sym
                      self._active_positions[symbol] = VirtualPosition(**p)
             logger.info(f"🔄 [LIFECYCLE] State Recovered: {list(self._active_positions.keys())}")
        except Exception as e:
             logger.error(f"❌ [LIFECYCLE] State recovery failed: {e}")

    def process_feedback(self):
        """Asynchronous, Lock-Free ingestion of operator actions (Telegram)."""
        if not self._feedback_queue: return
        while True:
            try:
                msg = self._feedback_queue.get_nowait()
                symbol = msg.get('symbol')
                action = msg.get('action') # 'CONFIRM' or 'SKIP'
                
                if action == 'CONFIRM' and symbol in self._active_positions:
                    pos = self._active_positions[symbol]
                    pos.status = 'ACTIVE'
                    self._persist_to_redis(pos)
                    logger.critical(f"✅ [LIFECYCLE] User confirmed Entry on {symbol}. Activating exit sentinel.")
                elif action == 'SKIP':
                    self._active_positions.pop(symbol, None)
                    if self._redis_client: self._redis_client.hdel("tekton:positions", symbol)
            except Empty:
                break

    def _persist_to_redis(self, pos: VirtualPosition):
        if self._redis_client:
             import json
             data = {"symbol": pos.symbol, "entry_price": pos.entry_price, 
                     "whale_cost_basis_vwap": pos.whale_cost_basis_vwap, 
                     "status": pos.status, "timestamp": pos.timestamp}
             self._redis_client.hset("tekton:positions", pos.symbol, json.dumps(data))

    def track_alert(self, symbol: str, price: float, whale_vwap: float):
        """New potential entry. Stays PENDING until user clicks 'CONFIRM'."""
        self._active_positions[symbol] = VirtualPosition(
            symbol=symbol, entry_price=price, 
            whale_cost_basis_vwap=whale_vwap, 
            status='PENDING', timestamp=time.time()
        )

    def monitor_active_ideas(self, symbol: str, price: float, ofi: float) -> Optional[dict]:
        """
        Microstructural Exit Sentinel.
        Includes [SHAKEOUT FILTER]: ignores breakdowns if Bid-Side is absorbing.
        """
        pos = self._active_positions.get(symbol)
        if not pos or pos.status != 'ACTIVE': return None

        # 1. Price is breaking the Whale's base
        is_breaking = price < pos.whale_cost_basis_vwap * 0.99
        
        if is_breaking:
             # [SHAKEOUT FILTER]: Check L2 Depth Absorption (OFI Expansion)
             # If OFI is strongly positive (> 0.6), it means Bids are hardening despite the drop.
             # This is a Liquidity Sweep (Whale buying the retail panic).
             if ofi > 0.6:
                  logger.info(f"⏳ [SHAKEOUT] {symbol} under VWAP, but Bid-Absorption detected (OFI: {ofi:.2f}). Holding alert.")
                  return None
             
             # True Breakdown: Price is down, and Order Flow is either weak (OFI < 0) or neutral.
             return {"type": "EXIT_INVALIDATION", "reason": "Structural Breakdown (No Absorption)", "symbol": symbol}
             
        return None

class MacroAwareRadarState:
    """
    [GEKTOR v14.6.0] Resilience Subsystem.
    Manages structural breaks and warmup phases after macro-shocks (NFP/CPI).
    """
    __slots__ = ('_mute_ts', '_is_warming_up', '_bar_engine', '_tracker')

    def __init__(self, bar_engine, tracker):
        self._mute_ts = 0.0
        self._is_warming_up = False
        self._bar_engine = bar_engine # TrueDynamicDollarBarEngine
        self._tracker = tracker # AdvisoryLifecycleTracker

    def check_and_sync(self, current_ts: float) -> bool:
        """Main check in hot loop. Return False if silent."""
        if current_ts < self._mute_ts:
             if not self._is_warming_up:
                  # Enter Tornado Mode: Destroy the past
                  self._execute_amnesia()
             return False

        if self._is_warming_up:
             # Progressively wait for bars to fill (buffer_full = count == capacity)
             if self._bar_engine._rolling_bars._count >= 50:
                  self._is_warming_up = False
                  logger.info("🔥 [WARMUP] Protocol Complete. Radar ARMED.")
                  return True
             return False # Still silent

        return True

    def sync_mute_ts(self, ts: float):
        if ts > self._mute_ts:
             self._mute_ts = ts
             logger.warning(f"🌪 [STORM_ALERT] Macro event incoming. Mute until {ts}")

    def _execute_amnesia(self):
        logger.critical("🚨 [AMNESIA] PROTOCOL EXECUTED. Wiping structural history.")
        self._bar_engine.reset_to_zero()
        self._tracker.invalidate_all("MACRO_STRUCTURAL_BREAK")
        self._is_warming_up = True
