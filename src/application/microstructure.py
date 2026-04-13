from loguru import logger
from dataclasses import dataclass
from typing import Optional, Dict
from src.infrastructure.config import settings

@dataclass(slots=True, frozen=True)
class L2Level:
    price: float
    volume: float

@dataclass(slots=True, frozen=True)
class L2Snapshot:
    symbol: str
    best_bid: L2Level
    best_ask: L2Level
    exchange_ts: int

class MicrostructureDefender:
    """
    [GEKTOR v2.0] Integrity Verification Module (Phantom Liquidity Filter).
    Used strictly for Phase 5 (Premise Invalidation) and Phase 1 (Spike Filtering).
    NOT for scalping or market-making. 
    Verifies that 'Informed Accumulation' is backed by realized L1 volume.
    """
    def __init__(self, symbol: str, confirmed_threshold: Optional[float] = None, release_factor: Optional[float] = None):
        self.symbol = symbol
        self._prev_snapshot: Optional[L2Snapshot] = None
        self._accumulated_ofi: float = 0.0
        self._current_state = "NEUTRAL"
        
        # [INTRADAY v4.1] Адаптивные пороги в зависимости от ликвидности актива
        is_major = symbol in ["BTCUSDT", "ETHUSDT"]
        config = settings.MICRO_OFI_CONFIG["MAJORS"] if is_major else settings.MICRO_OFI_CONFIG["ALTS"]
        
        self._confirmed_threshold = confirmed_threshold or config["threshold"]
        factor = release_factor or config["factor"]
        self._release_threshold = self._confirmed_threshold * factor
        
        # Tracks realized trade volume between snapshots
        self._execution_buffer: float = 0.0 
        
        # [HYSTERESIS STATE MACHINE]
        self._current_state: str = "NEUTRAL" # NEUTRAL, ACCUMULATION, DISTRIBUTION

    def update_execution(self, volume: float, side: str):
        """Updates the trade volume delta since the last L2 snapshot."""
        # Simple net volume: Buy (+) / Sell (-)
        if side.upper() == 'BUY':
            self._execution_buffer += volume
        else:
            self._execution_buffer -= volume

    def calculate_informed_ofi(self, current: L2Snapshot) -> float:
        """
        Calculates OFI with Trade-Delta Convergence.
        Prevents being trapped by HFT spoofers (Phantom Liquidity).
        """
        if not self._prev_snapshot:
            self._prev_snapshot = current
            return 0.0

        p_bid, p_ask = self._prev_snapshot.best_bid, self._prev_snapshot.best_ask
        c_bid, c_ask = current.best_bid, current.best_ask

        # 1. Raw OFI Calculation (Cont et al, 2014)
        ofi_bid = 0.0
        if c_bid.price >= p_bid.price: ofi_bid += c_bid.volume
        if c_bid.price <= p_bid.price: ofi_bid -= p_bid.volume

        ofi_ask = 0.0
        if c_ask.price <= p_ask.price: ofi_ask += c_ask.volume
        if c_ask.price >= p_ask.price: ofi_ask -= p_ask.volume

        raw_ofi = ofi_bid - ofi_ask
        
        # 2. [PHANTOM FILTER] Check convergence with realized Trade Delta
        confirmed_ofi = raw_ofi
        if raw_ofi > 0: # Bullish Imbalance
            confirmed_ofi = min(raw_ofi, max(self._execution_buffer, 0) * 1.5)
        elif raw_ofi < 0: # Bearish Imbalance
            confirmed_ofi = max(raw_ofi, min(self._execution_buffer, 0) * 1.5)

        # 3. Update state
        self._prev_snapshot = current
        self._execution_buffer = 0.0 # Reset for next window
        
        # EWMA for noise reduction
        self._accumulated_ofi = (confirmed_ofi * 0.2) + (self._accumulated_ofi * 0.8)
        return self._accumulated_ofi

    async def ingest_snapshot(self, snapshot: L2Snapshot) -> dict:
        """Entry point for the Event Bus with Hysteresis Shield."""
        ofi = self.calculate_informed_ofi(snapshot)
        is_new_impulse = False
        prev_state = self._current_state
        
        # Logic for state transitions (Schmitt Trigger / Hysteresis)
        if self._current_state == "NEUTRAL":
            if ofi > self._confirmed_threshold:
                self._current_state = "ACCUMULATION"
                is_new_impulse = True
                logger.warning(f"🚨 [MICROSTRUCTURE] ACCUMULATION IMPULSE STARTED | {snapshot.symbol} | OFI: {ofi:.2f}")
            elif ofi < -self._confirmed_threshold:
                self._current_state = "DISTRIBUTION"
                is_new_impulse = True
                logger.warning(f"🚨 [MICROSTRUCTURE] DISTRIBUTION IMPULSE STARTED | {snapshot.symbol} | OFI: {ofi:.2f}")

        elif self._current_state == "ACCUMULATION":
            # State release threshold: ofi must drop below release_threshold to reset
            if ofi < self._release_threshold:
                self._current_state = "NEUTRAL"
        
        elif self._current_state == "DISTRIBUTION":
            # State release threshold: ofi must rise above -release_threshold to reset
            if ofi > -self._release_threshold:
                self._current_state = "NEUTRAL"

        # Double check: is_new_impulse should only be true if state changed from NEUTRAL
        if is_new_impulse and prev_state != "NEUTRAL":
            is_new_impulse = False # Safety latch

        return {
            "symbol": snapshot.symbol,
            "ofi": ofi,
            "state": self._current_state,
            "is_new_impulse": is_new_impulse
        }
