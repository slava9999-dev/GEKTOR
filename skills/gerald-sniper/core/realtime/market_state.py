import time
import asyncio
from typing import Dict, List, Optional
from loguru import logger
from .models import SymbolLiveState, ZeroGCOrderbook
from data.bybit_rest import BybitREST
from data.bybit_ws import BybitWSManager

class LiveMarketState:
    """
    Central Nervous System for Real-time Data (v5.3 Refactored).
    - High-concurrency trade & orderbook ingestion.
    - Zero-allocation liquidation tracking.
    - Audit 25.5: State Recovery on short disconnects (< 15s).
    """
    MAX_SYMBOLS = 200       # Hard cap on tracked symbols
    STALE_TIMEOUT = 600     # 10 minutes without update = stale
    CLEANUP_INTERVAL = 60   # Run cleanup every 60s
    MAX_ALLOWED_GAP = 15    # Audit 25.5: State Recovery threshold (seconds)
    GAP_INVALIDATION_THRESHOLD = 90 # [NERVE REPAIR v4.1] Warm-up Invalidation (seconds)

    def __init__(self):
        self.symbols: Dict[str, Optional[SymbolLiveState]] = {} # Key: "exchange:symbol"
        self._last_cleanup = time.time()
        self._last_update_ts: Dict[str, float] = {}
        self._stats = {"created": 0, "cleaned": 0, "updates": 0}
        self.server_time_offset = 0 # ms (Server - Local)
        self.rest: Optional[BybitREST] = None
        self.ws: Optional[BybitWSManager] = None

    def set_clients(self, rest: BybitREST, ws: BybitWSManager):
        self.rest = rest
        self.ws = ws

    def _get_key(self, exchange: str, symbol: str) -> str:
        return f"{exchange.lower()}:{symbol.upper()}"

    def _validate_state_gap(self, key: str, now: float):
        """[NERVE REPAIR v4.1] Detects data gaps and invalidates warm-up status."""
        last_update = self._last_update_ts.get(key, 0)
        if last_update > 0:
            gap = now - last_update
            if gap > self.GAP_INVALIDATION_THRESHOLD:
                state = self.symbols.get(key)
                if state:
                    state.mark_cold(f"Data Gap: {gap:.1f}s")

    def update_trade(self, symbol: str, price: float, qty: float, side: str, ts_ms: int, exchange: str = "bybit"):
        key = self._get_key(exchange, symbol)
        now = time.time()
        
        self._validate_state_gap(key, now)

        if key not in self.symbols or self.symbols[key] is None:
            self.symbols[key] = SymbolLiveState(symbol=symbol)
            self._stats["created"] += 1
            logger.debug(f"🆕 [MarketState] Initialized state for {key}")

        state = self.symbols[key]
        if state.first_tick_ts is None:
            state.first_tick_ts = now
            
        # [Audit v6.1] BBO Cross-Validation (Zero Phase Lag)
        if not state.is_sane_price(price, state.bba_bid, state.bba_ask):
             return # Squelch anomalous spikes

        state.update(price, qty, side)
        state.last_trade_ts = now
        self._last_update_ts[key] = now
        self._stats["updates"] += 1

    def update_orderbook(self, symbol: str, bids_usd: float, asks_usd: float, 
                         bids: list = None, asks: list = None, update_id: int = 0, 
                         ts_ms: int = 0, is_snapshot: bool = False, exchange: str = "bybit"):
        key = self._get_key(exchange, symbol)
        now = time.time()
        
        self._validate_state_gap(key, now)

        if key not in self.symbols or self.symbols[key] is None:
            self.symbols[key] = SymbolLiveState(symbol=symbol)
            self._stats["created"] += 1
        
        state = self.symbols[key]
        if state.first_tick_ts is None:
            state.first_tick_ts = now
            
        state.bid_vol_usd = bids_usd
        state.ask_vol_usd = asks_usd
        state.update_id = update_id
        state.exchange_ts = ts_ms
        
        if is_snapshot:
            state.orderbook.apply_snapshot({"b": bids, "a": asks, "u": update_id})
        else:
            state.orderbook.apply_delta({"b": bids, "a": asks, "u": update_id})
            
        self._last_update_ts[key] = state.last_l2_ts = now
        self._stats["updates"] += 1

    def update_bba(self, symbol: str, bid: float, ask: float, ts_ms: int, update_id: int = 0, exchange: str = "bybit"):
        key = self._get_key(exchange, symbol)
        now = time.time()
        
        self._validate_state_gap(key, now)

        if key not in self.symbols or self.symbols[key] is None:
            self.symbols[key] = SymbolLiveState(symbol=symbol)
            
        state = self.symbols[key]
        if state.first_tick_ts is None:
            state.first_tick_ts = now
            
        # [Audit v6.1] L1 Integrity Check: Crossed Book Protection
        if bid >= ask and bid > 0:
             return # Squelch phantom L1 spikes with zero spread

        state.bba_bid = bid
        state.bba_ask = ask
        state.update_spread(bid, ask) # [Audit 6.7] Dynamic Slippage Input
        state.exchange_ts = ts_ms
        state.update_id = update_id
        self._last_update_ts[key] = state.last_l2_ts = time.time()

    def update_l2_metrics(self, symbol: str, imbalance: float, bids_usd: float, asks_usd: float, bid: float, ask: float):
        """Update market state with pre-computed L2 metrics from ShardWorker."""
        key = self._get_key("bybit", symbol)
        now = time.time()
        
        self._validate_state_gap(key, now)

        if key not in self.symbols or self.symbols[key] is None:
            self.symbols[key] = SymbolLiveState(symbol=symbol)
        
        state = self.symbols[key]
        if state.first_tick_ts is None:
            state.first_tick_ts = now
            
        state.micro_imbalance = imbalance  # Assuming this field exists in SymbolLiveState
        state.bid_vol_usd = bids_usd
        state.ask_vol_usd = asks_usd
        state.last_l2_ts = now
        state.bba_bid = bid
        state.bba_ask = ask
        self._last_update_ts[key] = time.time()

    async def trigger_resync(self, symbol: str, exchange: str = "bybit"):
        """Task 17.1: Active orderbook resync via REST."""
        if not self.rest: return
        key = self._get_key(exchange, symbol)
        if key not in self.symbols: return
        
        state = self.symbols[key]
        await state.orderbook.trigger_active_resync(self.rest)

    def update_liquidation(self, symbol: str, price: float, qty: float, side: str):
        """Zero-Allocation Liquidation Buffer Update."""
        key = self._get_key("bybit", symbol)
        now = time.time()
        
        self._validate_state_gap(key, now)

        if key not in self.symbols or self.symbols[key] is None:
            if len(self.symbols) >= self.MAX_SYMBOLS: return
            self.symbols[key] = SymbolLiveState(symbol=symbol)
        
        state = self.symbols[key]
        if state.first_tick_ts is None:
            state.first_tick_ts = now
            
        idx = state.liq_ptr % 500
        side_val = 1.0 if side.upper() == "BUY" else 0.0
        state.liq_history[idx] = [time.time(), price, qty, side_val]
        state.liq_ptr += 1
        self._last_update_ts[key] = time.time()

    def get_price_history(self, symbol: str, start_ts_ms: int, exchange: str = "bybit") -> List[Dict]:
        """
        [GEKTOR v10.2] Returns price history for auditing purposes.
        Scans SymbolLiveState buckets. Resolution: 1s.
        """
        state = self.get_state(symbol, exchange)
        if not state:
            return []
            
        start_ts_sec = int(start_ts_ms / 1000)
        history = []
        
        # Iterate through history_ts and collect bucket data within the window
        for ts in state.history_ts:
            if ts >= start_ts_sec:
                bucket = state.buckets.get(ts)
                if bucket:
                    # 'ts' here is unix seconds, convert to ms for consistency with Auditor expectation
                    history.append({
                        "ts": ts * 1000,
                        "price": bucket['price']
                    })
        
        return history

    def get_state(self, symbol: str, exchange: str = "bybit") -> Optional[SymbolLiveState]:
        key = self._get_key(exchange, symbol)
        return self.symbols.get(key)

    def cleanup(self, force: bool = False):
        """
        Prune stale symbols from memory (Audit 25.5 Guard).
        """
        now = time.time()
        self._last_cleanup = now

        stale_symbols = []
        for sym, last_ts in self._last_update_ts.items():
            if now - last_ts > self.STALE_TIMEOUT:
                stale_symbols.append(sym)

        for sym in stale_symbols:
            # Audit 25.5: NEVER prune if we recently updated (even if cleanup is triggered manually)
            if now - self._last_update_ts.get(sym, 0) < self.MAX_ALLOWED_GAP:
                continue
            self.symbols.pop(sym, None)
            self._last_update_ts.pop(sym, None)

        if stale_symbols:
            self._stats["cleaned"] += len(stale_symbols)
            logger.debug(f"🧹 MarketState cleanup: removed {len(stale_symbols)} stale symbols.")

        # Force cleanup: remove oldest 25% if still over limit
        if force and len(self.symbols) >= self.MAX_SYMBOLS:
            sorted_syms = sorted(self._last_update_ts.items(), key=lambda x: x[1])
            to_remove = len(self.symbols) // 4
            for sym, _ in sorted_syms[:to_remove]:
                self.symbols.pop(sym, None)
                self._last_update_ts.pop(sym, None)
            self._stats["cleaned"] += to_remove

    def get_latest_snapshot(self) -> Dict[str, SymbolLiveState]:
        """Returns a stable snapshot of all active symbols."""
        return {k: v for k, v in self.symbols.items() if v is not None}

    def is_toxic(self, symbol: str, direction: str) -> bool:
        """
        [NERVE REPAIR v4.2] L2 Bailout Sensor.
        Detects if orderbook imbalance is critically opposed to our position.
        """
        state = self.get_state(symbol)
        if not state:
            return False
            
        imb = float(state.micro_imbalance)
        if direction.upper() == "LONG":
            return imb <= -0.8 # Massive sell-side wall/pressure
        elif direction.upper() == "SHORT":
            return imb >= 0.8 # Massive buy-side wall/pressure
        return False

    def get_stats(self) -> dict:
        return {
            "tracked_symbols": len(self.symbols),
            "total_created": self._stats["created"],
            "total_cleaned": self._stats["cleaned"],
            "total_updates": self._stats["updates"],
        }

    def check_global_staleness(self, threshold: float = 5.0) -> bool:
        """
        [NERVE REPAIR v4.3] API Integrity Guard.
        Differentiates "Dead Market" from "Broken API/WebSocket".
        Returns True if high-liquidity symbols (BTC/ETH/SOL) haven't updated within threshold.
        """
        now = time.time()
        liquid_keys = [
            self._get_key("bybit", "BTCUSDT"),
            self._get_key("bybit", "ETHUSDT"),
            self._get_key("bybit", "SOLUSDT")
        ]
        
        updates_found = 0
        for key in liquid_keys:
            last = self._last_update_ts.get(key, 0)
            if last > 0 and (now - last) < threshold:
                updates_found += 1
                
        # If we have tracked data but NO updates found for ALL liquid pairs, 
        # it's a structural failure (WebSocket freeze).
        if len(self.symbols) > 5 and updates_found == 0:
            self.broadcast_blindness("GLOBAL_WS_FREEZE", "No updates on liquid pairs (BTC/ETH/SOL)")
            return True # GLOBAL STALENESS DETECTED
            
        return False

    def broadcast_blindness(self, reason: str, details: str):
        """[GEKTOR v10.0] Activates Red Alert across all components."""
        logger.critical(f"🚨 [RED ALERT] System Blindness Initiated: {reason} ({details})")
        
        for key, state in self.symbols.items():
            if state and not state.is_blind:
                state.is_blind = True
                state.blindness_reason = reason
                
        # Broadcast via bus for External Observers (Telegram, etc.)
        from core.events.events import SystemBlindnessEvent
        from core.events.nerve_center import bus
        asyncio.create_task(bus.publish(SystemBlindnessEvent(
            source="MarketState", 
            symbol="GLOBAL", 
            reason=reason, 
            stale_ms=0
        )))

    def invalidate_symbol(self, symbol: str, reason: str):
        """Atomic invalidation of a specific symbol on connection loss."""
        key = self._get_key("bybit", symbol)
        state = self.symbols.get(key)
        if state:
            state.is_blind = True
            state.blindness_reason = reason
            logger.warning(f"🧊 [MarketState] Symbol {symbol} invalidated: {reason}")

# Global Singleton
market_state = LiveMarketState()
