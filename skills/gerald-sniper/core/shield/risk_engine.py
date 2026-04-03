# core/shield/risk_engine.py

import asyncio
import time
from collections import defaultdict
from typing import Dict, Set, Tuple, Optional, List
from loguru import logger
from core.events.nerve_center import bus
from core.events.events import SignalEvent, ExecutionEvent, OrderUpdateEvent, EmergencyAlertEvent
from core.runtime.health import health_monitor

class GlobalRiskGuard:
    """
    [Gektor v8.4] Institutional Risk Armor.
    - Strictly Synchronous Check-and-Set (GIL-based atomicity).
    - Optimistic Deduction (Fixes Stream Race Condition).
    - Proportional Partial Fill Release.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(GlobalRiskGuard, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config: dict):
        if self._initialized: return
        
        risk_cfg = config.get("risk", {})
        exec_cfg = config.get("execution", {})
        
        self.max_global_positions = exec_cfg.get("max_positions", 5)
        self.risk_pct_per_trade = risk_cfg.get("risk_per_trade_pct", 0.01)
        self.max_daily_drawdown_pct = risk_cfg.get("max_drawdown_pct", 0.05)
        
        self.max_per_sector = risk_cfg.get("max_per_sector", 2)
        self.max_per_type = risk_cfg.get("max_per_type", 1)
        
        self.symbol_meta = {}
        self._load_symbol_metadata(config)

        # Persistence State
        self._real_free_margin = 0.0
        self._locked_margin = 0.0
        self._margin_locks: Dict[str, dict] = {} # {client_oid: {locked_amount, qty, price, symbol}}
        
        self.active_positions: Set[str] = set()
        self.sector_counts = defaultdict(int)
        self.type_counts = defaultdict(int)
        
        self.daily_starting_equity = 0.0
        self._initialized = True
        
        logger.info(
            f"🛡️ [RiskGuard 8.4] Institutional Armor ACTIVE. "
            f"Max Pos: {self.max_global_positions} | Atomic CaS Engaged."
        )

    @property
    def effective_margin(self) -> float:
        """Pessimistic effective margin calculation."""
        return max(0.0, self._real_free_margin - self._locked_margin)

    def _load_symbol_metadata(self, config: dict):
        # ... (Metadata loading remains same for correlation guarding)
        sectors = config.get("sectors", {}).get("definitions", {})
        for sector, symbols in sectors.items():
            for sym in symbols:
                s = sym.upper()
                if s not in self.symbol_meta: self.symbol_meta[s] = {}
                self.symbol_meta[s]["sector"] = sector.upper()
        
        types = {
            "MEME": ["PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "FLOKIUSDT", "BONKUSDT", "WIFUSDT", "POPCATUSDT"],
            "AI": ["WLDUSDT", "AGIXUSDT", "FETUSDT", "RNDRUSDT", "TAOUSDT"],
            "L1": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "DOTUSDT", "NEARUSDT", "SUIUSDT", "APTUSDT"]
        }
        for a_type, symbols in types.items():
            for sym in symbols:
                if sym not in self.symbol_meta: self.symbol_meta[sym] = {}
                self.symbol_meta[sym]["type"] = a_type

    def can_proceed(self, symbol: str) -> tuple[bool, str]:
        """[O(1) Sync] Non-atomic pre-flight check for Orchestrator early rejection."""
        symbol = symbol.upper()
        total_load = len(self.active_positions) + len(self._margin_locks)
        if total_load >= self.max_global_positions:
            return False, "LOAD_LIMIT"
        
        if self.daily_starting_equity > 0:
            dd = (self.daily_starting_equity - self._real_free_margin) / self.daily_starting_equity
            if dd >= self.max_daily_drawdown_pct:
                return False, "DRAWDOWN_LOCK"
        
        return True, "PROCEED"

    def calculate_lot_fast(self, symbol: str, entry_price: float, sl_dist_abs: float) -> float:
        """
        [O(1) Sync] Instant Lot Calculation.
        Formula: Qty = (Effective_Equity * Risk_%) / SL_Dist
        """
        if sl_dist_abs <= 0 or self.effective_margin <= 0: return 0.0
        
        risk_usd = self.effective_margin * self.risk_pct_per_trade
        qty = risk_usd / sl_dist_abs
        return qty

    def update_equity_from_ws(self, equity: float):
        """Standard wallet update (slow path)."""
        if equity <= 0: return
        # Note: If we just had an optimistic deduction, this might be slightly behind.
        # But it serves as the ultimate source of truth from the exchange.
        self._real_free_margin = equity
        if self.daily_starting_equity == 0:
            self.daily_starting_equity = equity

    def reserve_margin(self, client_oid: str, symbol: str, qty: float, price: float) -> bool:
        """
        [ATOMIC CaS] Synchronous reservation.
        Prevents Double-Spend without asyncio overhead.
        """
        required_margin = qty * price
        
        # 1. Margin Check
        if required_margin > self.effective_margin:
            logger.warning(f"🚫 [RISK REJECT] Insufficient Margin for {symbol}. Req: {required_margin:.2f}, Eff: {self.effective_margin:.2f}")
            health_monitor.record_gate_event('rejected_risk', 'insufficient_margin')
            return False

        # 2. Constraints Check (Load, Correlation)
        total_load = len(self.active_positions) + len(self._margin_locks)
        if total_load >= self.max_global_positions:
            logger.warning(f"🚫 [RISK REJECT] Load Limit: {total_load}/{self.max_global_positions}")
            return False

        meta = self.symbol_meta.get(symbol.upper(), {})
        sector = meta.get("sector", "UNKNOWN")
        a_type = meta.get("type", "ALT")

        if self.sector_counts[sector] >= self.max_per_sector:
            logger.warning(f"🚫 [RISK REJECT] Sector Correlation: {sector}")
            return False

        # 3. Commit Reservation
        self._locked_margin += required_margin
        self._margin_locks[client_oid] = {
            "locked_amount": required_margin,
            "qty": qty,
            "price": price,
            "symbol": symbol.upper(),
            "ts": time.time()
        }
        self.sector_counts[sector] += 1
        self.type_counts[a_type] += 1
        
        logger.info(f"🔒 [RISK LOCK] {required_margin:.2f} reserved for {symbol} ({client_oid}). Free: {self.effective_margin:.2f}")
        return True

    def release_margin_on_reject(self, client_oid: str):
        """Full rollback for cancelled/rejected orders."""
        lock = self._margin_locks.pop(client_oid, None)
        if lock:
            self._locked_margin = max(0.0, self._locked_margin - lock["locked_amount"])
            symbol = lock["symbol"]
            meta = self.symbol_meta.get(symbol, {})
            self.sector_counts[meta.get("sector", "UNKNOWN")] -= 1
            self.type_counts[meta.get("type", "ALT")] -= 1
            logger.debug(f"🔓 [RISK RELEASE] Rollback for {client_oid}. Locked: {self._locked_margin:.2f}")

    def process_fill(self, client_oid: str, filled_qty: float):
        """
        [OPTIMISTIC DEDUCTION] Handles Partial and Full Fills.
        Resolves Stream Race Condition by adjusting real_free_margin locally.
        """
        lock = self._margin_locks.get(client_oid)
        if not lock: return

        spent_margin = filled_qty * lock["price"]
        
        # Optimistic deduction: assume money is already gone from wallet
        self._real_free_margin = max(0.0, self._real_free_margin - spent_margin)
        
        # Reduce lock
        self._locked_margin = max(0.0, self._locked_margin - spent_margin)
        lock["locked_amount"] -= spent_margin
        lock["qty"] -= filled_qty
        
        symbol = lock["symbol"]
        if symbol not in self.active_positions:
            self.active_positions.add(symbol)

        if lock["qty"] <= 0.00001:
            self._margin_locks.pop(client_oid, None)
            logger.success(f"✅ [RISK SYNC] Full Fill {symbol}. Lock cleared.")
        else:
            logger.info(f"⚡ [RISK SYNC] Partial Fill {symbol} ({filled_qty}). Remaining lock: {lock['locked_amount']:.2f}")

    def start(self):
        bus.subscribe(OrderUpdateEvent, self._handle_order_update)
        # Periodic cleanup of zombie locks (> 60s)
        asyncio.create_task(self._cleanup_loop())

    async def _handle_order_update(self, event: OrderUpdateEvent):
        # 1. Update equity if present
        if hasattr(event, 'wallet_balance') and event.wallet_balance:
            self.update_equity_from_ws(event.wallet_balance)

        # 2. Map WS status to Risk Engine
        oid = event.client_oid
        if event.status in ["Filled", "PartiallyFilled"]:
            self.process_fill(oid, event.filled_qty)
        elif event.status in ["Cancelled", "Rejected", "Deactivated"]:
            self.release_margin_on_reject(oid)

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(30)
            now = time.time()
            to_purge = [oid for oid, l in self._margin_locks.items() if now - l["ts"] > 60]
            for oid in to_purge:
                logger.warning(f"🛡️ [RiskGuard] Purging zombie lock: {oid}")
                self.release_margin_on_reject(oid)

# Global Risk Armor
risk_guard = None
def init_risk_guard(config: dict) -> GlobalRiskGuard:
    global risk_guard
    risk_guard = GlobalRiskGuard(config)
    return risk_guard

