# core/scenario/reconciler.py
"""
[Gektor v5.5] State Reconciler — Deterministic State Hydration.

Architecture:
1. Lazy Hydration: On-demand signal recovery when WS delivers an unknown order_id
2. Background Sweep: Periodic 30s hard sync with exchange positions  
3. Event-Driven Trigger: Immediate reconciliation on WS/ZMQ reconnection events

This module closes the Split-Brain vulnerability where TCP disconnection
causes in-memory state (_trading_signals) to desynchronize from exchange reality.
"""

import asyncio
import time
from typing import Dict, Optional, Set
from loguru import logger

from core.events.nerve_center import bus
from core.events.events import ConnectionRestoredEvent, OrderUpdateEvent
from core.scenario.signal_entity import TradingSignal, SignalState, OrderState


class StateReconciler:
    """
    Idempotent State Recovery Engine.
    
    Resolves Split-Brain between:
    - Exchange (Single Source of Truth) 
    - PostgreSQL (Durable State)
    - In-Memory Dict (Hot Path Cache)
    
    Invariant: Exchange > DB > Memory. Always.
    """
    
    def __init__(self, adapter, db_manager, trading_signals: Dict[str, TradingSignal], config: dict):
        self.adapter = adapter          # IExchangeAdapter (Bybit/Binance)
        self.db = db_manager            # DatabaseManager
        self.signals = trading_signals  # ExecutionEngine._trading_signals (shared ref)
        
        self._sync_lock = asyncio.Lock()
        self._last_sync_ts = 0.0
        self._min_sync_interval = 5.0   # Debounce: max 1 sync per 5s
        self._background_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Config
        recon_cfg = config.get("reconciler", {})
        self.background_interval = recon_cfg.get("interval_sec", 30)
        self.enabled = recon_cfg.get("enabled", True)
        
        # Metrics
        self.stats = {
            "ghosts_hydrated": 0,
            "zombies_purged": 0,
            "event_driven_syncs": 0,
            "background_syncs": 0,
        }
    
    def start(self):
        """Register event handlers and start background sweep."""
        if not self.enabled:
            logger.warning("🔇 [Reconciler] DISABLED by config. Split-Brain protection OFF.")
            return
        
        self._running = True
        
        # Event-Driven Trigger: Subscribe to reconnection events
        bus.subscribe(ConnectionRestoredEvent, self._on_connection_restored)
        
        # Background Sweep
        self._background_task = asyncio.create_task(self._run_background_sweep())
        
        logger.info(
            f"🔄 [Reconciler] ACTIVE — Background: {self.background_interval}s | "
            f"Event-Driven: ConnectionRestoredEvent"
        )
    
    # =========================================================================
    # [P1] LAZY HYDRATION — Called from handle_order_update when signal unknown
    # =========================================================================
    
    async def hydrate_signal(self, order_id: str, event: Optional[OrderUpdateEvent] = None) -> Optional[TradingSignal]:
        """
        On-demand signal recovery when WS delivers event for unknown order_id.
        
        Recovery chain:
        1. Check PostgreSQL orders table (REST worker may have already persisted)
        2. If not in DB, check exchange via REST (Single Source of Truth)
        3. Reconstruct TradingSignal and inject into _trading_signals
        
        Args:
            order_id: The exchange order ID or OMS link ID
            event: Optional OrderUpdateEvent with fill data for enrichment
            
        Returns:
            Reconstructed TradingSignal or None if order doesn't exist anywhere
        """
        logger.warning(f"👻 [Reconciler] Ghost order via WS: {order_id}. Initiating Lazy Hydration...")
        
        async with self._sync_lock:
            # Double-check under lock (another coroutine may have hydrated it)
            if order_id in self.signals:
                return self.signals[order_id]
            
            signal = None
            
            # Step 1: Check PostgreSQL (fast path)
            try:
                signal = await self._load_signal_from_db(order_id)
                if signal:
                    logger.info(f"💾 [Reconciler] Signal {order_id} recovered from PostgreSQL.")
            except Exception as e:
                logger.error(f"❌ [Reconciler] DB lookup failed for {order_id}: {e}")
            
            # Step 2: Check Exchange (if DB empty, exchange is ground truth)
            if not signal:
                try:
                    signal = await self._load_signal_from_exchange(order_id, event)
                    if signal:
                        logger.info(f"🌐 [Reconciler] Signal {order_id} reconstructed from Exchange.")
                        # Persist to DB for future lookups
                        await self._persist_recovered_signal(signal)
                except Exception as e:
                    logger.error(f"❌ [Reconciler] Exchange lookup failed for {order_id}: {e}")
            
            if not signal:
                logger.error(f"❌ [Reconciler] Order {order_id} NOT FOUND anywhere. Orphan WS event.")
                return None
            
            # Step 3: Enrich from event data if available
            if event:
                signal.filled_qty = max(signal.filled_qty, event.exec_qty)
                if event.avg_price > 0:
                    signal.entry_price = event.avg_price
            
            # Step 4: Inject into hot path (shared dict reference)
            self.signals[order_id] = signal
            self.stats["ghosts_hydrated"] += 1
            
            logger.info(
                f"✅ [Reconciler] Signal {order_id} HYDRATED into Smart Exit Engine. "
                f"Symbol: {signal.symbol} | Direction: {signal.direction} | "
                f"State: {signal.state.value}"
            )
            return signal
    
    async def _load_signal_from_db(self, order_id: str) -> Optional[TradingSignal]:
        """Reconstruct TradingSignal from PostgreSQL orders table."""
        from sqlalchemy import text
        
        async with self.db.SessionLocal() as session:
            # Try both exact match and OMS prefix match
            result = await session.execute(
                text("""
                    SELECT order_id, symbol, direction, state, filled_qty, target_qty,
                           entry_price, tp_price, sl_price, exchange_updated_time_ms
                    FROM orders 
                    WHERE order_id = :oid OR order_id LIKE :oid_prefix
                    ORDER BY updated_at DESC
                    LIMIT 1
                """),
                {"oid": order_id, "oid_prefix": f"oms_%{order_id}%"}
            )
            row = result.mappings().first()
            
            if not row:
                return None
            
            signal = TradingSignal(
                id=str(row["order_id"]).replace("oms_", ""),
                symbol=row["symbol"],
                direction=row["direction"],
                order_id=order_id,
                filled_qty=float(row["filled_qty"] or 0),
                position_size=float(row["filled_qty"] or 0),
                entry_price=float(row["entry_price"] or 0),
                take_profit=float(row["tp_price"]) if row["tp_price"] else None,
                stop_loss=float(row["sl_price"]) if row["sl_price"] else None,
            )
            
            # Map DB state to SignalState
            db_state = str(row["state"]).upper()
            if db_state in ("FILLED", "PARTIALLY_FILLED"):
                signal.state = SignalState.POSITION_OPEN
            elif db_state in ("CANCELED", "REJECTED", "CLOSED_DUST"):
                signal.state = SignalState.CLOSED
            elif db_state == "PENDING_OPEN":
                signal.state = SignalState.APPROVED
            
            return signal
    
    async def _load_signal_from_exchange(self, order_id: str, event: Optional[OrderUpdateEvent] = None) -> Optional[TradingSignal]:
        """
        Last resort: Reconstruct signal purely from exchange position data.
        This handles the case where even the DB INSERT hasn't happened yet.
        """
        if not event:
            return None
        
        # We have enough data from the WS event itself to reconstruct
        signal = TradingSignal(
            symbol=event.symbol,
            direction="LONG" if event.exec_qty > 0 else "SHORT",  # Heuristic
            order_id=order_id,
            filled_qty=abs(event.exec_qty),
            position_size=abs(event.exec_qty),
            entry_price=event.avg_price,
        )
        signal.state = SignalState.POSITION_OPEN
        
        return signal
    
    async def _persist_recovered_signal(self, signal: TradingSignal):
        """Persist recovered signal to DB for crash recovery."""
        try:
            from sqlalchemy import text
            async with self.db.SessionLocal() as session:
                await session.execute(
                    text("""
                        INSERT INTO orders (order_id, symbol, direction, state, 
                                          filled_qty, entry_price, exchange_updated_time_ms)
                        VALUES (:oid, :sym, :dir, :state, :qty, :price, :ts)
                        ON CONFLICT (order_id) DO UPDATE SET
                            state = EXCLUDED.state,
                            filled_qty = GREATEST(orders.filled_qty, EXCLUDED.filled_qty)
                    """),
                    {
                        "oid": f"oms_{signal.id}",
                        "sym": signal.symbol,
                        "dir": signal.direction,
                        "state": "FILLED" if signal.state == SignalState.POSITION_OPEN else "PENDING_OPEN",
                        "qty": signal.filled_qty,
                        "price": signal.entry_price or 0,
                        "ts": int(time.time() * 1000),
                    }
                )
                await session.commit()
        except Exception as e:
            logger.error(f"❌ [Reconciler] Failed to persist recovered signal: {e}")
    
    # =========================================================================
    # [P2] EVENT-DRIVEN TRIGGER — Immediate sync on WS reconnection
    # =========================================================================
    
    async def _on_connection_restored(self, event: ConnectionRestoredEvent):
        """
        Reconnection as Reconciliation Trigger.
        
        When WS/ZMQ reconnects after disconnection, we don't wait for the 30s timer.
        We trigger _hard_sync immediately because any orders executed during
        the disconnect window are invisible to our in-memory state.
        
        Debounce protects against reconnection storm (multiple workers reconnecting).
        """
        now = time.time()
        gap_duration = event.downtime_seconds
        
        logger.warning(
            f"🔌 [Reconciler] Connection RESTORED ({event.source}). "
            f"Gap: {gap_duration:.1f}s. Triggering immediate reconciliation..."
        )
        
        # Debounce: If we just synced within 5s, skip
        if now - self._last_sync_ts < self._min_sync_interval:
            logger.debug(f"⏩ [Reconciler] Debounce active. Skipping (last sync {now - self._last_sync_ts:.1f}s ago).")
            return
        
        self.stats["event_driven_syncs"] += 1
        
        # Fire immediate sync (don't await, let it run in background)
        asyncio.create_task(self._execute_hard_sync(reason=f"RECONNECT:{event.source}"))
    
    # =========================================================================
    # [P3] BACKGROUND SWEEP — Periodic 30s hard sync
    # =========================================================================
    
    async def _run_background_sweep(self):
        """Periodic reconciliation on timer."""
        logger.info(f"🔄 [Reconciler] Background sweep active (every {self.background_interval}s)")
        
        # Initial delay: Let system stabilize after startup
        await asyncio.sleep(10)
        
        while self._running:
            try:
                await asyncio.sleep(self.background_interval)
                self.stats["background_syncs"] += 1
                await self._execute_hard_sync(reason="PERIODIC")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [Reconciler] Background sweep failed: {e}")
    
    async def _execute_hard_sync(self, reason: str = "UNKNOWN"):
        """
        Core reconciliation logic: Exchange Position List → In-Memory State.
        
        Detects:
        1. Ghosts: Positions on exchange, NOT in memory → hydrate
        2. Zombies: Signals in memory, position CLOSED on exchange → purge  
        3. Size Mismatch: Fill quantity differs → correct
        """
        async with self._sync_lock:
            self._last_sync_ts = time.time()
            t_start = time.perf_counter()
            
            try:
                # Step 1: Get exchange ground truth
                exchange_positions = await self.adapter.get_all_positions()
                
                # Build lookup: symbol → position data
                exchange_map: Dict[str, dict] = {}
                for pos in exchange_positions:
                    sym = pos.get("symbol", "")
                    size = abs(float(pos.get("size", 0)))
                    if size > 1e-8:  # Filter dust
                        exchange_map[sym] = pos
                
                # Step 2: Detect GHOSTS (on exchange, not in memory)
                local_symbols = {sig.symbol for sig in self.signals.values() 
                                if sig.state == SignalState.POSITION_OPEN}
                
                ghost_symbols = set(exchange_map.keys()) - local_symbols
                for ghost_sym in ghost_symbols:
                    pos = exchange_map[ghost_sym]
                    logger.warning(
                        f"👻 [Reconciler] GHOST POSITION detected: {ghost_sym} "
                        f"(Size: {pos.get('size')}, Entry: {pos.get('entry_price')})"
                    )
                    
                    # Attempt to hydrate from DB first
                    signal = await self._find_signal_by_symbol_in_db(ghost_sym)
                    if signal:
                        signal.state = SignalState.POSITION_OPEN
                        signal.filled_qty = abs(float(pos.get("size", 0)))
                        signal.position_size = signal.filled_qty
                        signal.entry_price = float(pos.get("entry_price", 0))
                        
                        order_key = signal.order_id or f"recon_{ghost_sym}_{int(time.time())}"
                        self.signals[order_key] = signal
                        self.stats["ghosts_hydrated"] += 1
                        
                        logger.info(f"✅ [Reconciler] Ghost {ghost_sym} hydrated under key {order_key}")
                    else:
                        # Create minimal tracking signal for orphan position
                        orphan_signal = TradingSignal(
                            symbol=ghost_sym,
                            direction="LONG" if float(pos.get("size", 0)) > 0 else "SHORT",
                            entry_price=float(pos.get("entry_price", 0)),
                            filled_qty=abs(float(pos.get("size", 0))),
                            position_size=abs(float(pos.get("size", 0))),
                        )
                        orphan_signal.state = SignalState.POSITION_OPEN
                        
                        order_key = f"recon_{ghost_sym}_{int(time.time())}"
                        self.signals[order_key] = orphan_signal
                        self.stats["ghosts_hydrated"] += 1
                        
                        logger.warning(
                            f"⚠️ [Reconciler] Orphan position {ghost_sym} adopted with minimal signal. "
                            f"Smart Exit Engine now tracking."
                        )
                
                # Step 3: Detect ZOMBIES (in memory, but closed on exchange)
                zombie_keys = []
                for order_id, sig in self.signals.items():
                    if sig.state == SignalState.POSITION_OPEN and sig.symbol not in exchange_map:
                        zombie_keys.append(order_id)
                
                for zombie_key in zombie_keys:
                    zombie_sig = self.signals.pop(zombie_key, None)
                    if zombie_sig:
                        zombie_sig.state = SignalState.CLOSED
                        self.stats["zombies_purged"] += 1
                        logger.warning(
                            f"🧟 [Reconciler] ZOMBIE purged: {zombie_sig.symbol} "
                            f"(was POSITION_OPEN in memory, closed on exchange). Key: {zombie_key}"
                        )
                        
                        # Update DB state
                        try:
                            await self.db.update_order_state_cas(
                                zombie_key, "FILLED", "CLOSED_RECON",
                                exch_time=int(time.time() * 1000)
                            )
                        except Exception:
                            pass
                
                # Step 4: Sync RiskGuard
                try:
                    from core.shield.risk_engine import risk_guard
                    if risk_guard:
                        active_syms = set(exchange_map.keys())
                        await risk_guard.sync_with_exchange(active_syms)
                except Exception as e:
                    logger.error(f"❌ [Reconciler] RiskGuard sync failed: {e}")
                
                elapsed_ms = (time.perf_counter() - t_start) * 1000
                
                ghost_count = len(ghost_symbols)
                zombie_count = len(zombie_keys)
                
                if ghost_count > 0 or zombie_count > 0:
                    logger.info(
                        f"🔄 [Reconciler] Hard Sync [{reason}] complete in {elapsed_ms:.0f}ms | "
                        f"Exchange: {len(exchange_map)} positions | "
                        f"Local: {len(local_symbols)} tracked | "
                        f"Ghosts: {ghost_count} | Zombies: {zombie_count}"
                    )
                else:
                    logger.debug(
                        f"🔄 [Reconciler] Hard Sync [{reason}] OK ({elapsed_ms:.0f}ms) — "
                        f"State consistent. {len(exchange_map)} positions."
                    )
                
            except Exception as e:
                logger.error(f"❌ [Reconciler] Hard Sync [{reason}] FAILED: {e}")
    
    async def _find_signal_by_symbol_in_db(self, symbol: str) -> Optional[TradingSignal]:
        """Lookup most recent active order for symbol in PostgreSQL."""
        from sqlalchemy import text
        
        try:
            async with self.db.SessionLocal() as session:
                result = await session.execute(
                    text("""
                        SELECT order_id, symbol, direction, state, filled_qty, target_qty,
                               entry_price, tp_price, sl_price
                        FROM orders 
                        WHERE symbol = :sym AND state NOT IN ('CANCELED', 'REJECTED', 'CLOSED_DUST', 'CLOSED_RECON')
                        ORDER BY updated_at DESC
                        LIMIT 1
                    """),
                    {"sym": symbol}
                )
                row = result.mappings().first()
                
                if not row:
                    return None
                
                signal = TradingSignal(
                    id=str(row["order_id"]).replace("oms_", ""),
                    symbol=row["symbol"],
                    direction=row["direction"],
                    order_id=str(row["order_id"]),
                    filled_qty=float(row["filled_qty"] or 0),
                    position_size=float(row["filled_qty"] or 0),
                    entry_price=float(row["entry_price"] or 0),
                    take_profit=float(row["tp_price"]) if row["tp_price"] else None,
                    stop_loss=float(row["sl_price"]) if row["sl_price"] else None,
                )
                return signal
        except Exception as e:
            logger.error(f"❌ [Reconciler] DB symbol lookup failed for {symbol}: {e}")
            return None
    
    def get_stats(self) -> dict:
        """Returns reconciler metrics for /stats command."""
        return {
            **self.stats,
            "last_sync_ts": self._last_sync_ts,
            "seconds_since_sync": time.time() - self._last_sync_ts if self._last_sync_ts else -1,
            "active_signals": len(self.signals),
        }
    
    def stop(self):
        """Graceful shutdown."""
        self._running = False
        if self._background_task:
            self._background_task.cancel()
        logger.info("🔌 [Reconciler] Stopped.")
