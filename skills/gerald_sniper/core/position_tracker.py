import asyncio
import time
from datetime import datetime
from loguru import logger
from typing import Dict, Any

class PaperTracker:
    def __init__(self, db_manager):
        self.db = db_manager
        # Mapping: alert_id -> {symbol, direction, entry, stop, target, ts, tier}
        self.positions: Dict[int, Dict[str, Any]] = {}
        self.is_running = True
        self._last_tracker_run = 0.0
        
        # GC Worker (Task 2.4)
        asyncio.create_task(self._gc_loop())

    async def _gc_loop(self):
        """Asynchronous worker that closes old state-stuck positions."""
        while self.is_running:
            try:
                await asyncio.sleep(60)
                now = time.time()
                to_force_close = []
                
                # Check ALL symbols in Price Tracker
                for aid, pos in list(self.positions.items()):
                    # Rule 2.4: 4 hour hard timeout (14400s)
                    if now - pos['ts'] > 14400:
                        to_force_close.append((aid, pos))
                
                for aid, pos in to_force_close:
                    logger.warning(f"🧹 [GC] Force closing stale position {pos['symbol']} (Age: >4h)")
                    # We'll close it in check_prices or direct call
                    await self.check_prices({pos['symbol']: -1.0}) # -1 code for FORCE_CLOSE
            except Exception as e:
                logger.error(f"❌ [PaperTracker] GC Loop error: {e}")

    def stop(self):
        self.is_running = False

    def get_stats(self) -> Dict[str, Any]:
        """Returns current tracking summary."""
        now = time.time()
        longs = len([p for p in self.positions.values() if p['direction'] == 'LONG'])
        shorts = len([p for p in self.positions.values() if p['direction'] == 'SHORT'])
        
        # Calculate time in trade for oldest/newest
        lifetimes = [now - p['ts'] for p in self.positions.values()]
        avg_life = sum(lifetimes) / len(lifetimes) / 3600 if lifetimes else 0
        
        return {
            "total_open": len(self.positions),
            "long_cnt": longs,
            "short_cnt": shorts,
            "avg_hold_h": round(avg_life, 1),
            "last_check_ts": self._last_tracker_run
        }

    async def load_active_positions(self):
        """Preload OPEN positions from the paper_positions table."""
        if not self.db:
            return
        
        try:
            active = await self.db.get_active_paper_positions()
            count = 0
            for row in active:
                pos_id = row['id']
                # Opened_at is TIMESTAMPTZ, row['opened_at'] should be datetime object
                ts = row['opened_at'].timestamp() if hasattr(row['opened_at'], 'timestamp') else time.time()
                    
                self.positions[pos_id] = {
                    'id': pos_id, 
                    'alert_id': row['alert_id'],
                    'symbol': row['symbol'],
                    'direction': row['side'],
                    'entry': row['entry_price'],
                    'qty': row['entry_qty'],
                    'stop': row['sl_price'],
                    'target': row['tp_price'],
                    'ts': ts,
                    'fee': row['fee_paid']
                }
                count += 1
            if count > 0:
                logger.info(f"📊 PaperTracker loaded {count} active positions from paper_positions database.")
        except Exception as e:
            logger.error(f"Failed to load active positions into PaperTracker: {e}")

    def add_position(self, pos_id: int, symbol: str, direction: str, entry_price: float, qty: float, stop_price: float, target_price: float, alert_id: int = None):
        """Adds a position to memory tracker (called by VirtualExchangeAdapter)."""
        self.positions[pos_id] = {
            'id': pos_id,
            'alert_id': alert_id,
            'symbol': symbol,
            'direction': direction,
            'entry': entry_price,
            'qty': qty,
            'stop': stop_price,
            'target': target_price,
            'ts': time.time(),
            # [NERVE REPAIR v4.2] Virtual Trailing Logic
            'highest_mark': entry_price if direction == 'LONG' else 0.0,
            'last_atr': 0.0 # Will be updated by price loop or signal meta
        }
        logger.info(f"📊 PaperTracker tracking new {direction} position for {symbol} (ID: {pos_id})")

    def absorb_orphan_position(self, pos: Dict) -> bool:
        """
        Adopts an existing position from the exchange into local memory.
        Ensures local state matches ground truth (Audit 15.3).
        """
        try:
            symbol = pos.get("symbol")
            side = pos.get("side")
            size = float(pos.get("size", 0))
            entry = float(pos.get("avgPrice", 0)) or float(pos.get("marketPrice", 0))
            
            # Create a virtual ID for the orphan
            pos_id = int(time.time() * 1000) % 1000000 
            
            self.positions[pos_id] = {
                'id': pos_id, 
                'alert_id': None,
                'symbol': symbol,
                'direction': 'LONG' if side == 'Buy' else 'SHORT',
                'entry': entry,
                'qty': size,
                'stop': 0.0, # Unknown. Reconciler should ideally fetch these separately.
                'target': 0.0,
                'ts': time.time()
            }
            logger.warning(f"🔄 [TRACKER] Orphan position ABSORBED: {symbol} | {side} | {size} @ {entry}")
            return True
        except Exception as e:
            logger.error(f"❌ [TRACKER] Failed to absorb orphan: {e}")
            return False

    def start(self):
        """ Gerald v6.0: Subscribes to real-time price updates via EventBus. """
        from core.events.events import PriceUpdateEvent
        from core.events.nerve_center import bus
        bus.subscribe(PriceUpdateEvent, self.handle_price_event)
        logger.info("🕵️ [Tracker] PaperTracker ACTIVE and monitoring EventBus for price updates.")

    async def handle_price_event(self, event):
        """Async callback for PriceUpdateEvent."""
        if event.prices:
            # Optionally pass ATR from event if available, or fetch from CandleCache
            await self.check_prices(event.prices)

    def _update_trailing_stop(self, pos: Dict, current_price: float):
        """[NERVE REPAIR v4.2] Dynamic Trailing based on High Water Mark."""
        if pos['direction'] == 'LONG':
            if current_price > pos['highest_mark']:
                pos['highest_mark'] = current_price
                atr = pos.get('last_atr', 0.0) or (current_price * 0.015)
                new_sl = current_price - (atr * 1.5)
                if new_sl > pos['stop']:
                    pos['stop'] = new_sl
        else: # SHORT
            if current_price < pos['highest_mark'] or pos['highest_mark'] == 0:
                pos['highest_mark'] = current_price
                atr = pos.get('last_atr', 0.0) or (current_price * 0.015)
                new_sl = current_price + (atr * 1.5)
                if pos['stop'] == 0 or new_sl < pos['stop']:
                    pos['stop'] = new_sl

    async def check_prices(self, current_prices: Dict[str, float]):
        """
        Check all active paper-trading positions against the current prices.
        Includes commissions (0.11%) and tiered slippage (Task 2.3).
        """
        to_close = []
        now = time.time()
        
        for aid, pos in self.positions.items():
            sym = pos['symbol']
            
            # Not all symbols might have price updates this tick, so skip if absent
            # Special case for GC: if price is -1.0, it's a force close triggered by _gc_loop
            is_gc_close = current_prices.get(sym) == -1.0
            
            if sym not in current_prices and not is_gc_close:
                # Still check hard timeout 3 days as fallback (Rule 1.x)
                if now - pos['ts'] > 259200:
                    to_close.append((aid, 'TIMEOUT', 0.0))
                continue
                
            current_price = current_prices[sym] if not is_gc_close else pos['entry'] 
            result = None
            raw_pnl_pct = 0.0
            
            # [NERVE REPAIR v4.2] Dynamic Logic
            if not is_gc_close:
                # 1. Update Trailing
                self._update_trailing_stop(pos, current_price)
                
                # 2. Check toxic L2 Bailout
                from core.realtime.market_state import market_state
                if market_state.is_toxic(sym, pos['direction']):
                    result = 'CLOSED_BAILOUT' # Exit before stop hit
                    exit_price = current_price
                    raw_pnl_pct = ((exit_price - pos['entry']) / pos['entry']) * 100 if pos['direction'] == 'LONG' else ((pos['entry'] - exit_price) / pos['entry']) * 100

            if result:
                pass # Already set by Bailout
            elif is_gc_close:
                result = 'FORCE_CLOSE'
                raw_pnl_pct = 0.0 
            elif pos['direction'] == 'LONG':
                if current_price <= pos['stop']:
                    result = 'CLOSED_LOSS'
                    exit_price = pos['stop'] # Assume fill at stop trigger for paper
                    raw_pnl_pct = ((exit_price - pos['entry']) / pos['entry']) * 100
                elif pos['target'] > 0 and current_price >= pos['target']:
                    result = 'CLOSED_WIN'
                    exit_price = pos['target'] 
                    raw_pnl_pct = ((exit_price - pos['entry']) / pos['entry']) * 100
            else: # SHORT
                if pos['stop'] > 0 and current_price >= pos['stop']:
                    result = 'CLOSED_LOSS'
                    exit_price = pos['stop']
                    raw_pnl_pct = ((pos['entry'] - exit_price) / pos['entry']) * 100
                elif pos['target'] > 0 and current_price <= pos['target']:
                    result = 'CLOSED_WIN'
                    exit_price = pos['target']
                    raw_pnl_pct = ((pos['entry'] - exit_price) / pos['entry']) * 100
            
            if not result and (now - pos['ts'] > 259200):
                result = 'TIMEOUT'
                raw_pnl_pct = ((current_price - pos['entry']) / pos['entry']) * 100 if pos['direction'] == 'LONG' else ((pos['entry'] - current_price) / pos['entry']) * 100

            if result:
                # Task 2.3: Commissions Model
                fee_pct = 0.11 # Bybit Taker Round Trip (0.055% * 2)
                
                # Deduct costs from PnL (Slippage already accounted for via exit_price if LOSS)
                final_pnl_pct = raw_pnl_pct - fee_pct
                
                to_close.append((aid, result, final_pnl_pct))

        self._last_tracker_run = now
        for aid, result, pnl_pct in to_close:
            pos = self.positions.pop(aid, None)
            if not pos:
                continue
                
            emoji = "🟢" if "WIN" in result else "🔴" if "LOSS" in result else "⚪"
            logger.info(f"{emoji} Paper Trade Closed: {pos['symbol']} {pos['direction']} -> {result} ({pnl_pct:+.2f}%) | Costs applied")
            
            if self.db:
                try:
                    # Update alerts table (legacy sync)
                    if pos['alert_id']:
                        await self.db.update_alert_result(pos['alert_id'], result, pnl_pct, f"Auto-closed (Reason: {result})")
                    
                    # Update paper_positions table (New Standard)
                    pnl_usd = (pnl_pct / 100.0) * (pos['entry'] * pos['qty'])
                    await self.db.update_paper_position(aid, result, pnl_usd, pnl_pct)
                except Exception as e:
                    logger.error(f"Failed to update paper position ID {aid}: {e}")

    async def track_unwatched_symbols(self, rest_client):
        """
        Gerald v4.2: Dedicated loop for 'cold' positions.
        Fetches prices for symbols having open positions but NOT in active watchlist.
        """
        while True:
            try:
                # 1. Identify symbols to check
                # Note: symbols in active watchlist are already checked via WS in CandleCache
                all_symbols = list(set(p['symbol'] for p in self.positions.values()))
                if not all_symbols:
                    await asyncio.sleep(300)
                    continue
                
                # We check ALL symbols every 10 min to be sure
                logger.debug(f"🔍 Background price check for {len(all_symbols)} potential positions...")
                
                # Fetch tickers in bulk if possible, or individual ones
                tickers = await rest_client.get_tickers()
                if not tickers:
                    await asyncio.sleep(60)
                    continue
                
                price_map = {t['symbol']: float(t['lastPrice']) for t in tickers if 'lastPrice' in t}
                
                # Filter price map to only include our positions
                check_map = {s: price_map[s] for s in all_symbols if s in price_map}
                
                if check_map:
                    await self.check_prices(check_map)
                    
            except Exception as e:
                logger.error(f"Error in PaperTracker background loop: {e}")
                
            await asyncio.sleep(600) # Every 10 min
