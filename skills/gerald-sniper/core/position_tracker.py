import asyncio
import time
from datetime import datetime
from loguru import logger
from typing import Dict, Any

class PaperTracker:
    def __init__(self, db_manager):
        self.db = db_manager
        # Mapping: alert_id -> {symbol, direction, entry, stop, target, ts}
        self.positions: Dict[int, Dict[str, Any]] = {}
        self.is_running = False

    async def load_active_positions(self):
        """Preload unresolved alerts from the database."""
        if not self.db:
            return
        
        try:
            unresolved = await self.db.get_unresolved_alerts(days=3)  # Only track up to 3 days old
            count = 0
            for row in unresolved:
                alert_id = row['id']
                try:
                    ts = datetime.fromisoformat(row['timestamp']).timestamp()
                except Exception:
                    ts = time.time()
                    
                self.positions[alert_id] = {
                    'symbol': row['symbol'],
                    'direction': row['direction'],
                    'entry': row['entry_price'],
                    'stop': row['stop_suggestion'],
                    'target': row['target_suggestion'],
                    'ts': ts
                }
                count += 1
            if count > 0:
                logger.info(f"📊 PaperTracker loaded {count} active positions from DB.")
        except Exception as e:
            logger.error(f"Failed to load active positions into PaperTracker: {e}")

    def add_position(self, alert_id: int, symbol: str, direction: str, entry_price: float, stop_price: float, target_price: float):
        if not alert_id:
            return
            
        self.positions[alert_id] = {
            'symbol': symbol,
            'direction': direction,
            'entry': entry_price,
            'stop': stop_price,
            'target': target_price,
            'ts': time.time(),
        }
        logger.info(f"📊 PaperTracker tracking new {direction} position for {symbol} (Alert ID: {alert_id})")

    async def check_prices(self, current_prices: Dict[str, float]):
        """
        Check all active paper-trading positions against the current prices.
        Called by CandleManager whenever M5 or M15 candles close, 
        or via a dedicated tracking loop.
        """
        to_close = []
        now = time.time()
        
        for aid, pos in self.positions.items():
            sym = pos['symbol']
            
            # Not all symbols might have price updates this tick, so skip if absent
            if sym not in current_prices:
                # Also check timeout (72h max hold time)
                if now - pos['ts'] > 259200:
                    to_close.append((aid, 'TIMEOUT', 0.0))
                continue
                
            current_price = current_prices[sym]
            result = None
            pnl_pct = 0.0

            if pos['direction'] == 'LONG':
                if current_price <= pos['stop']:
                    result = 'LOSS'
                    pnl_pct = ((current_price - pos['entry']) / pos['entry']) * 100
                elif current_price >= pos['target']:
                    result = 'WIN'
                    pnl_pct = ((current_price - pos['entry']) / pos['entry']) * 100
            else: # SHORT
                if current_price >= pos['stop']:
                    result = 'LOSS'
                    pnl_pct = ((pos['entry'] - current_price) / pos['entry']) * 100
                elif current_price <= pos['target']:
                    result = 'WIN'
                    pnl_pct = ((pos['entry'] - current_price) / pos['entry']) * 100
            
            if not result and (now - pos['ts'] > 259200):
                result = 'TIMEOUT'
                # Close at current price
                if pos['direction'] == 'LONG':
                    pnl_pct = ((current_price - pos['entry']) / pos['entry']) * 100
                else:
                    pnl_pct = ((pos['entry'] - current_price) / pos['entry']) * 100

            if result:
                to_close.append((aid, result, pnl_pct))

        for aid, result, pnl_pct in to_close:
            pos = self.positions.pop(aid)
            emoji = "🟢" if "WIN" in result else "🔴" if "LOSS" in result else "⚪"
            logger.info(f"{emoji} Paper Trade Closed: {pos['symbol']} {pos['direction']} -> {result} ({pnl_pct:+.2f}%)")
            
            if self.db:
                try:
                    await self.db.update_alert_result(aid, result, pnl_pct, "Auto-closed by PaperTracker")
                except Exception as e:
                    logger.error(f"Failed to update alert result for ID {aid}: {e}")
