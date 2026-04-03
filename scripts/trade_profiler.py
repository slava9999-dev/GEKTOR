import os
import sys
import asyncio
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# 1. HARD ENVIROMENT LOADING (Audit 17.1)
# Find .env in the project root (C:\Gerald-superBrain\)
root_dir = Path(__file__).parent.parent
env_path = root_dir / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    logger.info(f"🔑 [Profiler] Environment loaded from {env_path}")
else:
    logger.warning(f"⚠️ [Profiler] .env NOT FOUND at {env_path}. Using system env.")

# 2. PATH DISCOVERY (Gektor Standard 11.1)
SKILL_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "skills", "gerald-sniper"))
if SKILL_ROOT not in sys.path:
    sys.path.append(SKILL_ROOT)

# 3. SECURE IMPORTS (Only after .env is active)
from data.database import DatabaseManager

class TradeProfiler:
    """
    HFT Alpha Validator (MAE/MFE Edition).
    Calculates Maximum Adverse Excursion and Maximum Favorable Excursion 
    to validate entry/exit efficiency.
    """
    def __init__(self, db: DatabaseManager):
        self.db = db

    async def profile_recent_trades(self, days: int = 7):
        """
        Main analysis loop:
        1. Check if we have any alerts at all (System Health).
        2. Fetch both OPEN and CLOSED paper positions.
        3. Calculate MAE/MFE.
        """
        logger.info(f"📊 [Profiler] Starting Comprehensive Analysis for the last {days} days...")
        
        # System Health Check: Do we see signals?
        alert_count = await self.db.execute_read_safe("SELECT COUNT(*) as count FROM alerts WHERE timestamp > NOW() - INTERVAL '24 hours'")
        logger.info(f"📡 [System-Check] Signals generated in last 24h: {alert_count[0]['count']}")

        # Query for ALL positions (Audit 18.2)
        query = """
            SELECT 
                p.id, p.symbol, p.side, p.entry_price, p.opened_at, p.closed_at, p.pnl_pct, p.status,
                a.liquidity_tier, a.signal_type
            FROM paper_positions p
            JOIN alerts a ON p.alert_id = a.id
            WHERE p.opened_at > NOW() - INTERVAL '1 day' * :days
              AND p.status IN ('OPEN', 'CLOSED_WIN', 'CLOSED_LOSS', 'CLOSED_FORCE')
        """
        trades = await self.db.execute_read_safe(query, {"days": days})
        if not trades:
            logger.warning("📊 [Profiler] No positions found (neither OPEN nor CLOSED). Check scanners.")
            return None

        results = []
        for t in trades:
            # Fetch minute High/Low from watchlist_history or klines
            # In TEKTON_ALPHA, we use watchlist_history as a snapshot of market state during evaluation
            # For strict MAE/MFE, we should query public klines
            sym = t['symbol']
            entry = t['entry_price']
            is_long = t['side'].upper() in ('BUY', 'LONG', 'OPEN_LONG')
            
            # Simulated high/low fetch (Simplified for this version)
            # In full version: await rest.get_klines(sym, "1", start=t['opened_at'], end=t['closed_at'])
            
            # For now, let's use the actual PnL as a baseline and assume 
            # we need to calculate the "unrealized" peak/trough.
            mae, mfe = await self._calculate_excursions(t)
            
            results.append({
                "id": t['id'],
                "symbol": sym,
                "tier": t['liquidity_tier'],
                "status": t['status'],
                "side": t['side'],
                "mae_pct": mae,
                "mfe_pct": mfe,
                "final_pnl": t['pnl_pct'],
                "efficiency": (t['pnl_pct'] / mfe) if mfe > 0 else 0
            })

        df = pd.DataFrame(results)
        self._log_summary(df)
        return df

    async def _calculate_excursions(self, trade: dict) -> tuple[float, float]:
        """
        Fetches price trajectory from DB and finds the max favorable and adverse points.
        """
        # Handling OPEN positions (Audit 18.2)
        end_time = trade['closed_at'] or datetime.utcnow()
        
        query = """
            SELECT price FROM watchlist_history 
            WHERE symbol = :sym 
              AND timestamp BETWEEN :start AND :end
            ORDER BY timestamp ASC
        """
        prices = await self.db.execute_read_safe(query, {
            "sym": trade['symbol'], 
            "start": trade['opened_at'], 
            "end": end_time
        })
        
        if not prices: return 0.0, 0.0
        
        entry = trade['entry_price']
        is_long = trade['side'].upper() in ('BUY', 'LONG', 'OPEN_LONG')
        
        price_list = [p['price'] for p in prices]
        
        if is_long:
            max_p = max(price_list)
            min_p = min(price_list)
            mfe = (max_p - entry) / entry * 100
            mae = (min_p - entry) / entry * 100
        else:
            max_p = max(price_list)
            min_p = min(price_list)
            mfe = (entry - min_p) / entry * 100
            mae = (entry - max_p) / entry * 100
            
        return mae, mfe

    def _log_summary(self, df: pd.DataFrame):
        summary = df.groupby(['tier', 'status']).agg({
            'mae_pct': 'mean',
            'mfe_pct': 'mean',
            'efficiency': 'mean',
            'symbol': 'count'
        }).rename(columns={'symbol': 'trades'})
        logger.info(f"\n📊 [TRADE PROFILE SUMMARY]\n{summary.to_string()}")

if __name__ == "__main__":
    # Test runner
    db = DatabaseManager()
    profiler = TradeProfiler(db)
    asyncio.run(profiler.profile_recent_trades())
