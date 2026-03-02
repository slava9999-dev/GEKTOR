import aiosqlite
from loguru import logger
import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    async def initialize(self):
        """Creates tables if they don't exist."""
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS watchlist_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    rvol REAL NOT NULL,
                    delta_oi_4h_pct REAL,
                    atr_pct REAL NOT NULL,
                    funding_rate REAL,
                    price_change_24h_pct REAL,
                    composite_score REAL NOT NULL,
                    direction TEXT NOT NULL
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS detected_levels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    level_price REAL NOT NULL,
                    level_type TEXT NOT NULL,
                    touches INTEGER NOT NULL,
                    strength_score REAL NOT NULL,
                    timeframe TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    level_price REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_suggestion REAL,
                    target_suggestion REAL,
                    total_score INTEGER NOT NULL,
                    score_breakdown TEXT NOT NULL,
                    rvol REAL,
                    delta_oi_pct REAL,
                    funding_rate REAL,
                    btc_trend TEXT,
                    chart_path TEXT,
                    result TEXT DEFAULT NULL,
                    pnl_pct REAL DEFAULT NULL,
                    user_notes TEXT DEFAULT NULL
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS candle_cache (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open_time TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    volume REAL,
                    PRIMARY KEY (symbol, timeframe, open_time)
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS btc_context (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    btc_price REAL NOT NULL,
                    btc_change_1h_pct REAL,
                    btc_change_4h_pct REAL,
                    btc_trend TEXT NOT NULL,
                    btc_rsi_h1 REAL,
                    total_market_funding_avg REAL
                )
            """)
            
            # Create indices silently if they don't exist
            try:
                await db.execute("CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alerts(symbol)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_alerts_result ON alerts(result)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_timestamp ON watchlist_history(timestamp)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_candles ON candle_cache(symbol, timeframe)")
            except aiosqlite.OperationalError:
                pass # Indices likely already exist
                
            await db.commit()
            logger.info(f"Database initialized at {self.db_path}")

    # =========================================================================
    # WRITE OPERATIONS
    # =========================================================================

    async def insert_watchlist_history(self, records: list[dict]):
        """Inserts radar scan results into watchlist_history."""
        timestamp = datetime.utcnow().isoformat()
        
        async with aiosqlite.connect(self.db_path) as db:
            for r in records:
                m = r["metrics"]
                await db.execute("""
                    INSERT INTO watchlist_history 
                    (timestamp, symbol, rvol, delta_oi_4h_pct, atr_pct, funding_rate, price_change_24h_pct, composite_score, direction)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (timestamp, m.symbol, m.rvol, m.delta_oi_4h_pct, m.atr_pct, m.funding_rate, r["price_change_24h_pct"], r["score"], m.direction))
            await db.commit()

    async def insert_detected_levels(self, symbol: str, timeframe: str, levels: list[dict]):
        """Saves calculated mathematically significant levels into the DB."""
        timestamp = datetime.utcnow().isoformat()
        
        async with aiosqlite.connect(self.db_path) as db:
            for lvl in levels:
                await db.execute("""
                    INSERT INTO detected_levels 
                    (timestamp, symbol, level_price, level_type, touches, strength_score, timeframe, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """, (timestamp, symbol, lvl['price'], lvl['type'], lvl['touches'], lvl['strength'], timeframe))
            await db.commit()

    async def insert_alert(
        self, symbol: str, direction: str, signal_type: str,
        level_price: float, entry_price: float,
        stop_price: float, target_price: float,
        total_score: int, score_breakdown: dict,
        rvol: float, delta_oi_pct: float, funding_rate: float,
        btc_trend: str
    ):
        """Saves a fired alert for post-analysis by Gerald AI."""
        timestamp = datetime.utcnow().isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO alerts 
                (timestamp, symbol, direction, signal_type, level_price, entry_price,
                 stop_suggestion, target_suggestion, total_score, score_breakdown,
                 rvol, delta_oi_pct, funding_rate, btc_trend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp, symbol, direction, signal_type,
                level_price, entry_price, stop_price, target_price,
                total_score, json.dumps(score_breakdown, ensure_ascii=False),
                rvol, delta_oi_pct, funding_rate, btc_trend
            ))
            await db.commit()

    async def update_alert_result(self, alert_id: int, result: str, pnl_pct: float, notes: str = ""):
        """Updates an alert with the trade result (WIN/LOSS/SKIP)."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE alerts SET result = ?, pnl_pct = ?, user_notes = ?
                WHERE id = ?
            """, (result, pnl_pct, notes, alert_id))
            await db.commit()

    # =========================================================================
    # CLEANUP — runs daily at 04:00 MSK (called from main loop)
    # =========================================================================

    async def cleanup_old_data(
        self,
        watchlist_keep_days: int = 30,
        candle_keep_days: int = 30,
        level_keep_days: int = 60,
        btc_ctx_keep_days: int = 30,
        alert_keep_days: int = 365   # Alerts stay 1 YEAR for AI analysis
    ):
        """
        Purges old data from heavy tables while preserving alert history
        for Gerald AI backtesting and self-calibration.
        
        NEVER deletes alerts aggressively — they are tiny rows and
        critical for performance analysis. 365 days minimum.
        """
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.utcnow()
            
            # 1. Watchlist history (heaviest table — grows 20 rows/10min)
            cutoff_wl = (now - timedelta(days=watchlist_keep_days)).isoformat()
            r1 = await db.execute("DELETE FROM watchlist_history WHERE timestamp < ?", (cutoff_wl,))
            
            # 2. Candle cache
            cutoff_cc = (now - timedelta(days=candle_keep_days)).isoformat()
            r2 = await db.execute("DELETE FROM candle_cache WHERE open_time < ?", (cutoff_cc,))
            
            # 3. Old detected levels (mark inactive, don't delete — useful for pattern analysis)
            cutoff_lv = (now - timedelta(days=level_keep_days)).isoformat()
            r3 = await db.execute("UPDATE detected_levels SET is_active = 0 WHERE timestamp < ? AND is_active = 1", (cutoff_lv,))
            
            # 4. BTC context snapshots
            cutoff_btc = (now - timedelta(days=btc_ctx_keep_days)).isoformat()
            r4 = await db.execute("DELETE FROM btc_context WHERE timestamp < ?", (cutoff_btc,))
            
            # 5. Very old alerts (keep 1 year by default)
            cutoff_al = (now - timedelta(days=alert_keep_days)).isoformat()
            r5 = await db.execute("DELETE FROM alerts WHERE timestamp < ?", (cutoff_al,))
            
            await db.execute("PRAGMA optimize")
            await db.commit()
            
            logger.info(
                f"🧹 DB Cleanup: watchlist={r1.rowcount}, candles={r2.rowcount}, "
                f"levels_deactivated={r3.rowcount}, btc_ctx={r4.rowcount}, old_alerts={r5.rowcount}"
            )

    # =========================================================================
    # ANALYTICS — for Gerald AI to analyze alert quality & performance
    # =========================================================================

    async def get_recent_alerts(self, days: int = 7, limit: int = 50) -> List[Dict[str, Any]]:
        """Returns recent alerts for AI review."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT * FROM alerts 
                WHERE timestamp > ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (cutoff, limit))
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_alert_stats(self, days: int = 30) -> Dict[str, Any]:
        """
        Aggregated alert statistics for Gerald AI self-calibration.
        Returns win rate, average score, symbol frequency, etc.
        """
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            # Total alerts
            cursor = await db.execute(
                "SELECT COUNT(*) FROM alerts WHERE timestamp > ?", (cutoff,)
            )
            total = (await cursor.fetchone())[0]
            
            # Results breakdown
            cursor = await db.execute("""
                SELECT result, COUNT(*) as cnt, AVG(pnl_pct) as avg_pnl
                FROM alerts 
                WHERE timestamp > ? AND result IS NOT NULL
                GROUP BY result
            """, (cutoff,))
            results = {row[0]: {"count": row[1], "avg_pnl": round(row[2] or 0, 2)} for row in await cursor.fetchall()}
            
            # Win rate
            wins = results.get("WIN", {}).get("count", 0)
            losses = results.get("LOSS", {}).get("count", 0)
            evaluated = wins + losses
            win_rate = round(wins / evaluated * 100, 1) if evaluated > 0 else None
            
            # Top symbols by alert frequency
            cursor = await db.execute("""
                SELECT symbol, COUNT(*) as cnt 
                FROM alerts WHERE timestamp > ?
                GROUP BY symbol ORDER BY cnt DESC LIMIT 10
            """, (cutoff,))
            top_symbols = {row[0]: row[1] for row in await cursor.fetchall()}
            
            # Average score
            cursor = await db.execute(
                "SELECT AVG(total_score) FROM alerts WHERE timestamp > ?", (cutoff,)
            )
            avg_score = round((await cursor.fetchone())[0] or 0, 1)
            
            # Score distribution
            cursor = await db.execute("""
                SELECT 
                    SUM(CASE WHEN total_score < 60 THEN 1 ELSE 0 END) as low,
                    SUM(CASE WHEN total_score BETWEEN 60 AND 74 THEN 1 ELSE 0 END) as normal,
                    SUM(CASE WHEN total_score BETWEEN 75 AND 84 THEN 1 ELSE 0 END) as important,
                    SUM(CASE WHEN total_score >= 85 THEN 1 ELSE 0 END) as critical
                FROM alerts WHERE timestamp > ?
            """, (cutoff,))
            dist = await cursor.fetchone()
        
        return {
            "period_days": days,
            "total_alerts": total,
            "evaluated_trades": evaluated,
            "win_rate_pct": win_rate,
            "results_breakdown": results,
            "avg_score": avg_score,
            "score_distribution": {
                "low (<60)": dist[0] or 0,
                "normal (60-74)": dist[1] or 0,
                "important (75-84)": dist[2] or 0,
                "critical (85+)": dist[3] or 0,
            },
            "top_symbols": top_symbols,
        }

    async def get_symbol_alert_history(self, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Gets alert history for a specific symbol — for AI fundamental/speculative cross-check."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT * FROM alerts
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (symbol, limit))
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_weekly_summary(self) -> str:
        """
        Generates a human-readable weekly summary for Telegram.
        Designed to be called by Gerald AI for the weekly report.
        """
        stats = await self.get_alert_stats(days=7)
        
        summary = "📊 <b>Еженедельный отчёт Gerald Sniper</b>\n\n"
        summary += f"📈 Всего алертов: <b>{stats['total_alerts']}</b>\n"
        summary += f"🎯 Средний Score: <b>{stats['avg_score']}/100</b>\n"
        
        if stats['win_rate_pct'] is not None:
            wr_emoji = "🟢" if stats['win_rate_pct'] >= 50 else "🔴"
            summary += f"{wr_emoji} Win Rate: <b>{stats['win_rate_pct']}%</b> ({stats['evaluated_trades']} сделок)\n"
        else:
            summary += "⚪ Win Rate: <i>нет оценённых сделок</i>\n"
            
        if stats['results_breakdown']:
            for result, data in stats['results_breakdown'].items():
                summary += f"  • {result}: {data['count']} (avg PnL: {data['avg_pnl']}%)\n"
        
        summary += f"\n🏆 Топ монеты: {', '.join(list(stats['top_symbols'].keys())[:5])}\n"
        
        dist = stats['score_distribution']
        summary += f"\n📊 Распределение скоров:\n"
        summary += f"  🟢 Critical (85+): {dist['critical (85+)']}\n"
        summary += f"  🟡 Important (75-84): {dist['important (75-84)']}\n"
        summary += f"  ⚪ Normal (60-74): {dist['normal (60-74)']}\n"
        
        summary += f"\n<i>Gerald Sniper 🤖 | Self-Calibration Report</i>"
        return summary

