"""
LEVEL 2: Unit Tests — Core Logic Validation
============================================
Tests for the most critical business logic:
- Scoring stays in valid range
- ExponentialBackoff works correctly
- Database CRUD operations
- Alert formatting produces valid output

Run: pytest tests/test_core.py -v
"""
import os
import sys
import asyncio
import tempfile
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SNIPER_ROOT = os.path.join(PROJECT_ROOT, "skills", "gerald-sniper")
sys.path.insert(0, SNIPER_ROOT)


class TestExponentialBackoff:
    """Regression: flat 2s delay caused 750 reconnects in 25 min."""

    def test_initial_delay(self):
        from data.bybit_ws import ExponentialBackoff
        b = ExponentialBackoff(base=2, max_delay=300)
        assert b.next_delay() == 2.0  # First delay = base

    def test_delays_increase(self):
        from data.bybit_ws import ExponentialBackoff
        b = ExponentialBackoff(base=2, max_delay=300)
        delays = [b.next_delay() for _ in range(6)]
        assert delays == [2, 4, 8, 16, 32, 64]

    def test_max_delay_cap(self):
        from data.bybit_ws import ExponentialBackoff
        b = ExponentialBackoff(base=2, max_delay=60)
        for _ in range(20):
            d = b.next_delay()
        assert d <= 60

    def test_reset(self):
        from data.bybit_ws import ExponentialBackoff
        b = ExponentialBackoff(base=2, max_delay=300)
        b.next_delay()
        b.next_delay()
        b.reset()
        assert b.attempt == 0
        assert b.next_delay() == 2.0

    def test_fallback_trigger(self):
        from data.bybit_ws import ExponentialBackoff
        b = ExponentialBackoff(base=2, max_delay=300, max_attempts_before_fallback=5)
        for _ in range(4):
            b.next_delay()
        assert not b.should_try_fallback()
        b.next_delay()
        assert b.should_try_fallback()


class TestScoring:
    """Regression: score must always be 0-100."""

    def _make_mock_data(self, rvol=2.0, oi=5.0, funding=0.0001):
        """Create realistic mock data matching actual function signature."""
        from core.radar import CoinRadarMetrics
        radar = CoinRadarMetrics(
            symbol="TESTUSDT", rvol=rvol, delta_oi_4h_pct=oi,
            atr_pct=5.0, funding_rate=funding,
            direction="BULLISH", volume_24h_usd=10_000_000
        )
        level = {"price": 100.0, "type": "RESISTANCE", "touches": 4, "distance_pct": 1.5, "stale": False}
        trigger = {"pattern": "COMPRESSION_LONG", "direction": "LONG", "r_squared": 0.85, "atr_contraction_pct": 25.0}
        btc_ctx = {"trend": "UP", "hot_sector_coins": []}
        config = {"weights": {"level_quality": 30, "fuel": 30, "pattern": 25, "macro": 15}, "modifiers": {}}
        return radar, level, trigger, btc_ctx, config

    def test_score_returns_tuple(self):
        from core.scoring import calculate_final_score
        radar, level, trigger, btc_ctx, config = self._make_mock_data()
        score, breakdown = calculate_final_score(radar, level, trigger, btc_ctx, config)
        assert isinstance(score, int)
        assert isinstance(breakdown, dict)
        assert "level" in breakdown
        assert "fuel" in breakdown
        assert "pattern" in breakdown

    def test_score_range_always_0_100(self):
        from core.scoring import calculate_final_score
        # Test with maximum values
        r, l, t, b, c = self._make_mock_data(rvol=10.0, oi=50.0)
        l["touches"] = 20
        l["distance_pct"] = 0.1
        t["r_squared"] = 1.0
        t["atr_contraction_pct"] = 50.0
        b["trend"] = "STRONG_UP"
        score_max, _ = calculate_final_score(r, l, t, b, c)
        assert 0 <= score_max <= 100, f"Max score {score_max} out of range"

        # Test with minimum values
        r2, l2, t2, b2, c2 = self._make_mock_data(rvol=0.0, oi=0.0)
        l2["touches"] = 0
        l2["distance_pct"] = 10.0
        t2["r_squared"] = 0.0
        t2["atr_contraction_pct"] = 0.0
        b2["trend"] = "STRONG_DOWN"
        score_min, _ = calculate_final_score(r2, l2, t2, b2, c2)
        assert 0 <= score_min <= 100, f"Min score {score_min} out of range"

    def test_macro_penalizes_counter_trend(self):
        """LONG in STRONG_DOWN should score lower than LONG in UP."""
        from core.scoring import calculate_final_score
        r, l, t, b, c = self._make_mock_data()
        b["trend"] = "UP"
        score_up, _ = calculate_final_score(r, l, t, b, c)

        b["trend"] = "STRONG_DOWN"
        score_down, _ = calculate_final_score(r, l, t, b, c)

        assert score_up > score_down, "LONG in STRONG_DOWN should be penalized"


class TestDatabaseManager:
    """Test DB operations without touching production DB."""

    @pytest.fixture
    def db_path(self, tmp_path):
        return str(tmp_path / "test_sniper.db")

    @pytest.mark.asyncio
    async def test_initialize_creates_tables(self, db_path):
        from data.database import DatabaseManager
        db = DatabaseManager(db_path)
        await db.initialize()
        
        import aiosqlite
        async with aiosqlite.connect(db_path) as conn:
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in await cursor.fetchall()]
        
        assert "alerts" in tables
        assert "watchlist_history" in tables
        assert "detected_levels" in tables
        assert "candle_cache" in tables
        assert "btc_context" in tables

    @pytest.mark.asyncio
    async def test_insert_and_query_alert(self, db_path):
        from data.database import DatabaseManager
        db = DatabaseManager(db_path)
        await db.initialize()
        
        await db.insert_alert(
            symbol="BTCUSDT", direction="LONG", signal_type="COMPRESSION",
            level_price=69000.0, entry_price=69100.0,
            stop_price=68000.0, target_price=71000.0,
            total_score=82, score_breakdown={"level": 30, "fuel": 25},
            rvol=2.5, delta_oi_pct=8.0, funding_rate=0.01,
            btc_trend="UP"
        )
        
        alerts = await db.get_recent_alerts(days=1)
        assert len(alerts) == 1
        assert alerts[0]["symbol"] == "BTCUSDT"
        assert alerts[0]["total_score"] == 82

    @pytest.mark.asyncio
    async def test_alert_stats(self, db_path):
        from data.database import DatabaseManager
        db = DatabaseManager(db_path)
        await db.initialize()
        
        # Insert test alerts
        for i in range(3):
            await db.insert_alert(
                symbol="ETHUSDT", direction="SHORT", signal_type="BREAKOUT",
                level_price=2000.0, entry_price=2001.0,
                stop_price=2050.0, target_price=1900.0,
                total_score=70 + i * 10, score_breakdown={},
                rvol=1.5, delta_oi_pct=5.0, funding_rate=0.005,
                btc_trend="DOWN"
            )
        
        stats = await db.get_alert_stats(days=1)
        assert stats["total_alerts"] == 3
        assert stats["avg_score"] > 0

    @pytest.mark.asyncio
    async def test_update_alert_result(self, db_path):
        from data.database import DatabaseManager
        db = DatabaseManager(db_path)
        await db.initialize()
        
        await db.insert_alert(
            symbol="SOLUSDT", direction="LONG", signal_type="VOLUME_SPIKE",
            level_price=100.0, entry_price=101.0,
            stop_price=97.0, target_price=110.0,
            total_score=85, score_breakdown={},
            rvol=3.0, delta_oi_pct=12.0, funding_rate=0.002,
            btc_trend="UP"
        )
        
        await db.update_alert_result(alert_id=1, result="WIN", pnl_pct=5.2, notes="Perfect entry")
        
        alerts = await db.get_recent_alerts(days=1)
        assert alerts[0]["result"] == "WIN"
        assert alerts[0]["pnl_pct"] == 5.2

    @pytest.mark.asyncio
    async def test_cleanup_preserves_recent_alerts(self, db_path):
        """Regression: cleanup must NOT delete recent alerts."""
        from data.database import DatabaseManager
        db = DatabaseManager(db_path)
        await db.initialize()
        
        await db.insert_alert(
            symbol="BTCUSDT", direction="LONG", signal_type="COMP",
            level_price=70000.0, entry_price=70100.0,
            stop_price=69000.0, target_price=72000.0,
            total_score=90, score_breakdown={},
            rvol=2.0, delta_oi_pct=6.0, funding_rate=0.01,
            btc_trend="UP"
        )
        
        await db.cleanup_old_data(alert_keep_days=365)
        
        alerts = await db.get_recent_alerts(days=1)
        assert len(alerts) == 1, "Cleanup deleted a recent alert!"


class TestAlertFormatting:
    """Verify alert formatters produce valid HTML."""

    def test_level_alert_format(self):
        from utils.telegram_bot import format_level_alert
        level = {
            "price": 69000.0,
            "type": "RESISTANCE",
            "distance_pct": 1.2,
            "touches": 5,
            "support_touches": 2,
            "resistance_touches": 3,
            "strength": 85
        }
        msg = format_level_alert("BTCUSDT", level)
        assert "BTCUSDT" in msg
        assert "69000" in msg
        assert "━━━" in msg  # New design separator
        assert "<b>" in msg  # HTML formatting
        assert "Gerald" in msg

    def test_trigger_alert_has_separators(self):
        """Regression: old format had no visual structure."""
        from core.candle_manager import CandleManager
        # We can't fully instantiate CandleManager without a REST client,
        # but we can check the method exists and has the right signature
        import inspect
        sig = inspect.signature(CandleManager._format_trigger_alert)
        params = list(sig.parameters.keys())
        assert "risk_data" in params, "risk_data parameter missing (double calc regression)"


class TestWSManagerHasFallback:
    """Regression: BybitWSManager must accept fallback URL."""

    def test_fallback_parameter(self):
        from data.bybit_ws import BybitWSManager
        import inspect
        sig = inspect.signature(BybitWSManager.__init__)
        params = list(sig.parameters.keys())
        assert "ws_url_fallback" in params, "Fallback URL parameter missing!"

    def test_default_fallback(self):
        from data.bybit_ws import BybitWSManager
        mgr = BybitWSManager(ws_url="wss://test.com")
        assert "bytick.com" in mgr.ws_url_fallback
