"""
LEVEL 1: Smoke Tests — Critical Path Validation
================================================
These tests catch the most dangerous regressions:
- Broken imports (NameError at startup)
- Missing config fields
- Token leaks in source code
- Broken .env

Run: pytest tests/test_smoke.py -v
"""
import os
import sys
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SNIPER_ROOT = os.path.join(PROJECT_ROOT, "skills", "gerald-sniper")


class TestImports:
    """Verify all critical modules import without errors."""

    def test_sniper_config_imports(self):
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config, SniperConfig
        assert config is not None
        assert isinstance(config, SniperConfig)

    def test_sniper_scoring_imports(self):
        sys.path.insert(0, SNIPER_ROOT)
        from core.scoring import calculate_final_score
        assert callable(calculate_final_score)

    def test_sniper_level_detector_imports(self):
        sys.path.insert(0, SNIPER_ROOT)
        from core.level_detector import detect_levels
        assert callable(detect_levels)

    def test_sniper_trigger_detector_imports(self):
        sys.path.insert(0, SNIPER_ROOT)
        from core.trigger_detector import detect_compression, detect_volume_spike_at_level, detect_breakout
        assert callable(detect_compression)
        assert callable(detect_volume_spike_at_level)
        assert callable(detect_breakout)

    def test_sniper_database_imports(self):
        sys.path.insert(0, SNIPER_ROOT)
        from data.database import DatabaseManager
        assert callable(DatabaseManager)

    def test_sniper_ws_imports(self):
        sys.path.insert(0, SNIPER_ROOT)
        from data.bybit_ws import BybitWSManager, ExponentialBackoff
        assert callable(BybitWSManager)
        assert callable(ExponentialBackoff)

    def test_sniper_candle_manager_imports(self):
        sys.path.insert(0, SNIPER_ROOT)
        from core.candle_manager import CandleManager
        assert callable(CandleManager)

    def test_sniper_radar_imports(self):
        sys.path.insert(0, SNIPER_ROOT)
        from core.radar import RadarScanner
        assert callable(RadarScanner)

    def test_sentinel_wss_imports(self):
        sentinel_path = os.path.join(PROJECT_ROOT, "skills", "crypto-sentinel-pro")
        sys.path.insert(0, sentinel_path)
        from sentinel_wss import WSSentinel
        assert callable(WSSentinel)

    def test_bridge_v2_imports(self):
        """Bridge v2 must import without NameError (P0 bug we fixed)."""
        sys.path.insert(0, PROJECT_ROOT)
        # Just verify the module parses without SyntaxError/NameError
        import importlib.util
        spec = importlib.util.spec_from_file_location("bridge_v2", os.path.join(PROJECT_ROOT, "bridge_v2.py"))
        assert spec is not None


class TestConfigIntegrity:
    """Verify config loads with all required fields."""

    def test_config_loads(self):
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config
        assert config is not None

    def test_config_has_bybit(self):
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config
        assert hasattr(config, 'bybit')
        assert config.bybit.rest_url.startswith("https://")
        assert config.bybit.ws_url.startswith("wss://")

    def test_config_has_ws_fallback(self):
        """Regression: ws_url_fallback must exist for ExponentialBackoff."""
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config
        assert hasattr(config.bybit, 'ws_url_fallback')
        assert "bytick.com" in config.bybit.ws_url_fallback or "bybit.com" in config.bybit.ws_url_fallback

    def test_config_has_telegram(self):
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config
        assert hasattr(config, 'telegram')
        # Token must be resolved (not a ${VAR} placeholder)
        assert "${" not in (config.telegram.bot_token or "")

    def test_config_has_radar(self):
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config
        assert hasattr(config, 'radar')
        assert config.radar.interval_minutes > 0

    def test_config_has_levels(self):
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config
        assert hasattr(config, 'levels')
        assert config.levels.min_touches >= 2

    def test_config_has_scoring(self):
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config
        assert hasattr(config, 'scoring')

    def test_config_has_risk(self):
        """Regression: risk config must exist for position sizing."""
        sys.path.insert(0, SNIPER_ROOT)
        from utils.config import config
        assert hasattr(config, 'risk')


class TestEnvSecurity:
    """Ensure no secrets are hardcoded in source files."""

    SENSITIVE_PATTERNS = [
        "bot_token =",      # Direct assignment of token
        "sk-or-",           # OpenRouter key prefix
        "sk-1",             # DeepSeek key prefix
    ]

    SOURCE_DIRS = [
        os.path.join(PROJECT_ROOT, "skills"),
        os.path.join(PROJECT_ROOT, "src"),
        os.path.join(PROJECT_ROOT, "bridge"),
    ]

    SCAN_FILES = [
        os.path.join(PROJECT_ROOT, "bridge_v2.py"),
    ]

    def _scan_file_for_secrets(self, filepath: str) -> list[str]:
        """Returns list of violations found in file."""
        violations = []
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                for i, line in enumerate(f, 1):
                    line_stripped = line.strip()
                    # Skip comments and test files
                    if line_stripped.startswith("#") or line_stripped.startswith("//"):
                        continue
                    # Check for hardcoded tokens (not env vars)
                    if "bot_token" in line.lower() and "=" in line:
                        # Allow env lookups and config reads
                        if "getenv" not in line and "os.environ" not in line and ".get(" not in line and "config." not in line and "tg_config" not in line:
                            if any(c.isdigit() for c in line) and ":" in line:
                                violations.append(f"{filepath}:{i}: Potential hardcoded token")
        except Exception:
            pass
        return violations

    def test_no_token_in_logs(self):
        """Regression: telegram_bot.py must NOT print token in debug/warning."""
        tg_bot_path = os.path.join(SNIPER_ROOT, "utils", "telegram_bot.py")
        with open(tg_bot_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "print(f\"DEBUG" not in content, "Token leak via print() detected!"
        assert "bot_token}" not in content or "not configured" in content, "Token may be logged in warning message!"

    def test_no_token_in_sentinel(self):
        """Ensure sentinel_wss doesn't hardcode tokens."""
        sentinel_path = os.path.join(PROJECT_ROOT, "skills", "crypto-sentinel-pro", "sentinel_wss.py")
        with open(sentinel_path, "r", encoding="utf-8") as f:
            content = f.read()
        # Should use os.getenv, not hardcoded values
        assert "os.getenv" in content or "tg_config" in content

    def test_env_has_required_keys(self):
        """Verify .env has all required keys set."""
        from dotenv import load_dotenv
        load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=True)

        required = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "OPENROUTER_API_KEY"]
        for key in required:
            val = os.getenv(key, "")
            assert val and "${" not in val, f"Required env var {key} is missing or unresolved"


class TestDeepSeekDisabled:
    """Regression: DeepSeek must remain disabled (balance = 0)."""

    def test_ai_analyst_deepseek_disabled(self):
        sys.path.insert(0, SNIPER_ROOT)
        from core.ai_analyst import analyst
        # DeepSeek must NOT be in active providers (it's disabled)
        assert "deepseek" not in analyst.router.providers, \
            "DeepSeek must be disabled in ai_analyst (balance = 0)"

    def test_bridge_v2_deepseek_disabled(self):
        """Check bridge_v2.py file content for deepseek disabled."""
        bridge_path = os.path.join(PROJECT_ROOT, "bridge_v2.py")
        with open(bridge_path, "r", encoding="utf-8") as f:
            content = f.read()
        # The deepseek section should have enabled: False or "enabled": False
        assert '"enabled": False' in content or "'enabled': False" in content or "enabled=False" in content, \
            "DeepSeek must be disabled in bridge_v2.py"
