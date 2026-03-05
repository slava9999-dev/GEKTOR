import sys
import yaml
import os
# Define a robust mechanism to load the .env file from the system root
try:
    from dotenv import load_dotenv
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    target_env = os.path.join(project_root, ".env")
    if os.path.exists(target_env):
        load_dotenv(target_env, override=True)
except Exception:
    pass
load_dotenv(override=True)

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

class BybitConfig(BaseModel):
    rest_url: str = "https://api.bybit.com"
    rest_url_fallback: str = "https://api.bytick.com"
    ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    ws_url_fallback: str = "wss://stream.bytick.com/v5/public/linear"
    fallback_switch_after_failures: int = 3
    fallback_primary_recheck_sec: int = 600
    ws_ping_interval_sec: int = 18
    ws_reconnect_delay_sec: int = 3
    ws_max_reconnect_attempts: int = 10
    ws_max_subscriptions_per_conn: int = 190

class TelegramConfig(BaseModel):
    bot_token: str = ""
    chat_id: str = ""
    max_sniper_alerts_per_day: int = 15
    max_setup_notifications_per_day: int = 30
    message_queue_size: int = 50

class MacroFilterConfig(BaseModel):
    enabled: bool = True
    btc_symbol: str = "BTCUSDT"
    btc_trend_timeframe: str = "60"
    block_alt_longs_if_btc_drop_4h_pct: float = 3.0
    block_alt_shorts_if_btc_pump_4h_pct: float = 3.0
    btc_exempt_from_macro: bool = True
    market_funding_warning_threshold: float = 0.0008

class RadarConfig(BaseModel):
    interval_minutes: int = 10
    min_volume_24h_usd: float = 5000000.0
    watchlist_size: int = 20
    rvol_min_large_caps: float = 1.5
    rvol_min_altcoins: float = 2.0
    rvol_lookback_days: int = 14
    delta_oi_min_pct: float = 5.0
    delta_oi_lookback_hours: int = 4
    atr_min_pct: float = 2.5
    atr_period: int = 14
    funding_penalty_threshold: float = 0.0005
    funding_bonus_threshold: float = -0.0001
    large_caps: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT"]
    blacklist: list[str] = ["LUNAUSDT", "USTCUSDT", "FTTUSDT"]
    min_filters_passed: int = 2

class LevelsConfig(BaseModel):
    enable_dynamic_levels: bool = True
    dynamic_level_max_strength: int = 75
    adaptive_params_enabled: bool = True
    candles_lookback: int = 168
    recalculate_interval_minutes: int = 60
    timeframe: str = "60"
    swing_order: int = 5
    kde_bandwidth: float = 0.015
    kde_bandwidth_adaptive: bool = True
    kde_bandwidth_min: float = 0.008
    kde_bandwidth_max: float = 0.025
    min_touches: int = 3
    touch_tolerance_pct: float = 0.003
    touch_tolerance_adaptive: bool = True
    touch_tolerance_high_price: float = 0.002
    touch_tolerance_low_price: float = 0.005
    price_threshold_high: float = 1000.0
    price_threshold_low: float = 0.1
    max_distance_pct: float = 4.0
    max_levels_per_coin: int = 6
    freshness_enabled: bool = True
    freshness_decay_hours: int = 72
    freshness_penalty_pct: int = 20

class CompressionConfig(BaseModel):
    timeframe: str = "15"
    lookback_candles: int = 20
    min_r_squared: float = 0.35
    max_distance_to_level_pct: float = 1.5
    min_atr_contraction_pct: float = 12.0
    anti_false_compression: bool = True
    anti_false_lookback_candles: int = 10

class VolumeSpikeConfig(BaseModel):
    timeframe: str = "5"
    min_volume_ratio: float = 2.0
    max_distance_to_level_pct: float = 1.0
    avg_volume_lookback: int = 50
    min_body_ratio: float = 0.4

class SqueezeConfig(BaseModel):
    enabled: bool = True
    min_candles: int = 25
    bb_length: int = 20
    bb_std: float = 2.0
    kc_length: int = 20
    kc_scalar: float = 1.5
    min_squeeze_bars: int = 4
    max_distance_to_level_pct: float = 3.0
    vol_lookback: int = 20
    min_vol_ratio_on_fire: float = 0.8

class VolumeExplosionConfig(BaseModel):
    enabled: bool = True
    min_volume_ratio: float = 2.0
    max_distance_to_level_pct: float = 1.0
    avg_volume_lookback: int = 50
    min_body_ratio: float = 0.4

class LiquidationConfig(BaseModel):
    window_seconds: int = 300
    min_cascade_count: int = 5
    min_cascade_volume_usd: float = 100000.0

class BreakoutConfig(BaseModel):
    enabled: bool = True
    timeframe: str = "5"
    close_beyond_level: bool = True
    min_breakout_volume_ratio: float = 1.5
    min_body_ratio: float = 0.4

class TriggersConfig(BaseModel):
    compression: CompressionConfig = Field(default_factory=CompressionConfig)
    volume_spike: VolumeSpikeConfig = Field(default_factory=VolumeSpikeConfig)
    squeeze: SqueezeConfig = Field(default_factory=SqueezeConfig)
    volume_explosion: VolumeExplosionConfig = Field(default_factory=VolumeExplosionConfig)
    liquidation: LiquidationConfig = Field(default_factory=LiquidationConfig)
    breakout: BreakoutConfig = Field(default_factory=BreakoutConfig)

class ScoringWeightsConfig(BaseModel):
    level_quality: int = 30
    fuel: int = 30
    pattern: int = 25
    macro: int = 15

class ScoringModifiersConfig(BaseModel):
    sector_momentum_bonus: int = 5
    stale_level_penalty: int = -10
    liquidation_cascade_bonus: int = 10
    adverse_funding_penalty: int = -8
    favorable_funding_bonus: int = 5

class ScoringConfig(BaseModel):
    min_score_for_alert: int = 65
    min_score_for_setup_notify: int = 50
    weights: ScoringWeightsConfig = Field(default_factory=ScoringWeightsConfig)
    modifiers: ScoringModifiersConfig = Field(default_factory=ScoringModifiersConfig)

class PriorityTierConfig(BaseModel):
    min_score: int
    max_score: int
    telegram_silent: bool
    repeat_if_unread_minutes: int | None = None

class PriorityTiersConfig(BaseModel):
    normal: PriorityTierConfig = Field(default_factory=lambda: PriorityTierConfig(min_score=65, max_score=74, telegram_silent=True))
    important: PriorityTierConfig = Field(default_factory=lambda: PriorityTierConfig(min_score=75, max_score=84, telegram_silent=False))
    critical: PriorityTierConfig = Field(default_factory=lambda: PriorityTierConfig(min_score=85, max_score=100, telegram_silent=False, repeat_if_unread_minutes=2))

class AlertsConfig(BaseModel):
    cooldown_hours: int = 4
    quiet_hours_enabled: bool = True
    quiet_hours: list[str] = ["02:00", "07:00"]
    morning_digest_enabled: bool = True
    morning_digest_time: str = "07:05"
    attach_chart: bool = True
    chart_timeframe: str = "15"
    chart_candles: int = 100
    show_risk_reward: bool = True
    priority_tiers: PriorityTiersConfig = Field(default_factory=PriorityTiersConfig)

class RiskConfig(BaseModel):
    atr_stop_multiplier: float = 0.5
    default_rr_ratio: float = 2.0
    max_stop_pct: float = 3.0
    position_sizing_enabled: bool = True
    risk_per_trade_usd: float = 20.0

class SectorsConfig(BaseModel):
    enabled: bool = True
    min_coins_for_sector_momentum: int = 3
    definitions: dict[str, list[str]] = Field(default_factory=lambda: {
        "AI": ["FETUSDT", "RENDERUSDT", "TAOUSDT", "NEARUSDT", "GRTUSDT"],
        "L1": ["SOLUSDT", "AVAXUSDT", "SUIUSDT", "APTUSDT", "SEIUSDT"],
        "L2": ["ARBUSDT", "OPUSDT", "STRKUSDT", "MANTAUSDT"],
        "DEFI": ["AABORUSDT", "UNIUSDT", "MKRUSDT", "AAVEUSDT"],
        "MEME": ["DOGEUSDT", "PEPEUSDT", "WIFUSDT", "BONKUSDT", "FLOKIUSDT"],
        "GAMING": ["IMXUSDT", "AXSUSDT", "GALAUSDT", "PIXELUSDT"]
    })

class StorageConfig(BaseModel):
    db_path: str = "./data_run/sniper.db"
    candle_cache_max_days: int = 30
    alert_history_max_days: int = 180
    cleanup_hour: int = 4

class LoggingConfig(BaseModel):
    level: str = "INFO"
    file: str = "./logs/sniper.log"
    rotation: str = "10 MB"
    retention: str = "14 days"

class StatisticsConfig(BaseModel):
    enabled: bool = True
    weekly_report_enabled: bool = True
    weekly_report_day: str = "sunday"
    weekly_report_time: str = "20:00"
    auto_calibration_suggest: bool = True
    min_acceptable_winrate: float = 0.40
    min_alerts_per_month: int = 10

class SniperConfig(BaseSettings):
    bybit: BybitConfig = Field(default_factory=BybitConfig)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    macro_filter: MacroFilterConfig = Field(default_factory=MacroFilterConfig)
    radar: RadarConfig = Field(default_factory=RadarConfig)
    levels: LevelsConfig = Field(default_factory=LevelsConfig)
    triggers: TriggersConfig = Field(default_factory=TriggersConfig)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    sectors: SectorsConfig = Field(default_factory=SectorsConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    statistics: StatisticsConfig = Field(default_factory=StatisticsConfig)
    timezone: str = "Europe/Moscow"
    
    @classmethod
    def load(cls, config_path: str = "config_sniper.yaml") -> "SniperConfig":
        if not os.path.exists(config_path):
            return cls()
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            
        # Hard override for Telegram tokens from the environment variables, prioritizing OS env
        if "telegram" not in data:
            data["telegram"] = {}
            
        bot_token = os.environ.get("GERALD_BOT_TOKEN") or data["telegram"].get("bot_token", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")  or data["telegram"].get("chat_id", "")
        
        # Clean placeholders if they weren't resolved
        if "${" in bot_token: bot_token = ""
        if "${" in chat_id: chat_id = ""
        
        data["telegram"]["bot_token"] = bot_token
        data["telegram"]["chat_id"] = chat_id
                
        return cls(**data)

try:
    # Attempt to load config assuming running from sniper root or project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config_sniper.yaml")
    config = SniperConfig.load(config_path)
except Exception as e:
    print(f"FAILED TO LOAD CONFIG: {e}")
    config = SniperConfig()
