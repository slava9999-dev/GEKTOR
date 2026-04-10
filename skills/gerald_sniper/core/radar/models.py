from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class RadarV2Metrics:
    symbol: str
    
    # Raw stats
    price: float
    volume_24h: float
    
    # Calculated relative metrics
    volume_spike: float  # vol_5m / avg_vol_1h
    velocity: float      # trades_1m / avg_trades_10m
    momentum_pct: float  # abs(price_change_5m)
    atr_ratio: float     # ATR_5m / ATR_1h
    orderflow_imbalance: float = 1.0 # aggressive_buy / aggressive_sell
    
    # Raw stats (with defaults)
    funding_rate: float = 0.0
    delta_oi_4h_pct: float = 0.0
    
    # Individual scores (0-40, 0-25, etc.)
    volume_spike_score: float = 0.0
    velocity_score: float = 0.0
    momentum_score: float = 0.0
    volatility_score: float = 0.0
    orderflow_score: float = 0.0
    
    # Final combined score (0-100)
    final_score: int = 0
    
    # Meta
    timestamp: float = field(default_factory=lambda: 0.0)
    liquidity_tier: str = "C" # A, B, C

# Legacy support for Radar v1 imports
@dataclass
class CoinRadarMetrics:
    symbol: str
    rvol: float = 1.0
    delta_oi_4h_pct: float = 0.0
    atr_pct: float = 1.5
    funding_rate: float = 0.0
    direction: str = 'NEUTRAL'
    volume_24h_usd: float = 0.0
    trade_count_24h: int = 0
    trade_velocity: float = 1.0
    btc_correlation: float = 0.0
    activity_ratio: float = 1.0
