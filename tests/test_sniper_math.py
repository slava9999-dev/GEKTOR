import sys
import os
import pytest
import math

# Inject paths specifically for sniper testing
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SNIPER_DIR = os.path.join(PROJECT_ROOT, "skills", "gerald-sniper")
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, SNIPER_DIR)

from core.scoring import calculate_final_score

class DummyRadar:
    def __init__(self, rvol=1.0, momentum=0.5):
        self.symbol = "BTCUSDT"
        self.rvol = rvol
        self.momentum_15m = momentum
        self.trend_4h = "NEUTRAL"
        self.delta_oi_4h_pct = 0.0
        self.funding_rate = 0.0

def test_calculate_final_score_kde_vs_round_number():
    """Test that a heavily corroborated KDE beats a ROUND_NUMBER due to logarithmic scoring."""
    radar = DummyRadar()
    
    # 25 touches KDE -> should max out touches_score
    kde_level = {
        'price': 100.0,
        'distance_pct': 1.0, # further away
        'touches': 25,
        'source': 'KDE',
        'type': 'SUPPORT'
    }
    
    # 7 touches ROUND_NUMBER -> closer to price but mathematically weaker
    round_level = {
        'price': 100.0,
        'distance_pct': 0.5, # closer
        'touches': 7,
        'source': 'ROUND_NUMBER',
        'type': 'SUPPORT'
    }
    
    trigger = {'score': 20, 'pattern': 'TEST'}
    btc_ctx = {'trend': 'NEUTRAL'}
    
    cfg = {
        'weights': {'level_quality': 30, 'fuel': 30, 'pattern': 25, 'macro': 15},
        'modifiers': {}
    }
    
    kde_score, kde_bd = calculate_final_score(radar, kde_level, trigger, btc_ctx, cfg)
    round_score, rnd_bd = calculate_final_score(radar, round_level, trigger, btc_ctx, cfg)
    
    assert kde_bd['level'] > rnd_bd['level'], "Logarithmic scale failed, 25 touch KDE lost to 7 touch Round Number"
    
def test_calculate_final_score_dynamic_source_bonus():
    """Test that KDE+WEEK_EXTREME gets bonus points."""
    radar = DummyRadar()
    
    level_combined = {
        'price': 100.0,
        'distance_pct': 2.0,
        'touches': 2,
        'source': 'KDE+WEEK_EXTREME',
        'type': 'RESISTANCE'
    }
    trigger = {'score': 20, 'pattern': 'TEST'}
    btc_ctx = {'trend': 'NEUTRAL'}
    cfg = {}
    
    combined_score, combined_bd = calculate_final_score(radar, level_combined, trigger, btc_ctx, cfg)
    
    # In scoring logic, touches=2 becomes effective_touches=5.0 for '+' sources, plus a 1.15 multiplier.
    assert combined_bd['level'] > 0
    # Level score should be heavily boosted
    assert combined_bd['level'] > 15

