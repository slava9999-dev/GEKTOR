import pytest
import numpy as np
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../skills/gerald-sniper')))

from core.trigger_detector import (
    detect_squeeze_fire, detect_volume_explosion,
    detect_breakout, detect_compression
)


# --- Fixtures ---
@pytest.fixture
def base_level():
    return {
        'price': 100.0,
        'type': 'RESISTANCE',
        'touches': 5,
        'strength': 70.0,
        'source': 'KDE'
    }

@pytest.fixture
def support_level():
    return {
        'price': 50.0,
        'type': 'SUPPORT',
        'touches': 5,
        'strength': 70.0,
        'source': 'KDE'
    }

def _generate_candles(num_candles: int, open_val=100.0, high_val=105.0, low_val=95.0, close_val=102.0, vol_val=1000) -> list[dict]:
    candles = []
    base_time = 1600000000
    for i in range(num_candles):
        candles.append({
            'open_time': str(base_time + int(i * 300)),
            'open': open_val,
            'high': high_val,
            'low': low_val,
            'close': close_val,
            'volume': vol_val
        })
    return candles


class TestSqueezeFireDetector:
    def test_squeeze_fails_insufficient_candles(self):
        candles = _generate_candles(10) # default min is 25
        res = detect_squeeze_fire(candles)
        assert res is None

    def test_squeeze_fires_on_valid_setup(self):
        # We need a long enough setup to trigger a squeeze and then fire
        candles = _generate_candles(40, open_val=100, high_val=101, low_val=99, close_val=100, vol_val=1000)
        
        # Create a squeeze by forcing the last N-1 candles to be tight
        for i in range(5, 39):
            candles[i]['high'] = 100.2
            candles[i]['low'] = 99.8
            candles[i]['close'] = 100.1
            
        # Fire! High volume, strong close away from SMA
        candles[39]['open'] = 100.1
        candles[39]['high'] = 105.0
        candles[39]['low'] = 100.1
        candles[39]['close'] = 104.5
        candles[39]['volume'] = 5000
        
        # Test
        res = detect_squeeze_fire(candles, config={'min_squeeze_bars': 4})
        
        assert res is not None
        assert res['direction'] == 'LONG'
        assert res['squeeze_bars'] >= 4


    def test_squeeze_rejected_no_momentum(self):
        candles = _generate_candles(40, open_val=100, high_val=101, low_val=99, close_val=100, vol_val=1000)
        
        for i in range(5, 39):
            candles[i]['high'] = 100.2
            candles[i]['low'] = 99.8
            candles[i]['close'] = 100.1
            
        # Fire with no momentum (just another tight candle)
        candles[39]['open'] = 100.1
        candles[39]['high'] = 100.2
        candles[39]['low'] = 99.8
        candles[39]['close'] = 100.1
        
        res = detect_squeeze_fire(candles)
        assert res is None

    def test_squeeze_rejected_weak_volume(self):
        candles = _generate_candles(40, open_val=100, high_val=101, low_val=99, close_val=100, vol_val=1000)
        
        for i in range(5, 39):
            candles[i]['high'] = 100.2
            candles[i]['low'] = 99.8
            candles[i]['close'] = 100.1
            
        # Fire! But with terrible volume
        candles[39]['close'] = 104.5
        candles[39]['volume'] = 100 # Super low volume comparing to 1000
        
        res = detect_squeeze_fire(candles)
        assert res is None


class TestVolumeExplosion:
    def test_bullish_explosion_near_resistance(self, base_level):
        candles = _generate_candles(60, close_val=95, vol_val=1000)
        
        # Explode near resistance
        candles[-1] = {
            'open_time': '123', 'open': 95.0, 'high': 100.0, 'low': 94.0, 'close': 99.5, 'volume': 5000
        }
        res = detect_volume_explosion(candles, base_level, {'min_volume_ratio': 2.0})
        
        assert res is not None
        assert res['direction'] == 'LONG'
        assert res['volume_ratio'] >= 2.0

    def test_rejected_doji_candle(self, base_level):
        candles = _generate_candles(60, close_val=95, vol_val=1000)
        
        # High volume but doji (open ~= close)
        candles[-1] = {
            'open_time': '123', 'open': 99.0, 'high': 105.0, 'low': 94.0, 'close': 99.1, 'volume': 5000
        }
        res = detect_volume_explosion(candles, base_level, {'min_body_ratio': 0.4})
        
        assert res is None

    def test_rejected_too_far_from_level(self, base_level):
        candles = _generate_candles(60, close_val=95, vol_val=1000)
        
        # Explode FAR AWAY from resistance (price = 80, level = 100)
        candles[-1] = {
            'open_time': '123', 'open': 75.0, 'high': 81.0, 'low': 74.0, 'close': 80.0, 'volume': 5000
        }
        res = detect_volume_explosion(candles, base_level, {'max_distance_to_level_pct': 1.0})
        
        assert res is None


class TestBreakout:
    def test_fresh_breakout_above_resistance(self, base_level):
        candles = _generate_candles(60, close_val=95, vol_val=1000)
        
        # Prev close below
        candles[-2]['close'] = 99.0
        
        # Cur close ABOVE resistance (100.0) with good volume
        candles[-1] = {
            'open_time': '123', 'open': 99.0, 'high': 102.0, 'low': 98.0, 'close': 101.5, 'volume': 3000
        }
        
        res = detect_breakout(candles, base_level, {'min_body_ratio': 0.4})
        
        assert res is not None
        assert res['direction'] == 'LONG'

    def test_rejected_continuation_not_fresh(self, base_level):
        candles = _generate_candles(60, close_val=95, vol_val=1000)
        
        # Prev close already above
        candles[-2]['close'] = 101.0
        
        # Cur close even higher
        candles[-1] = {
            'open_time': '123', 'open': 101.0, 'high': 105.0, 'low': 100.0, 'close': 104.5, 'volume': 3000
        }
        
        res = detect_breakout(candles, base_level, {})
        assert res is None


class TestCompression:
    def test_valid_compression_toward_resistance(self, base_level):
        candles = _generate_candles(30, vol_val=1000)
        
        # Simulate linearly rising lows towards 100.0
        for i in range(30):
            candles[i]['low'] = 95.0 + (i * 0.1) # 95 -> 98
            candles[i]['high'] = 99.0 # Flat highs
            candles[i]['close'] = 98.0 + (i * 0.05) # ~99.5
        
        # Set large ATR at start to simulate contraction
        candles[0]['high'] = 105.0
        candles[0]['low'] = 85.0
            
        res = detect_compression(candles, base_level, {
            'min_r_squared': 0.35,
            'lookback_candles': 20,
            'min_atr_contraction_pct': 10.0,
            'anti_false_compression': False
        })
        
        assert res is not None
        assert res['direction'] == 'LONG'
        assert res['atr_contraction_pct'] > 10.0

    def test_rejected_insufficient_contraction(self, base_level):
        candles = _generate_candles(30, vol_val=1000)
        
        # Same ATR the whole time
        for i in range(30):
            candles[i]['low'] = 95.0 + (i * 0.1)
            candles[i]['high'] = candles[i]['low'] + 2.0
            candles[i]['close'] = candles[i]['low'] + 1.0
            
        res = detect_compression(candles, base_level, {'min_atr_contraction_pct': 15.0, 'anti_false_compression': False})
        assert res is None
