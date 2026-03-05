import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../skills/gerald-sniper')))

from core.level_detector import detect_dynamic_levels, detect_all_levels


def _generate_candles(num_candles: int, low_base=95.0, high_base=105.0) -> list[dict]:
    candles = []
    base_time = 1600000000
    for i in range(num_candles):
        candles.append({
            'open_time': str(base_time + int(i * 3600)),
            'open': 100.0,
            'high': high_base + (i % 3),
            'low': low_base - (i % 3),
            'close': 100.5,
            'volume': 1000
        })
    return candles


class TestDynamicLevels:
    def test_week_extremes_detected(self):
        # 24H * 7 = 168 candles
        candles_h1 = _generate_candles(168, low_base=90.0, high_base=110.0)
        # Force a specific extreme
        candles_h1[50]['high'] = 120.0
        candles_h1[100]['low'] = 80.0
        
        current_price = 100.0
        
        config = {
            'enable_dynamic_levels': True,
            'max_distance_pct': 30.0, # large enough to catch 120 and 80 from 100
        }
        
        levels = detect_dynamic_levels(candles_h1, current_price, config)
        
        assert len(levels) > 0
        extreme_res = [l for l in levels if l['type'] == 'RESISTANCE' and l['source'] == 'WEEK_EXTREME']
        extreme_sup = [l for l in levels if l['type'] == 'SUPPORT' and l['source'] == 'WEEK_EXTREME']
        
        assert len(extreme_res) == 1
        assert extreme_res[0]['price'] == 120.0
        
        assert len(extreme_sup) == 1
        assert extreme_sup[0]['price'] == 80.0

    def test_round_numbers_for_high_price_coin(self):
        candles_h1 = _generate_candles(10)
        current_price = 65000.0 # BTC example
        config = {'enable_dynamic_levels': True, 'max_distance_pct': 5.0}
        
        levels = detect_dynamic_levels(candles_h1, current_price, config)
        round_levels = [l for l in levels if l['source'] == 'ROUND_NUMBER']
        
        assert len(round_levels) > 0
        prices = [l['price'] for l in round_levels]
        # Should detect steps like 1000, 500, 250 -> e.g. 65000, 66000, 64000
        assert 65000.0 in prices or 66000.0 in prices or 64000.0 in prices

    def test_distance_filter_rejects_far_levels(self):
        candles_h1 = _generate_candles(10)
        candles_h1[0]['high'] = 200.0 # Extremely far week high
        current_price = 100.0
        
        # very tight filter
        config = {'enable_dynamic_levels': True, 'max_distance_pct': 1.0}
        levels = detect_dynamic_levels(candles_h1, current_price, config)
        
        # 200 is 100% away, should be rejected by week_max_distance (which is max_distance_pct * 2.0 = 2.0%)
        far_res = [l for l in levels if l['price'] == 200.0]
        assert len(far_res) == 0


class TestKDELevels:
    def test_detect_all_levels_combines_kde_and_dynamic(self):
        # We need realistic swing data for KDE to find clusters
        candles_h1 = _generate_candles(200, low_base=85.0, high_base=95.0)
        
        # Create a KDE cluster with pronounced extremes, spaced out > swing_order=5
        for i in range(20, 100, 15):
            candles_h1[i]['high'] = 105.1
            candles_h1[i]['low'] = 80.0 # Wide candle
            # ensure it's a peak by making neighbors lower
            candles_h1[i-1]['high'] = 90.0
            candles_h1[i+1]['high'] = 90.0

        current_price = 100.0
        config = {
            'enable_dynamic_levels': True,
            'max_distance_pct': 10.0,
            'min_touches': 1, # Relax touches for test
            'max_levels_per_coin': 10
        }
        
        levels = detect_all_levels(candles_h1, current_price, config)
        
        assert len(levels) > 0
        sources = [l['source'] for l in levels]
        
        assert any('KDE' in s for s in sources)
        assert any('DYNAMIC' in s or 'WEEK_EXTREME' in s or 'ROUND_NUMBER' in s for s in sources)
