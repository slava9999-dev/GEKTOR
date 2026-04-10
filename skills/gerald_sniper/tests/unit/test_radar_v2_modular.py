import unittest
import sys
import os

# Add skill root to path
skill_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if skill_root not in sys.path:
    sys.path.append(skill_root)

from core.radar.scoring import compute_final_radar_score
from core.radar.volume_spike import score_volume_spike
from core.radar.trade_velocity import score_trade_velocity
from core.radar.momentum import score_momentum
from core.radar.volatility import score_volatility_expansion

class TestRadarV2Module(unittest.TestCase):
    def test_modular_scoring(self):
        # Test individual module scores
        self.assertEqual(score_volume_spike(5.5), 40.0)
        self.assertEqual(score_trade_velocity(3.5), 25.0)
        self.assertEqual(score_momentum(2.5), 15.0)
        self.assertEqual(score_volatility_expansion(1.7), 15.0)
        
    def test_final_integration(self):
        s_spike = score_volume_spike(5.5)
        s_vel = score_trade_velocity(3.5)
        s_mom = score_momentum(2.5)
        s_vola = score_volatility_expansion(1.7)
        
        final = compute_final_radar_score(s_spike, s_vel, s_mom, s_vola)
        self.assertEqual(final, 100)

if __name__ == '__main__':
    unittest.main()
