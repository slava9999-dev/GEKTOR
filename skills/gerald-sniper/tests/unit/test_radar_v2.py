import unittest
import sys
import os

# Add skill root to path
skill_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if skill_root not in sys.path:
    sys.path.append(skill_root)

from core.radar.scoring import (
    compute_volume_spike_score,
    compute_velocity_score,
    compute_momentum_score,
    compute_volatility_score,
    compute_final_radar_score
)

class TestRadarScoring(unittest.TestCase):
    def test_max_score(self):
        # vol_spike > 5 -> 40
        # velocity > 3 -> 25
        # momentum > 2% -> 15
        # volatility > 1.6 -> 15
        # Total raw points = 40 + 25 + 15 + 15 = 95
        # Weights: 40*0.4 + 25*0.25 + 15*0.2 + 15*0.15 = 16 + 6.25 + 3 + 2.25 = 27.5
        
        s_spike = compute_volume_spike_score(6.0)
        s_vel = compute_velocity_score(4.0)
        s_mom = compute_momentum_score(3.0)
        s_vola = compute_volatility_score(2.0)
        
        self.assertEqual(s_spike, 40.0)
        self.assertEqual(s_vel, 25.0)
        self.assertEqual(s_mom, 15.0)
        self.assertEqual(s_vola, 15.0)
        
        final = compute_final_radar_score(s_spike, s_vel, s_mom, s_vola)
        # 27.5 normalized to 100 scale should be 100
        self.assertEqual(final, 100)

    def test_low_score(self):
        final = compute_final_radar_score(0, 0, 0, 0)
        self.assertEqual(final, 0)

    def test_mid_score(self):
        # 2-3 vol spike -> 20. points * 0.4 = 8
        # sum = 8. (8/27.5)*100 = 29
        final = compute_final_radar_score(20, 0, 0, 0)
        self.assertEqual(final, 29)

if __name__ == '__main__':
    unittest.main()
