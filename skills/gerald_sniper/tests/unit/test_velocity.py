import unittest
import sys
import os

# Add skill root to path
skill_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if skill_root not in sys.path:
    sys.path.append(skill_root)

from core.radar.trade_velocity import calculate_trade_velocity, score_trade_velocity

class TestVelocity(unittest.TestCase):
    def test_calculation(self):
        # 30 trades in last min, avg 10 trades per min in last 10m -> velocity 3.0
        v = calculate_trade_velocity(30, 10.0)
        self.assertEqual(v, 3.0)

    def test_scoring(self):
        self.assertEqual(score_trade_velocity(1.1), 0)
        self.assertEqual(score_trade_velocity(1.4), 5)
        self.assertEqual(score_trade_velocity(1.9), 10)
        self.assertEqual(score_trade_velocity(2.5), 20)
        self.assertEqual(score_trade_velocity(3.5), 25)

if __name__ == '__main__':
    unittest.main()
