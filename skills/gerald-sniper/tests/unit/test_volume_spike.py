import unittest
import sys
import os

# Add skill root to path
skill_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if skill_root not in sys.path:
    sys.path.append(skill_root)

from core.radar.volume_spike import calculate_volume_spike, score_volume_spike

class TestVolumeSpike(unittest.TestCase):
    def test_calculation(self):
        # last candle 100, previous candles avg 50 -> spike 2.0
        history = [50] * 11 + [100]
        spike = calculate_volume_spike(history)
        self.assertEqual(spike, 2.0)

    def test_scoring(self):
        self.assertEqual(score_volume_spike(0.5), 0)
        self.assertEqual(score_volume_spike(1.3), 5)
        self.assertEqual(score_volume_spike(1.8), 10)
        self.assertEqual(score_volume_spike(2.5), 20)
        self.assertEqual(score_volume_spike(4.0), 30)
        self.assertEqual(score_volume_spike(5.5), 40)

if __name__ == '__main__':
    unittest.main()
