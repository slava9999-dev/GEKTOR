import unittest
import sys
import os

# Add skill root to path
skill_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if skill_root not in sys.path:
    sys.path.append(skill_root)

from core.radar.watchlist import select_watchlist, calculate_watchlist_diff

class TestWatchlist(unittest.TestCase):
    def test_selection(self):
        results = [
            {'symbol': 'A', 'score': 80},
            {'symbol': 'B', 'score': 60},
            {'symbol': 'C', 'score': 40},
            {'symbol': 'D', 'score': 55},
        ]
        # Min score 55, Top 2
        watchlist = select_watchlist(results, max_size=2, min_score=55)
        self.assertEqual(watchlist, ['A', 'B'])
        
        # Min score 55, Top 5
        watchlist_3 = select_watchlist(results, max_size=5, min_score=55)
        self.assertEqual(watchlist_3, ['A', 'B', 'D'])

    def test_diff(self):
        old = ['BTC', 'ETH', 'SOL']
        new = ['ETH', 'SOL', 'ADA']
        
        added, removed, kept = calculate_watchlist_diff(old, new)
        self.assertEqual(set(added), {'ADA'})
        self.assertEqual(set(removed), {'BTC'})
        self.assertEqual(set(kept), {'ETH', 'SOL'})

if __name__ == '__main__':
    unittest.main()
