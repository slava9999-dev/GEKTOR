import sys
import os
from typing import List

# Add skills/gerald-sniper to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.levels.models import LevelCluster, LevelSide
from services.level_service import LevelService

def make_cluster(price: float, touches: int, strength: float, confluence: int = 1) -> LevelCluster:
    return LevelCluster(
        symbol="SUIUSDT",
        price=price,
        side=LevelSide.RESISTANCE,
        sources=["SWING:1h"],
        touches_total=touches,
        confluence_score=confluence,
        strength=strength,
        tolerance_pct=0.003,
        level_id=f"test_{price}"
    )

def test_filter_removes_weak_levels():
    svc = LevelService(None)
    clusters = [
        make_cluster(1.0, touches=1, strength=0.5), # too weak, too low touches
        make_cluster(1.1, touches=2, strength=3.0), # should pass
    ]
    result = svc._filter_levels(clusters, 1.05)
    assert len(result) == 1
    assert result[0].price == 1.1

def test_filter_max_levels():
    svc = LevelService(None)
    # 20 clusters should be limited to 8
    clusters = [
        make_cluster(float(i), touches=2, strength=3.0)
        for i in range(1, 21)
    ]
    result = svc._filter_levels(clusters, 10.0)
    assert len(result) <= 8

def test_filter_removes_distant_levels():
    svc = LevelService(None)
    # MAX_DIST_PCT is 15.0
    clusters = [
        make_cluster(1.0, touches=2, strength=3.0),
        make_cluster(5.0, touches=3, strength=5.0), # far away
    ]
    # Dist = abs(5-1)/1 * 100 = 400%
    result = svc._filter_levels(clusters, 1.0)
    prices = [c.price for c in result]
    assert 5.0 not in prices
    assert 1.0 in prices

def test_filter_sorts_by_strength():
    svc = LevelService(None)
    clusters = [
        make_cluster(1.0, touches=2, strength=2.0),
        make_cluster(1.1, touches=3, strength=5.0),
        make_cluster(1.2, touches=2, strength=3.0),
    ]
    result = svc._filter_levels(clusters, 1.1)
    strengths = [s.strength for s in result]
    # Check that it's descending order
    assert strengths == sorted(strengths, reverse=True)

if __name__ == "__main__":
    try:
        test_filter_removes_weak_levels()
        test_filter_max_levels()
        test_filter_removes_distant_levels()
        test_filter_sorts_by_strength()
        print("✅ BUG-3 Unit Tests Passed!")
    except Exception as e:
        print(f"❌ BUG-3 Unit Tests Failed: {e}")
        import traceback
        traceback.print_exc()
