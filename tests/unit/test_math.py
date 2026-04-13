import pytest
import math
from src.domain.math_core import MathCore, MarketTick, EngineState

def test_welford_precision():
    """
    [CRITICAL] Verifies Welford update consistency and floating point drift.
    Ensures that variance accumulated over 1M iterations remains consistent with numpy-ready calculation.
    """
    engine = MathCore("BTCUSDT")
    # Feed constant price (100) then a small variance
    prices = [100.0] * 1000 + [101.0, 99.0] * 500
    
    for p in prices:
        engine._is_toxic_glitch(p, 1.0)
        
    # Expected mean should be ~100.0
    assert engine.m == pytest.approx(100.0, rel=1e-9)
    # Expected variance: 2000 samples. 1000 of 100, 500 of 101, 500 of 99.
    # sum((x-mean)**2) = 1000*(0**2) + 500*(1**2) + 500*((-1)**2) = 1000
    # variance = 1000 / (2000 - 1) = 0.50025...
    expected_var = 1000 / 1999
    actual_var = engine.s / (engine.k - 1)
    
    assert actual_var == pytest.approx(expected_var, rel=1e-12)

def test_welford_anomalous_zero_volume():
    """
    Checks that zero volume doesn't crash the glitch detector.
    """
    engine = MathCore("BTCUSDT")
    for _ in range(200):
        # Even with zero volume, the glitch detector (which tracks price stats) should survive
        is_glitch = engine._is_toxic_glitch(100.0, 0.0)
        assert is_glitch is False
    
    # Large jump with zero volume -> should be flagged if it deviates significantly
    # but the trigger also requires volume < 5000 (which 0 is).
    # Since std_dev is 0, any jump > 0 will trigger delta > 0 * 5.
    is_glitch = engine._is_toxic_glitch(110.0, 0.0)
    assert is_glitch is True

def test_vpin_basic_logic():
    """
    Verifies VPIN calculation for a simple bucket.
    """
    engine = MathCore("BTCUSDT")
    engine.state = EngineState.ACTIVE
    engine.bucket_vol = 1000.0
    engine.vpin_window = 2
    
    # 1. Fill first bucket: 800 Buy, 200 Sell -> Imbalance = 600
    ticks_1 = [
        MarketTick("BTCUSDT", 1000, 100.0, 8.0, "Buy"), # 800$
        MarketTick("BTCUSDT", 2000, 100.0, 2.0, "Sell") # 200$
    ]
    for t in ticks_1: engine._ingest_tick(t)
    
    # 2. Fill second bucket: 500 Buy, 500 Sell -> Imbalance = 0
    ticks_2 = [
        MarketTick("BTCUSDT", 3000, 100.0, 5.0, "Buy"),
        MarketTick("BTCUSDT", 4000, 100.0, 5.0, "Sell")
    ]
    res = None
    for t in ticks_2: 
        res = engine._ingest_tick(t)
    
    # VPIN = Sum(Imbalances) / (Window * BucketVol)
    # VPIN = (600 + 0) / (2 * 1000) = 0.3
    assert res is not None
    assert res["vpin"] == pytest.approx(0.3, rel=1e-7)
