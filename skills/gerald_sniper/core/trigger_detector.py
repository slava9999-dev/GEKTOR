# core/trigger_detector.py
from .structure_detector import structure_detector

# Proxy exports to satisfy smoke tests and legacy code
detect_compression = structure_detector.detect_compression
detect_breakout = structure_detector.detect_break
detect_retest = structure_detector.detect_retest
detect_sweep = structure_detector.detect_sweep

def detect_volume_spike_at_level(candles, level_price, side):
    """
    Mock/Strap for legacy trigger validation. 
    Actual volume scoring happens in scoring.py or radar/volume_spike.py.
    """
    return True # Placeholder for import validation
