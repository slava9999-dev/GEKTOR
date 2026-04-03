import pytest
from quant_tools import calculate_stop_loss, calculate_take_profits, validate_rr

def test_calculate_stop_loss():
    # LONG
    stop = calculate_stop_loss(entry=100.0, direction="LONG", atr=1.0, level=100.0)
    assert stop == 98.5
    assert stop < 100.0
    
    # SHORT
    stop_short = calculate_stop_loss(entry=100.0, direction="SHORT", atr=1.0, level=100.0)
    assert stop_short == 101.5
    assert stop_short > 100.0

def test_calculate_take_profits():
    # LONG
    tp1, tp2 = calculate_take_profits(entry=100.0, stop=98.5, direction="LONG")
    assert tp1 == 102.25
    assert tp2 == 104.5
    assert tp1 > 100.0
    
    # SHORT
    tp1_short, tp2_short = calculate_take_profits(entry=100.0, stop=101.5, direction="SHORT")
    assert tp1_short == 97.75
    assert tp2_short == 95.5
    assert tp1_short < 100.0

def test_validate_rr():
    assert validate_rr(entry=100.0, stop=98.5, tp1=102.25) == True
    assert validate_rr(entry=100.0, stop=98.5, tp1=100.5) == False
    
    assert validate_rr(entry=100.0, stop=101.5, tp1=97.75) == True
    assert validate_rr(entry=100.0, stop=101.5, tp1=99.5) == False
