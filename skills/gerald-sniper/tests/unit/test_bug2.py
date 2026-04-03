import sys
import os
from dataclasses import dataclass
from typing import Any

# Add skills/gerald-sniper to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.levels.models import (
    get_level_price,
    get_level_side,
    get_level_id
)

@dataclass
class StoredLevel:
    symbol:     str
    price:      float
    side:       str
    source:     str
    touches:    int
    score:      float
    level_id:   str
    confluence: int

def test_get_price_from_dict():
    level = {"price": 1.23, "side": "RESISTANCE"}
    assert get_level_price(level) == 1.23

def test_get_price_from_stored_level():
    level = StoredLevel(
        symbol="SUIUSDT",
        price=1.23,
        side="RESISTANCE",
        source="SWING",
        touches=2,
        score=55.0,
        level_id="abc",
        confluence=2
    )
    assert get_level_price(level) == 1.23

def test_get_price_none_value():
    level = {"price": None}
    assert get_level_price(level) == 0.0

def test_get_price_missing_key():
    level = {}
    assert get_level_price(level) == 0.0

def test_get_side_from_dict():
    level = {"side": "SUPPORT"}
    assert get_level_side(level) == "SUPPORT"

def test_get_side_from_stored_level():
    level = StoredLevel(
        symbol="SUIUSDT",
        price=1.23,
        side="SUPPORT",
        source="SWING",
        touches=2,
        score=55.0,
        level_id="abc",
        confluence=2
    )
    assert get_level_side(level) == "SUPPORT"

if __name__ == "__main__":
    try:
        test_get_price_from_dict()
        test_get_price_from_stored_level()
        test_get_price_none_value()
        test_get_price_missing_key()
        test_get_side_from_dict()
        test_get_side_from_stored_level()
        print("✅ BUG-2 Unit Tests Passed!")
    except Exception as e:
        print(f"❌ BUG-2 Unit Tests Failed: {e}")
        import traceback
        traceback.print_exc()
