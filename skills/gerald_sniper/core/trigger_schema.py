from typing import Any, Dict

REQUIRED_TRIGGER_FIELDS = {
    "symbol": str,
    "interval": str,
    "candle_start_ts": int,
    "trigger_type": str,
    "level": (int, float),
}

def validate_trigger_schema(trigger: Dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(trigger, dict):
        return False, "trigger_not_dict"

    for field, expected_type in REQUIRED_TRIGGER_FIELDS.items():
        if field not in trigger:
            return False, f"missing_{field}"
        if not isinstance(trigger[field], expected_type):
            return False, f"invalid_type_{field}"

    if not trigger["symbol"]:
        return False, "empty_symbol"

    if not trigger["interval"]:
        return False, "empty_interval"

    if trigger["candle_start_ts"] <= 0:
        return False, "invalid_candle_start_ts"

    if not trigger["trigger_type"]:
        return False, "empty_trigger_type"

    try:
        level = float(trigger["level"])
    except Exception:
        return False, "invalid_level_cast"

    if level <= 0:
        return False, "nonpositive_level"

    return True, "ok"
