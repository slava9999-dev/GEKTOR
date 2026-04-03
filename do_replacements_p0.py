import os

base_dir = r"c:\Gerald-superBrain"

# 1. P0: quant_tools.py
quant_tools_path = os.path.join(base_dir, "skills/gerald-super-analyst/quant_tools.py")
with open(quant_tools_path, "r", encoding="utf-8") as f:
    orig_tools = f.read()

# We'll just prepend the new functions and keep format_candles_for_prompt
new_quant = """import numpy as np
from typing import Tuple

def calculate_stop_loss(entry: float, direction: str, atr: float, level: float) -> float:
    buffer = atr * 1.5
    if direction.upper() == "LONG":
        stop = level - buffer
        assert stop < entry - 0.001, f"LONG stop {stop:.8f} >= entry {entry:.8f}"
    else:  # SHORT
        stop = level + buffer
        assert stop > entry + 0.001, f"SHORT stop {stop:.8f} <= entry {entry:.8f}"
    return round(stop, 8)

def calculate_take_profits(entry: float, stop: float, direction: str, rr1: float = 1.5, rr2: float = 3.0) -> Tuple[float, float]:
    risk = abs(entry - stop)
    if direction.upper() == "LONG":
        tp1 = entry + (risk * rr1)
        tp2 = entry + (risk * rr2)
        assert tp1 > entry + 0.001, "LONG TP1 <= entry"
    else:
        tp1 = entry - (risk * rr1)
        tp2 = entry - (risk * rr2)
        assert tp1 < entry - 0.001, "SHORT TP1 >= entry"
    return round(tp1, 8), round(tp2, 8)

def validate_rr(entry: float, stop: float, tp1: float) -> bool:
    risk = abs(entry - stop)
    reward = abs(tp1 - entry)
    rr = reward / risk
    return rr >= 1.5

def format_candles_for_prompt(candles: list, last_n: int = 5) -> str:
    recent = candles[-last_n:]
    if not recent: return "No candle data available."
    all_volumes = [float(c.get('volume', 0)) for c in candles[-30:]]
    avg_vol = sum(all_volumes) / len(all_volumes) if all_volumes else 1
    lines = []
    for i, c in enumerate(recent):
        o, h, l, cl = float(c['open']), float(c['high']), float(c['low']), float(c['close'])
        v = float(c.get('volume', 0))
        body = cl - o
        full_range = h - l if h > l else 0.0001
        body_pct = abs(body) / full_range * 100
        direction = "GREEN" if cl > o else "RED"
        vol_ratio = v / avg_vol if avg_vol > 0 else 0
        if body_pct < 20: candle_type = "DOJI"
        elif body_pct > 70 and vol_ratio > 1.5: candle_type = "STRONG"
        elif body_pct > 70: candle_type = "BIG_BODY"
        else: candle_type = "NORMAL"
        upper_wick = ((h - max(o, cl)) / full_range * 100)
        lower_wick = ((min(o, cl) - l) / full_range * 100)
        lines.append(f"  [{i+1}] {direction} {candle_type} | body:{body_pct:.0f}% | upper_wick:{upper_wick:.0f}% | lower_wick:{lower_wick:.0f}% | vol:{vol_ratio:.1f}x avg")
    return "\\n".join(lines)
"""
with open(quant_tools_path, "w", encoding="utf-8") as f:
    f.write(new_quant)

# P0: Prompts
bull_p = """Аргументы ЗА сделку: {direction} {symbol} entry={entry} level={level} stop={stop} tp1={tp1}.
Confidence 0-10 (≤5 если BTC DOWN и LONG).
JSON ONLY: {{"confidence": int, "args": ["list"]}}"""

bear_p = """Аргументы ПРОТИВ: {direction} {symbol} entry={entry} level={level} stop={stop} tp1={tp1}.
JSON ONLY: {{"confidence": int, "args": ["list"]}}"""

scen_p = """3 сценария для {direction} {symbol} entry={entry} stop={stop} tp1={tp1}:
A: Импульс (win) B: Откат (partial) C: Отмена (loss).
JSON: {{"scenario_a": "text", "scenario_b": "text", "scenario_c": "text"}}
НЕ считать цены!"""

with open(os.path.join(base_dir, "skills/gerald-super-analyst/prompts/bull_agent.txt"), "w", encoding="utf-8") as f:
    f.write(bull_p)
with open(os.path.join(base_dir, "skills/gerald-super-analyst/prompts/bear_agent.txt"), "w", encoding="utf-8") as f:
    f.write(bear_p)
with open(os.path.join(base_dir, "skills/gerald-super-analyst/prompts/scenario_planner.txt"), "w", encoding="utf-8") as f:
    f.write(scen_p)

