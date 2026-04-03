from datetime import datetime
from loguru import logger
from typing import List, Optional
from core.structure.models import StructureEvent, StructureType
from core.levels.models import LevelCluster


def detect_all(
    symbol: str, 
    timeframe: str, 
    candles: List[dict],
    cluster: LevelCluster
) -> List[StructureEvent]:
    """
    Runs full Digash behavioral analysis on price/level interaction.
    """
    events = []
    
    # 1. Scenario 4: Compression
    comp = detect_compression(symbol, timeframe, candles, cluster)
    if comp: events.append(comp)
    
    # 2. Scenario 2: Break
    brk = detect_break(symbol, timeframe, candles, cluster)
    if brk: events.append(brk)
    
    # 3. Scenario 3: Retest
    ret = detect_retest(symbol, timeframe, candles, cluster)
    if ret: events.append(ret)
    
    # 4. Sweep (Liquidity Grab)
    sw = detect_sweep(symbol, timeframe, candles, cluster)
    if sw: events.append(sw)
    
    return events


def detect_compression(
    symbol: str, timeframe: str, candles: List[dict], cluster: LevelCluster, min_candles: int = 4
) -> Optional[StructureEvent]:
    """
    Checks for Scenario 4: Price shrinking approach.
    """
    if len(candles) < min_candles: return None
    
    last = candles[-min_candles:]
    ranges = [c['high'] - c['low'] for c in last]
    
    # Shrinking ranges check
    is_compressing = all(ranges[i] < ranges[i-1] for i in range(1, len(ranges)))
    if not is_compressing: return None
    
    current_price = candles[-1]['close']
    dist = abs(current_price - cluster.price) / (current_price or 1)
    
    # Proximity for compression
    prox = max(0.003, cluster.tolerance_pct * 2)
    if dist > prox: return None
    
    strength = 1.0 - (dist / prox)
    return StructureEvent(
        symbol=symbol, timeframe=timeframe, type=StructureType.COMPRESSION,
        level_id=cluster.level_id, level_price=cluster.price, candle_ts=datetime.utcnow(),
        strength=round(strength, 2), extra={"ranges": [round(r, 6) for r in ranges], "dist": dist}
    )


def detect_break(
    symbol: str, timeframe: str, candles: List[dict], cluster: LevelCluster
) -> Optional[StructureEvent]:
    """
    Checks for Scenario 2: Momentum breakout.
    """
    if len(candles) < 5: return None
    
    level = cluster.price
    prev = candles[-2]
    curr = candles[-1]
    tol = max(cluster.tolerance_pct, 0.001)
    
    # Resistance break
    res_break = prev['close'] < level and curr['close'] > level * (1 + tol)
    # Support break
    sup_break = prev['close'] > level and curr['close'] < level * (1 - tol)
    
    if not (res_break or sup_break): return None
    
    vols = [c['volume'] for c in candles[-11:-1]]
    avg_vol = sum(vols) / len(vols) if vols else 0
    vol_spike = curr['volume'] > avg_vol * 1.3
    
    break_pct = abs(curr['close'] - level) / level
    strength = min(1.0, break_pct / 0.005)
    if vol_spike: strength = min(1.0, strength * 1.3)
    
    return StructureEvent(
        symbol=symbol, timeframe=timeframe, type=StructureType.BREAK,
        level_id=cluster.level_id, level_price=level, candle_ts=datetime.utcnow(),
        strength=round(strength, 2), extra={"direction": "up" if res_break else "down", "vol_spike": vol_spike}
    )


def detect_retest(
    symbol: str, timeframe: str, candles: List[dict], cluster: LevelCluster, lookback: int = 12
) -> Optional[StructureEvent]:
    """
    Checks for Scenario 3: Price confirms level after break.
    """
    if len(candles) < lookback + 2: return None
    
    level = cluster.price
    tol = max(cluster.tolerance_pct, 0.002)
    hist = candles[-lookback-2:-2]
    
    # Resistance retest: was broken up, now price touches from above
    broken_up = any(c['close'] > level * (1 + tol) for c in hist)
    if broken_up:
        retest = candles[-2]['low'] <= level * (1 + tol * 2) and candles[-1]['close'] > level
        if retest:
            dist = abs(candles[-2]['low'] - level) / level
            return StructureEvent(
                symbol=symbol, timeframe=timeframe, type=StructureType.RETEST,
                level_id=cluster.level_id, level_price=level, candle_ts=datetime.utcnow(),
                strength=round(max(0.3, 1.0 - dist / tol), 2), extra={"side": "up_retest"}
            )
    
    return None


def detect_sweep(
    symbol: str, timeframe: str, candles: List[dict], cluster: LevelCluster
) -> Optional[StructureEvent]:
    """
    Liquidity Sweep / False breakout check.
    """
    if len(candles) < 2: return None
    
    curr = candles[-1]
    level = cluster.price
    tol = max(cluster.tolerance_pct, 0.001)
    
    res_sweep = curr['high'] > level * (1 + tol) and curr['close'] < level
    sup_sweep = curr['low'] < level * (1 - tol) and curr['close'] > level
    
    if not (res_sweep or sup_sweep): return None
    
    wick_size = (curr['high'] - curr['close']) / curr['close'] if res_sweep else (curr['close'] - curr['low']) / curr['low']
    strength = min(1.0, wick_size / 0.01)
    
    return StructureEvent(
        symbol=symbol, timeframe=timeframe, type=StructureType.SWEEP,
        level_id=cluster.level_id, level_price=level, candle_ts=datetime.utcnow(),
        strength=round(strength, 2), extra={"wick_pct": round(wick_size * 100, 3)}
    )
