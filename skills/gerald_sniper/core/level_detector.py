import numpy as np
import math
from scipy.signal import argrelextrema, find_peaks
from loguru import logger
from typing import List, Dict, Any
from utils.safe_math import safe_float


def detect_swing_points(candles_h1: List[Dict], left: int = 5, right: int = 5) -> List[Dict]:
    """
    Точка является swing high/low если она выше/ниже всех N свечей слева и справа.
    """
    levels = []
    highs = [safe_float(c.get('high', 0)) for c in candles_h1]
    lows = [safe_float(c.get('low', 0)) for c in candles_h1]
    
    for i in range(left, len(candles_h1) - right):
        is_high = all(highs[i] >= highs[j] for j in range(i-left, i+right+1) if j != i)
        is_low = all(lows[i] <= lows[j] for j in range(i-left, i+right+1) if j != i)
        
        if is_high:
            levels.append({'price': highs[i], 'type': 'RESISTANCE', 'idx': i})
        if is_low:
            levels.append({'price': lows[i], 'type': 'SUPPORT', 'idx': i})
            
    return levels

def cluster_levels(swing_points: List[Dict], current_price: float, tolerance_pct: float = 0.5) -> List[Dict]:
    """
    Группирует swing points в кластеры уровней (Алгоритм Level Clustering).
    """
    if not swing_points: return []
    
    # Sort by price
    sorted_points = sorted(swing_points, key=lambda x: x['price'])
    clusters = []
    
    if not sorted_points: return []
    
    current_cluster = [sorted_points[0]]
    
    for pt in sorted_points[1:]:
        dist_pct = abs(pt['price'] - current_cluster[-1]['price']) / current_cluster[-1]['price'] * 100
        
        if dist_pct <= tolerance_pct:
            current_cluster.append(pt)
        else:
            avg_price = sum(p['price'] for p in current_cluster) / len(current_cluster)
            res_touches = len([p for p in current_cluster if p['type'] == 'RESISTANCE'])
            sup_touches = len([p for p in current_cluster if p['type'] == 'SUPPORT'])
            last_touch_idx = max(p['idx'] for p in current_cluster)
            
            clusters.append({
                'price': avg_price,
                'touches': len(current_cluster),
                'res_count': res_touches,
                'sup_count': sup_touches,
                'last_idx': last_touch_idx,
                'points': current_cluster
            })
            current_cluster = [pt]
            
    # Add last
    avg_price = sum(p['price'] for p in current_cluster) / len(current_cluster)
    clusters.append({
        'price': avg_price,
        'touches': len(current_cluster),
        'res_count': len([p for p in current_cluster if p['type'] == 'RESISTANCE']),
        'sup_count': len([p for p in current_cluster if p['type'] == 'SUPPORT']),
        'last_idx': max(p['idx'] for p in current_cluster),
        'points': current_cluster
    })
    
    return clusters

def calculate_level_strength(c: Dict, current_price: float, total_candles: int, natr: float) -> float:
    """
    Strength = (touches * 15) + TF bonuses + Recency + Round bonus (Алгоритм Level Strength).
    """
    strength = c['touches'] * 15.0
    
    # Recency: if touched in last 24 candles (for H1) -> +10
    hours_ago = total_candles - 1 - c['last_idx']
    if hours_ago < 24:
        strength += 10.0
        
    # Roundness bonus
    price = c['price']
    if abs(price - round(price, 0)) < (price * 0.001) or \
       abs(price * 10 - round(price * 10, 0)) < (price * 0.001):
        strength += 5.0
        
    # Cap at 100
    return min(100.0, strength)

def find_confluence(level_price: float, htf_levels: List[float], tolerance_pct: float = 0.8) -> bool:
    """
    Ищет подтверждение уровня на старшем ТФ (HTF Confluence).
    """
    for htf in htf_levels:
        dist = abs(level_price - htf) / htf * 100
        if dist <= tolerance_pct:
            return True
    return False


def detect_levels(
    candles_h1: List[Dict],
    current_price: float,
    config: Dict
) -> List[Dict]:
    if len(candles_h1) < 20: 
        return []

    highs = np.array([safe_float(c.get('high', 0)) for c in candles_h1])
    lows = np.array([safe_float(c.get('low', 0)) for c in candles_h1])
    atr = np.mean([abs(h - l) for h, l in zip(highs, lows)])
    natr = (atr / current_price) * 100 if current_price > 0 else 1.0

    # 1. Swing Point Detection (Fractal Algorithm)
    swings = detect_swing_points(candles_h1, left=config.get('swing_left', 5), right=config.get('swing_right', 5))
    
    # 2. Level Clustering
    # Adaptive Clustering Tolerance based on NATR
    cluster_tol = max(0.2, min(0.8, natr * 0.3))
    clusters = cluster_levels(swings, current_price, tolerance_pct=cluster_tol)
    
    levels = []
    total_candles = len(candles_h1)
    
    for c in clusters:
        # Distance check
        dist_pct = abs(c['price'] - current_price) / current_price * 100
        if dist_pct > config.get('max_distance_pct', 5.0):
            continue
            
        strength = calculate_level_strength(c, current_price, total_candles, natr)
        
        # Digash Filter: Only keep levels with at least 2 touches or high strength
        if c['touches'] < 2 and strength < 40:
            continue
            
        ltype = 'RESISTANCE' if c['price'] > current_price else 'SUPPORT'
        
        levels.append({
            'price': round(safe_float(c['price']), 8),
            'type': ltype,
            'touches': c['touches'],
            'resistance_touches': c['res_count'],
            'support_touches': c['sup_count'],
            'distance_pct': round(safe_float(dist_pct), 2),
            'strength': round(safe_float(strength), 1),
            'stale': (total_candles - 1 - c['last_idx']) > config.get('freshness_decay_hours', 72),
            'source': 'SWING_CLUSTER',
            'hours_since_touch': total_candles - 1 - c['last_idx'],
        })
        
    levels.sort(key=lambda x: (-x['strength'], x['distance_pct']))
    return levels[:config.get('max_levels_per_coin', 6)]


def detect_dynamic_levels(
    candles_h1: List[Dict],
    current_price: float,
    config: Dict,
    symbol: str = "UNKNOWN"
) -> List[Dict]:
    if not candles_h1 or current_price <= 0:
        return []

    if not config.get('enable_dynamic_levels', True):
        return []

    levels = []
    max_distance_pct = config.get('max_distance_pct', 4.0)
    dynamic_max_strength = config.get('dynamic_level_max_strength', 75)

    highs = [safe_float(c['high']) for c in candles_h1]
    lows = [safe_float(c['low']) for c in candles_h1]

    # === 1. Недельный максимум и минимум ===
    week_high = max(highs)
    week_low = min(lows)

    # Для WEEK_EXTREME — свой distance filter:
    week_max_distance = max_distance_pct * 2.0  # 8% вместо 4%

    logger.debug(
        f"WEEK_EXTREME check: price={current_price:.6f}, "
        f"week_high={week_high:.6f} (dist={abs(week_high-current_price)/current_price*100:.1f}%), "
        f"week_low={week_low:.6f} (dist={abs(week_low-current_price)/current_price*100:.1f}%)"
    )

    for price, ltype in [(week_high, 'RESISTANCE'), (week_low, 'SUPPORT')]:
        dist_pct = abs(price - current_price) / current_price * 100
        if 0.3 < dist_pct < week_max_distance:
            # We strictly enforce that a Weekly Extreme requires at least a double top/bottom (2 touches)
            # The exact number will be validated against historical candles, but for now we set a base expectation.
            levels.append({
                'price': round(safe_float(price), 8),
                'type': ltype,
                'touches': 2, # Base significance for a weekly extreme
                'resistance_touches': 1 if ltype == 'RESISTANCE' else 0,
                'support_touches': 1 if ltype == 'SUPPORT' else 0,
                'distance_pct': round(dist_pct, 2),
                'strength': 60.0, # Increased from 50 to differentiate from trash
                'stale': False,
                'source': 'WEEK_EXTREME',
                'hours_since_touch': 0,
            })

    # === 2. Круглые числа (психологические уровни) ===
    if current_price > 10000:
        steps = [1000, 500, 250]
    elif current_price > 1000:
        steps = [100, 50, 25]
    elif current_price > 100:
        steps = [10, 5, 2.5]
    elif current_price > 10:
        steps = [1, 0.5, 0.25]
    elif current_price > 1:
        steps = [0.1, 0.05, 0.025]
    elif current_price > 0.01:
        steps = [0.01, 0.005, 0.0025]
    elif current_price > 0.001:
        steps = [0.001, 0.0005, 0.00025]
    else:
        steps = [0.0001, 0.00005, 0.000025]

    for step in steps:
        above = (int(current_price / step) + 1) * step
        below = int(current_price / step) * step

        for rn, ltype in [(above, 'RESISTANCE'), (below, 'SUPPORT')]:
            dist_pct = abs(rn - current_price) / current_price * 100
            
            # Проверяем: был ли разворот у этого круглого числа?
            raw_touches = 0
            tolerance = rn * 0.003  # 0.3% зона
            for h, l in zip(highs, lows):
                if abs(h - rn) < tolerance or abs(l - rn) < tolerance:
                    raw_touches += 1
                    
            effective_max_dist = max_distance_pct
            if raw_touches >= 10:
                effective_max_dist *= 2.5
            elif raw_touches >= 5:
                effective_max_dist *= 1.5

            if 0.1 < dist_pct < effective_max_dist:

                # Continuous scoring: base + touches + proximity bonus
                proximity_bonus = max(0, 10 - dist_pct * 3)  # closer = better
                
                # --- NOISE REDUCTION: Ignore Round Numbers with 0 historical touches ---
                if raw_touches < 1:
                    continue

                # --- ANTI-ROUND-NUMBER BIAS ---
                # Force raw touches to 1 to prevent artificial scoring inflation
                effective_touches = 1
                # Adjusted formula: base 45 + bonus. Max around 60. 
                # This allows them to pass the 55-score alert threshold when very close.
                strength = (45.0 + (effective_touches * 5) + proximity_bonus) 
                # Extra bonus if it has 3+ real touches
                if raw_touches >= 3:
                     strength += 5

                strength = min(safe_float(dynamic_max_strength), strength)

                logger.debug(
                    f"[ROUND_DEBUG] symbol={symbol} source=ROUND_NUMBER raw_touches={raw_touches} "
                    f"effective_touches={effective_touches} score={strength:.1f}"
                )

                levels.append({
                    'price': round(safe_float(rn), 8),
                    'type': ltype,
                    'touches': raw_touches, # Keep real touches for audit
                    'resistance_touches': raw_touches if ltype == 'RESISTANCE' else 0,
                    'support_touches': raw_touches if ltype == 'SUPPORT' else 0,
                    'distance_pct': round(dist_pct, 2),
                    'strength': round(strength, 1),
                    'stale': False,
                    'source': 'ROUND_NUMBER',
                    'hours_since_touch': 0,
                })

    # Дедупликация: если два уровня ближе 0.3% — оставить сильнейший
    levels.sort(key=lambda x: x['price'])
    deduped = []
    for lvl in levels:
        if deduped and lvl['price'] > 0 and abs(lvl['price'] - deduped[-1]['price']) / lvl['price'] < 0.003:
            if lvl['strength'] > deduped[-1]['strength']:
                deduped[-1] = lvl
        else:
            deduped.append(lvl)

    return deduped


def validate_kline_data(candles: List[Dict], symbol: str) -> List[Dict]:
    """Фильтрует аномальные свечи (шпильки, битые данные) перед расчётом уровней."""
    if len(candles) < 30:
        return candles

    # Extract prices for statistical analysis
    highs = np.array([safe_float(c.get('high', 0)) for c in candles])
    lows = np.array([safe_float(c.get('low', 0)) for c in candles])
    closes = np.array([safe_float(c.get('close', 0)) for c in candles])
    
    # Avoid division by zero
    valid_mask = closes > 0
    if not np.any(valid_mask):
        return candles

    ranges = np.zeros_like(closes)
    ranges[valid_mask] = (highs[valid_mask] - lows[valid_mask]) / closes[valid_mask]
    
    # Use Median and MAD (Median Absolute Deviation) for robust outlier detection
    med_range = np.median(ranges)
    mad_range = np.median(np.abs(ranges - med_range))
    if mad_range == 0:
        mad_range = med_range * 0.1 # Fallback
    
    # Threshold for "crazy" volatility (Z-score > 8 roughly, or absolute > 25%)
    # Liquid alts on 1h rarely move > 15-20% in a single candle without it being a flash-crash/spike
    upper_bound = max(0.20, med_range + 10 * mad_range)
    
    clean = []
    removed_count = 0
    
    # Reference median price for sanity check
    recent_median_price = np.median(closes[-50:]) if len(closes) >= 50 else np.median(closes)

    for i, c in enumerate(candles):
        r = ranges[i]
        price = closes[i]
        
        # 1. Price Sanity (0.2x to 5x of median)
        if price > recent_median_price * 5 or price < recent_median_price * 0.2:
            removed_count += 1
            continue
            
        # 2. Range Anomaly (flash spikes)
        if r > upper_bound:
            # Check if it's a "confirmed" level or just a wick. 
            # If body is small but range is huge -> it's a spike.
            body = abs(safe_float(c.get('open', 0)) - price) / price
            if body < r * 0.3: # Spike with small body
                removed_count += 1
                continue
                
        # 3. Flat / Zero-data candles (sometimes Bybit sends these during maintenance)
        if r == 0 and i > 0 and price == closes[i-1]:
            # This is fine, just duplicate/stale, but not an anomaly to "gate"
            pass

        clean.append(c)

    if removed_count > 0:
        logger.warning(
            f"🗑️ {symbol}: Gated {removed_count} anomalous candles "
            f"(Upper range bound: {upper_bound:.2%}, Median range: {med_range:.2%})"
        )

    return clean


def detect_all_levels(
    candles_h1: List[Dict],
    current_price: float,
    config: Dict,
    symbol: str = "UNKNOWN"
) -> List[Dict]:
    candles_h1 = validate_kline_data(candles_h1, symbol)
    
    # KDE-уровни (существующая логика с адаптивными порогами)
    kde_levels = detect_levels(candles_h1, current_price, config)

    # Динамические уровни (новое)
    dyn_levels = detect_dynamic_levels(candles_h1, current_price, config, symbol=symbol)

    logger.info(
        f"📐 KDE: {len(kde_levels)} levels, Dynamic: {len(dyn_levels)} levels "
        f"(candles={len(candles_h1)}, price={current_price:.4f})"
    )

    # Объединение с дедупликацией
    final = list(kde_levels)  # Начинаем с копии KDE

    for dyn in dyn_levels:
        is_duplicate = False
        for kde in final:
            if kde['price'] > 0 and abs(dyn['price'] - kde['price']) / kde['price'] < 0.005:
                # Совпадение — бонус к KDE уровню за подтверждение
                kde['strength'] = min(100.0, kde['strength'] + 10)
                kde['source'] = kde.get('source', 'KDE') + '+' + dyn.get('source', 'DYN')
                is_duplicate = True
                break
        if not is_duplicate:
            final.append(dyn)

    # Сортировка: сильнейшие и ближайшие первые
    final.sort(key=lambda x: (-x.get('strength', 0), x.get('distance_pct', 999)))

    max_levels = config.get('max_levels_per_coin', 6)

    if final:
        sources = [lvl.get('source', '?') for lvl in final[:max_levels]]
        logger.debug(f"Levels found: {len(final)} total, sources: {sources}")

    return final[:max_levels]
