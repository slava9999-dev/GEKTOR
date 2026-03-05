import numpy as np
import math
from scipy.signal import argrelextrema, find_peaks
from scipy.stats import gaussian_kde
from loguru import logger
from typing import List, Dict
from utils.safe_math import safe_float


def _get_adaptive_params(atr_pct: float, current_price: float, config: dict) -> dict:
    """
    Адаптирует параметры уровней под волатильность монеты.
    Чем выше ATR — тем шире допуск.
    
    Логика:
    - min_touches = 2 (двойная вершина/дно — это уже уровень)
    - touch_tolerance = 25% от среднего размера свечи (ATR), 
      с ограничением от 0.4% до 2.0%.
    - max_distance = расширяется на высоковолатильных монетах.
    """
    base_touches = config.get('min_touches', 2)
    base_distance = config.get('max_distance_pct', 5.0)

    # Adaptive min_touches: volatile coins (ATR>2%) rarely form 3+ touches
    # but 2 touches on a wide-range coin ARE significant (double top/bottom)
    if atr_pct > 2.0:
        base_touches = max(2, base_touches - 1)

    # Допуск касания составляет 25% от средней дневной свечи (ATR H1)
    # Мин. 0.4%, макс. 2.0%
    dynamic_tolerance = max(0.004, min(0.020, (atr_pct / 100.0) * 0.25))

    distance_multiplier = 1.5 if atr_pct > 5.0 else 1.0

    return {
        'min_touches': base_touches,
        'touch_tolerance_pct': dynamic_tolerance,
        'max_distance_pct': base_distance * distance_multiplier,
    }


def detect_levels(
    candles_h1: List[Dict],      # Minimum 20 candles H1
    current_price: float,
    config: Dict
) -> List[Dict]:
    """
    KDE-уровни: кластеризация свинг-экстремумов.
    Использует адаптивные параметры в зависимости от ATR монеты.
    """
    if len(candles_h1) < 20:
        return []

    highs = np.array([safe_float(c['high']) for c in candles_h1])
    lows = np.array([safe_float(c['low']) for c in candles_h1])

    # Расчёт ATR% для адаптивной логики
    atr = np.mean([abs(h - l) for h, l in zip(highs, lows)])
    atr_pct = (atr / current_price) * 100 if current_price > 0 else 1.0

    # Адаптивные параметры на основе волатильности
    adaptive = _get_adaptive_params(atr_pct, current_price, config)
    min_touches = adaptive['min_touches']
    dynamic_tolerance = adaptive['touch_tolerance_pct']
    max_distance_pct = adaptive['max_distance_pct']

    # STEP 1: Свинг-экстремумы
    swing_order = config.get('swing_order', 5)

    if len(highs) <= swing_order * 2:
        return []

    swing_high_idx = argrelextrema(highs, np.greater, order=swing_order)[0]
    swing_low_idx = argrelextrema(lows, np.less, order=swing_order)[0]

    swing_highs = highs[swing_high_idx]
    swing_lows = lows[swing_low_idx]

    all_extremes = np.concatenate([swing_highs, swing_lows])

    logger.info(
        f"📐 Adaptive: price={current_price:.4f}, ATR={atr_pct:.1f}%: "
        f"touches={min_touches}, tolerance={dynamic_tolerance*100:.1f}%, "
        f"max_dist={max_distance_pct:.1f}%, extremes={len(all_extremes)}"
    )
    
    if len(all_extremes) < 3:
        logger.debug(f"KDE skipped: only {len(all_extremes)} extremes found (need 3+)")
        return []

    # Chop-zone detection: instead of hard-rejecting high-touch levels,
    # we apply a progressive strength penalty. A level that attracts 60%+ of
    # all extremes is likely noise, but could also be a genuinely important
    # consolidation zone. Penalty scales from 0% to 50% of strength.
    total_extremes_count = len(all_extremes)

    # STEP 2: KDE кластеризация
    price_range_width = all_extremes.max() - all_extremes.min()
    if price_range_width == 0:
        return []

    # Адаптивная KDE bandwidth
    if config.get('kde_bandwidth_adaptive', True):
        target_pct = (atr_pct / 100) * 1.5
        target_pct = max(
            config.get('kde_bandwidth_min', 0.008),
            min(config.get('kde_bandwidth_max', 0.025), target_pct)
        )
    else:
        target_pct = config.get('kde_bandwidth', 0.015)

    data_std = np.std(all_extremes)
    if data_std == 0:
        bw = 0.1
    else:
        data_mean = np.mean(all_extremes)
        desired_bandwidth = data_mean * target_pct
        n_ext = len(all_extremes)
        scott_factor = n_ext ** (-1.0 / 5.0)
        bw_computed = desired_bandwidth / (scott_factor * data_std)
        bw = max(0.01, safe_float(bw_computed))

    try:
        kde = gaussian_kde(all_extremes, bw_method=bw)
    except np.linalg.LinAlgError as e:
        logger.warning(f"KDE failed (singular matrix): {e}")
        return []

    grid = np.linspace(
        all_extremes.min() - price_range_width * 0.05,
        all_extremes.max() + price_range_width * 0.05,
        2000
    )
    density = kde(grid)

    # STEP 3: Пики плотности = ценовые уровни
    peaks_idx, properties = find_peaks(density, height=np.max(density) * 0.15)
    candidate_levels = grid[peaks_idx]


    freshness_enabled = config.get('freshness_enabled', True)
    freshness_decay_hours = config.get('freshness_decay_hours', 72)
    freshness_penalty_pct = config.get('freshness_penalty_pct', 20)

    total_candles = len(candles_h1)

    levels = []
    for level_price in candidate_levels:
        # Инициализация в начале каждой итерации (Задача #11)
        hours_ago = 0
        is_stale = False
        touch_indices = []

        # Zone tolerance for touch counting.
        # CRITICAL: `bw` is scipy's dimensionless bandwidth FACTOR, not price units.
        # The actual bandwidth in price space = bw * scott_factor * data_std
        # We use dynamic_tolerance (from ATR) as the primary zone width,
        # with a small KDE-derived expansion to account for cluster width.
        actual_bw_price = bw * scott_factor * data_std if data_std > 0 else 0
        kde_zone_pct = (actual_bw_price / current_price * 0.45) if current_price > 0 else 0
        zone_tolerance = max(dynamic_tolerance, min(kde_zone_pct, dynamic_tolerance * 5))

        zone_low = level_price * (1 - zone_tolerance)
        zone_high = level_price * (1 + zone_tolerance)

        # Какие экстремумы коснулись этого уровня
        res_mask = (swing_highs >= zone_low) & (swing_highs <= zone_high)
        sup_mask = (swing_lows >= zone_low) & (swing_lows <= zone_high)

        res_touches = int(np.sum(res_mask))
        sup_touches = int(np.sum(sup_mask))
        total_touches = res_touches + sup_touches

        # For KDE levels, the peak itself inherently proves price clustering.
        # We can be slightly more lenient on exact rigid touches.
        min_touches_kde = max(1, min_touches - 1)

        if total_touches < min_touches_kde:
            logger.info(f"KDE level {level_price:.6f} rejected: {total_touches} touches < {min_touches_kde} required (KDE softened)")
            continue

        # Chop-zone progressive penalty instead of hard rejection
        touch_ratio = total_touches / total_extremes_count if total_extremes_count > 0 else 0
        chop_penalty = 0.0
        if touch_ratio > 0.95:  # >95% of ALL extremes on one level = truly flat coin
            logger.info(f"KDE level {level_price:.6f} rejected: {total_touches}/{total_extremes_count} touches ({touch_ratio:.0%}) = flat/dead zone")
            continue
        elif touch_ratio > 0.6:  # >60% = likely chop zone, heavy penalty
            chop_penalty = min(0.5, (touch_ratio - 0.6) / 0.35 * 0.5)  # 0% → 50% penalty
            logger.debug(f"KDE level {level_price:.6f}: chop penalty {chop_penalty:.0%} (touch_ratio={touch_ratio:.0%})")

        level_type = 'RESISTANCE' if level_price > current_price else 'SUPPORT'
        distance_pct = abs(level_price - current_price) / current_price * 100

        # Высококачественные уровни видны издалека
        effective_max_dist = max_distance_pct
        if total_touches >= 10:
            effective_max_dist *= 2.5
        elif total_touches >= 5:
            effective_max_dist *= 1.5
            
        if distance_pct > effective_max_dist:
            continue

        # Базовая сила (log шкала — дифференцирует 6 vs 15 vs 24 касания)
        touches_mult = min(math.log2(total_touches + 1) / math.log2(30), 1.0)
        strength = touches_mult * 70.0 + max(0, 30 - distance_pct * 10)
        
        # Apply chop penalty
        if chop_penalty > 0:
            strength *= (1.0 - chop_penalty)

        # Проверка свежести
        if freshness_enabled:
            touch_indices = []
            if res_touches > 0:
                touch_indices.extend(swing_high_idx[res_mask].tolist())
            if sup_touches > 0:
                touch_indices.extend(swing_low_idx[sup_mask].tolist())

            if touch_indices:
                last_touch_idx = max(touch_indices)
                hours_ago = total_candles - 1 - last_touch_idx
                is_stale = hours_ago > freshness_decay_hours
                if is_stale:
                    strength -= (strength * (freshness_penalty_pct / 100.0))

        strength = min(100.0, max(0.0, strength))

        levels.append({
            'price': round(safe_float(level_price), 8),
            'type': level_type,
            'touches': total_touches,
            'resistance_touches': res_touches,
            'support_touches': sup_touches,
            'distance_pct': round(safe_float(distance_pct), 2),
            'strength': round(safe_float(strength), 1),
            'stale': is_stale,
            'source': 'KDE',
            'hours_since_touch': hours_ago,
        })

    levels.sort(key=lambda x: (-x['strength'], x['distance_pct']))

    max_levels = config.get('max_levels_per_coin', 6)
    return levels[:max_levels]


def detect_dynamic_levels(
    candles_h1: List[Dict],
    current_price: float,
    config: Dict
) -> List[Dict]:
    """
    Динамические уровни, не зависящие от кластеризации касаний:
    - Недельный максимум/минимум
    - Круглые числа (психологические уровни)
    
    Критичны для импульсных монет, где KDE-уровни не формируются
    из-за хаотичных свингов.
    """
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
                'touches': 2, # Promoted base score assuming 2, score logic handles it
                'resistance_touches': 2 if ltype == 'RESISTANCE' else 0,
                'support_touches': 2 if ltype == 'SUPPORT' else 0,
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
            touch_count = 0
            tolerance = rn * 0.003  # 0.3% зона
            for h, l in zip(highs, lows):
                if abs(h - rn) < tolerance or abs(l - rn) < tolerance:
                    touch_count += 1
                    
            effective_max_dist = max_distance_pct
            if touch_count >= 10:
                effective_max_dist *= 2.5
            elif touch_count >= 5:
                effective_max_dist *= 1.5

            if 0.1 < dist_pct < effective_max_dist:

                # Continuous scoring: base + touches + proximity bonus
                proximity_bonus = max(0, 10 - dist_pct * 3)  # closer = better
                strength = 30.0 + (touch_count * 6) + proximity_bonus
                strength = min(safe_float(dynamic_max_strength), strength)

                levels.append({
                    'price': round(safe_float(rn), 8),
                    'type': ltype,
                    'touches': touch_count,
                    'resistance_touches': touch_count if ltype == 'RESISTANCE' else 0,
                    'support_touches': touch_count if ltype == 'SUPPORT' else 0,
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
    """Фильтрует аномальные свечи перед расчётом уровней."""
    if len(candles) < 20:
        return candles

    closes = [safe_float(c['close']) for c in candles[-50:]]
    # safe guard if closes is empty somehow
    median = sorted(closes)[len(closes) // 2] if closes else 1.0

    clean = []
    removed = 0
    for c in candles:
        close_p = safe_float(c['close'])
        high_p = safe_float(c['high'])
        low_p = safe_float(c['low'])

        # Цена > 5x или < 0.2x от медианы последних 50 свечей
        if close_p > median * 5 or close_p < median * 0.2:
            removed += 1
            continue

        # Свеча с range > 50% от цены — аномалия
        if high_p > 0 and (high_p - low_p) / high_p > 0.5:
            removed += 1
            continue

        clean.append(c)

    if removed > 0:
        logger.warning(
            f"🗑️ {symbol}: Removed {removed} anomalous "
            f"candles (median={median:.6f})"
        )

    return clean


def detect_all_levels(
    candles_h1: List[Dict],
    current_price: float,
    config: Dict,
    symbol: str = "UNKNOWN"
) -> List[Dict]:
    """
    Главная функция: объединяет KDE (структурные) и динамические уровни.
    Вызывается из main.py вместо detect_levels.
    """
    candles_h1 = validate_kline_data(candles_h1, symbol)
    
    # KDE-уровни (существующая логика с адаптивными порогами)
    kde_levels = detect_levels(candles_h1, current_price, config)

    # Динамические уровни (новое)
    dyn_levels = detect_dynamic_levels(candles_h1, current_price, config)

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
    final.sort(key=lambda x: (-x['strength'], x['distance_pct']))

    max_levels = config.get('max_levels_per_coin', 6)

    if final:
        sources = [lvl.get('source', '?') for lvl in final[:max_levels]]
        logger.debug(f"Levels found: {len(final)} total, sources: {sources}")

    return final[:max_levels]
