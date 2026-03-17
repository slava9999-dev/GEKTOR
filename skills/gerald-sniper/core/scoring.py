from core.radar import CoinRadarMetrics
from utils.safe_math import safe_float

def calculate_final_score(
    radar, # Could be CoinRadarMetrics or RadarV2Metrics
    level: dict,
    trigger: dict,
    btc_ctx: dict,
    config: dict
) -> tuple[int, dict]:
    """
    Returns (score, breakdown) where breakdown is a dictionary with details.
    """
    breakdown = {}
    
    # Defaults from config if present
    weights = config.get('weights', {})
    modifiers = config.get('modifiers', {})
    
    w_level = weights.get('level_quality', 30)
    w_fuel = weights.get('fuel', 30)
    w_pattern = weights.get('pattern', 25)
    w_macro = weights.get('macro', 15)
    
    # Модификатор по источнику уровня (V4.0)
    source = str(level.get('source', 'KDE'))
    
    # Для динамических уровней ограничиваем халявные касания
    if 'WEEK_EXTREME' in source:
        effective_touches = safe_float(level.get('touches', 1))
        source_multiplier = 0.85   # 85% — сильный экстрим, но не кластерный KDE
    elif 'ROUND_NUMBER' in source:
        # ROUND_NUMBER (V4.2): Allow them to be useful but not dominant
        effective_touches = 1.5 
        source_multiplier = 0.6 # Reduced penalty (was 0.4)
    else:
        # Standard KDE (V4.2)
        effective_touches = safe_float(level.get('touches', 1))
        source_multiplier = 1.0
        
    # Bonus for Confluence (Multi-Source or Cluster)
    if '+' in source or level.get('confluence_score', 0) >= 2:
        effective_touches = max(4.0, effective_touches) # Floor for clusters
        source_multiplier = 1.25   # 125% — Verified Cluster

    import math
    # Логарифмическая шкала решает проблему насыщения скоринга.
    # Ранее touches=6 и touches=21 давали одинаковый балл, теперь 20+ штук дают больше
    touches_score = min(math.log2(effective_touches + 1) / math.log2(25), 1.0) * (w_level * 0.6)
    proximity_score = max(0, 1.0 - (level['distance_pct'] / 4.0)) * (w_level * 0.4)
    base_level_score = touches_score + proximity_score

    breakdown['level'] = round(base_level_score * source_multiplier, 1)
    
    # --- B. Fuel: Volume + OI ---
    # Radar v2 uses volume_spike as rvol. delta_oi is currently 0 in v2.
    current_rvol = getattr(radar, 'rvol', getattr(radar, 'volume_spike', 1.0))
    current_oi = getattr(radar, 'delta_oi_4h_pct', 0.0)
    
    rvol_score = min(current_rvol / 5.0, 1.0) * (w_fuel * 0.5)
    oi_score = min(current_oi / 20.0, 1.0) * (w_fuel * 0.5)
    breakdown['fuel'] = round(rvol_score + oi_score, 1)
    
    # --- C. Pattern Quality ---
    pattern_score = 10
    if trigger.get('squeeze_bars') is not None:  # Squeeze Fire — highest priority
        # Squeeze with 4+ bars = strong base score, more bars = higher score
        sqz_bars = trigger['squeeze_bars']
        pattern_score = min(sqz_bars / 8.0, 1.0) * w_pattern * 0.7 + w_pattern * 0.3
    elif trigger.get('r_squared') is not None:  # Compression
        pattern_score = trigger['r_squared'] * (w_pattern * 0.6)
        pattern_score += min(trigger.get('atr_contraction_pct', 0) / 30, 1.0) * (w_pattern * 0.4)
    elif trigger.get('volume_ratio') is not None:  # Volume spike / Breakout
        pattern_score = min(trigger['volume_ratio'] / 5.0, 1.0) * w_pattern
        
    breakdown['pattern'] = round(pattern_score, 1)
    
    # --- D. Macro Context ---
    direction = trigger.get('direction')
    if not direction:
        direction = 'LONG' if 'LONG' in trigger['pattern'] or level['type'] == 'RESISTANCE' else 'SHORT'
    
    macro_adj = 0
    # v5.1: Use centralized MarketSentiment for macro adjustment
    try:
        from core.realtime.sentiment import market_sentiment
        if not market_sentiment.is_stale:
            macro_adj = market_sentiment.macro_score_adjustment(direction, max_points=w_macro)
        else:
            # Fallback to old logic if sentiment data is stale
            if direction == 'LONG':
                if btc_ctx.get('trend') in ('STRONG_DOWN', 'DOWN'):
                    macro_adj = -w_macro
                elif btc_ctx.get('trend') == 'STRONG_UP':
                    macro_adj = w_macro * 0.7
            elif direction == 'SHORT':
                if btc_ctx.get('trend') in ('STRONG_UP', 'UP'):
                    macro_adj = -w_macro
                elif btc_ctx.get('trend') == 'STRONG_DOWN':
                    macro_adj = w_macro * 0.7
    except ImportError:
        # Fallback if module not available
        if direction == 'LONG':
            if btc_ctx.get('trend') in ('STRONG_DOWN', 'DOWN'):
                macro_adj = -w_macro
            elif btc_ctx.get('trend') == 'STRONG_UP':
                macro_adj = w_macro * 0.7
        elif direction == 'SHORT':
            if btc_ctx.get('trend') in ('STRONG_UP', 'UP'):
                macro_adj = -w_macro
            elif btc_ctx.get('trend') == 'STRONG_DOWN':
                macro_adj = w_macro * 0.7
            
    breakdown['macro'] = round(macro_adj, 1)
    
    # --- E. Modifiers (V4.0) ---
    mods_total = 0
    
    # Sector momentum
    sector_momentum_active = radar.symbol in btc_ctx.get('hot_sector_coins', [])
    if sector_momentum_active:
        mods_total += modifiers.get('sector_momentum_bonus', 5)
        
    # Multitrigger bonus
    if trigger.get('multi_bonus'):
        mods_total += trigger['multi_bonus']
        
    # Stale level penalty
    if level.get('stale', False):
        mods_total += modifiers.get('stale_level_penalty', -10)
        
    # Level Age adjustments
    age = level.get('age', 1)
    if age < 3: # younger than 15 mins
        mods_total += modifiers.get('young_level_penalty', -10)
    elif age > 6: # older than 30 mins
        mods_total += modifiers.get('mature_level_bonus', 5)
        
    # Liquidation bonus
    if trigger.get('liquidation_cascade'):
        mods_total += modifiers.get('liquidation_cascade_bonus', 10)
        
    # Funding adjustments
    if direction == 'LONG':
        if radar.funding_rate > 0.0005:
            mods_total += modifiers.get('adverse_funding_penalty', -8)
        elif radar.funding_rate < -0.0001:
            mods_total += modifiers.get('favorable_funding_bonus', 5)
    elif direction == 'SHORT':
        if radar.funding_rate < -0.0001:
            mods_total += modifiers.get('adverse_funding_penalty', -8)
        elif radar.funding_rate > 0.0001:
            mods_total += modifiers.get('favorable_funding_bonus', 5)
            
    breakdown['modifiers'] = mods_total
    
    # --- TOTAL ---
    total = breakdown['level'] + breakdown['fuel'] + breakdown['pattern'] + breakdown['macro'] + breakdown['modifiers']
    total = int(max(0, min(100, round(total))))
    
    return total, breakdown
