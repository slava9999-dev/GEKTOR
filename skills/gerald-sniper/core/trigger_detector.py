"""
Gerald Sniper — Trigger Detector v5.0
=====================================
Detects the BEGINNING of large moves via:
  1. Squeeze Fire  — BB inside Keltner → release with momentum = big move starts
  2. Volume Explosion — 2x+ volume with strong body near key level
  3. Breakout — Level break with volume + body confirmation
  4. Compression — Price tightening toward a level (R² + ATR contraction)
"""
import numpy as np
import pandas as pd
from scipy.stats import linregress
from utils.safe_math import safe_float


# ═══════════════════════════════════════════════════════════════
# 1. SQUEEZE FIRE  — The #1 signal for catching large move starts
# ═══════════════════════════════════════════════════════════════
def detect_squeeze_fire(
    candles_m15: list[dict],     # Last 30+ candles M15
    level: dict | None = None,   # Optional — squeeze works without a level too
    config: dict | None = None
) -> dict | None:
    """
    BB Squeeze = Bollinger Bands trade inside Keltner Channels.
    When squeeze releases and momentum has clear direction → FIRE.
    
    This is the industry-standard signal for the beginning of explosive moves.
    Proven by TTM Squeeze (John Carter) and widely used in institutional trading.
    """
    config = config or {}
    min_candles = config.get('min_candles', 25)
    if len(candles_m15) < min_candles:
        return None

    df = pd.DataFrame(candles_m15)
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)

    bb_length = config.get('bb_length', 20)
    bb_std = config.get('bb_std', 2.0)
    kc_length = config.get('kc_length', 20)
    kc_scalar = config.get('kc_scalar', 1.5)

    # --- Bollinger Bands ---
    sma = df['close'].rolling(bb_length).mean()
    std = df['close'].rolling(bb_length).std()
    bb_upper = sma + bb_std * std
    bb_lower = sma - bb_std * std

    # --- Keltner Channels ---
    tr = pd.DataFrame({
        'hl': df['high'] - df['low'],
        'hc': abs(df['high'] - df['close'].shift(1)),
        'lc': abs(df['low'] - df['close'].shift(1))
    }).max(axis=1)
    atr = tr.rolling(kc_length).mean()
    kc_mid = df['close'].rolling(kc_length).mean()
    kc_upper = kc_mid + kc_scalar * atr
    kc_lower = kc_mid - kc_scalar * atr

    # --- Squeeze state: BB inside KC = compressed ---
    squeeze_on = (bb_lower > kc_lower) & (bb_upper < kc_upper)

    # Must have enough computed values
    if squeeze_on.isna().iloc[-1]:
        return None

    # Current candle must be squeeze OFF (just fired)
    if squeeze_on.iloc[-1]:
        return None

    # Count consecutive squeeze bars BEFORE the fire
    squeeze_count = 0
    for i in range(len(squeeze_on) - 2, -1, -1):
        if squeeze_on.iloc[i]:
            squeeze_count += 1
        else:
            break

    min_squeeze_bars = config.get('min_squeeze_bars', 4)
    if squeeze_count < min_squeeze_bars:
        return None

    # --- Momentum direction (close relative to SMA) ---
    momentum = df['close'] - sma
    cur_mom = safe_float(momentum.iloc[-1])
    prev_mom = safe_float(momentum.iloc[-2])

    if cur_mom > 0 and cur_mom > prev_mom:
        direction = 'LONG'
    elif cur_mom < 0 and cur_mom < prev_mom:
        direction = 'SHORT'
    else:
        return None  # No clear momentum → skip

    # --- Volume confirmation: last candle should have above-avg volume ---
    vol_lookback = config.get('vol_lookback', 20)
    if len(df) > vol_lookback:
        avg_vol = df['volume'].iloc[-vol_lookback - 1:-1].mean()
        cur_vol = safe_float(df['volume'].iloc[-1])
        if avg_vol > 0 and cur_vol < avg_vol * config.get('min_vol_ratio_on_fire', 0.8):
            return None  # Squeeze fired on weak volume → fakeout risk

    # --- Distance to level (if provided, used for stop/target calc) ---
    if level:
        current_price = safe_float(candles_m15[-1]['close'])
        distance = abs(current_price - level['price']) / current_price * 100
        max_dist = config.get('max_distance_to_level_pct', 3.0)
        if distance > max_dist:
            return None

    return {
        'pattern': f'SQUEEZE_FIRE_{direction}',
        'direction': direction,
        'description': f"🔥 Squeeze FIRE ({squeeze_count} bar сжатие) → {direction}!",
        'squeeze_bars': squeeze_count,
        'momentum': round(cur_mom, 6),
    }


# ═══════════════════════════════════════════════════════════════
# 2. VOLUME EXPLOSION — Institutional entry near key level
# ═══════════════════════════════════════════════════════════════
def detect_volume_explosion(
    candles_m5: list[dict],       # Last 60 candles M5
    level: dict,
    config: dict
) -> dict | None:
    """
    Detects explosive volume bar near a key level with strong body (not doji).
    This catches the moment when institutions push through a level.
    
    Wider distance than old volume_spike (1% vs 0.3%) to not miss setups.
    """
    lookback = config.get('avg_volume_lookback', 50)
    if len(candles_m5) <= lookback:
        return None

    current = candles_m5[-1]
    c_price = safe_float(current['close'])
    c_open = safe_float(current['open'])
    c_high = safe_float(current['high'])
    c_low = safe_float(current['low'])
    c_vol = safe_float(current['volume'])

    # Distance to level
    distance = abs(c_price - level['price']) / c_price * 100
    if distance > config.get('max_distance_to_level_pct', 1.0):
        return None

    # Volume check
    avg_volume = np.mean([safe_float(c['volume']) for c in candles_m5[-lookback - 1:-1]])
    if avg_volume == 0:
        return None

    volume_ratio = c_vol / avg_volume
    if volume_ratio < config.get('min_volume_ratio', 2.0):
        return None

    # Body must be strong (filter dojis / hammers)
    total_range = c_high - c_low
    if total_range == 0:
        return None
    body = abs(c_price - c_open)
    body_ratio = body / total_range
    if body_ratio < config.get('min_body_ratio', 0.4):
        return None

    # Direction from candle + level alignment
    is_bullish = c_price > c_open
    is_res = level['type'] == 'RESISTANCE'

    if is_res and is_bullish:
        direction = 'LONG'
    elif not is_res and not is_bullish:
        direction = 'SHORT'
    else:
        return None  # Candle direction conflicts with level type

    return {
        'pattern': 'VOLUME_EXPLOSION',
        'direction': direction,
        'description': f"💥 Объём {volume_ratio:.1f}x (body {body_ratio:.0%}) у {level['price']}",
        'volume_ratio': round(safe_float(volume_ratio), 1),
        'body_ratio': round(safe_float(body_ratio), 2),
    }


# ═══════════════════════════════════════════════════════════════
# 3. BREAKOUT — Level break with volume + body confirmation
# ═══════════════════════════════════════════════════════════════
def detect_breakout(
    candles_m5: list[dict],
    level: dict,
    config: dict
) -> dict | None:
    """
    Breakout = close beyond level + volume above average + strong body.
    Previous candle must have been on the other side (fresh break, not continuation).
    
    Softened from v4: volume 1.5x (was 2.0x), body 0.4 (was 0.5).
    """
    if not config.get('enabled', True):
        return None

    lookback_vol = 50
    if len(candles_m5) <= lookback_vol:
        return None

    current = candles_m5[-1]
    prev = candles_m5[-2]

    c_close = safe_float(current['close'])
    c_open = safe_float(current['open'])
    c_high = safe_float(current['high'])
    c_low = safe_float(current['low'])
    c_vol = safe_float(current['volume'])

    p_close = safe_float(prev['close'])

    is_res = level['type'] == 'RESISTANCE'
    l_price = level['price']

    # 1. Current close beyond level
    if is_res and c_close <= l_price:
        return None
    if not is_res and c_close >= l_price:
        return None

    # 2. Previous close on the other side (fresh breakout)
    if is_res and p_close > l_price:
        return None
    if not is_res and p_close < l_price:
        return None

    # 3. Volume confirmation
    avg_vol = np.mean([safe_float(c['volume']) for c in candles_m5[-lookback_vol - 1:-1]])
    if avg_vol == 0:
        return None

    vol_ratio = c_vol / avg_vol
    if vol_ratio < config.get('min_breakout_volume_ratio', 1.5):
        return None

    # 4. Body ratio (reject dojis)
    high_low_range = c_high - c_low
    body_range = abs(c_close - c_open)
    if high_low_range > 0:
        body_ratio = body_range / high_low_range
        if body_ratio < config.get('min_body_ratio', 0.4):
            return None
    else:
        return None

    direction = 'LONG' if is_res else 'SHORT'

    return {
        'pattern': f"BREAKOUT_{direction}",
        'direction': direction,
        'description': f"⚡ Пробой ({vol_ratio:.1f}x объём) уровня {l_price}!",
        'volume_ratio': round(safe_float(vol_ratio), 1),
    }


# ═══════════════════════════════════════════════════════════════
# 4. COMPRESSION — Price tightening toward a level
# ═══════════════════════════════════════════════════════════════
def detect_compression(
    candles_m15: list[dict],     # Last 30 candles M15
    level: dict,
    config: dict
) -> dict | None:
    """
    Detects linear price tightening toward a S/R level.
    Lows rising toward resistance, or highs dropping toward support.
    
    Softened from v4: R² 0.35 (was 0.5), distance 1.5% (was 0.8%), contraction 12% (was 15%).
    """
    lookback = config.get('lookback_candles', 20)
    if len(candles_m15) < lookback:
        return None

    lows = np.array([safe_float(c['low']) for c in candles_m15[-lookback:]])
    highs = np.array([safe_float(c['high']) for c in candles_m15[-lookback:]])
    closes = np.array([safe_float(c['close']) for c in candles_m15[-lookback:]])
    x = np.arange(lookback)
    current_price = closes[-1]

    distance_to_level = abs(current_price - level['price']) / current_price * 100

    if distance_to_level > config.get('max_distance_to_level_pct', 1.5):
        return None

    is_resistance = level['type'] == 'RESISTANCE'

    # Anti-false compression: price shouldn't have crossed the level recently
    if config.get('anti_false_compression', True):
        look = config.get('anti_false_lookback_candles', 10)
        recent_closes = closes[-look:]
        if is_resistance:
            if any(c > level['price'] for c in recent_closes):
                return None
        else:
            if any(c < level['price'] for c in recent_closes):
                return None

    y_vals = lows if is_resistance else highs
    slope, intercept, r_value, _, _ = linregress(x, y_vals)

    if (is_resistance and slope <= 0) or (not is_resistance and slope >= 0):
        return None

    if r_value ** 2 < config.get('min_r_squared', 0.35):
        return None

    half = lookback // 2
    atr_first_half = np.mean(highs[:half] - lows[:half])
    atr_second_half = np.mean(highs[half:] - lows[half:])

    if atr_first_half == 0:
        return None

    contraction_pct = (1 - atr_second_half / atr_first_half) * 100
    min_contraction = config.get('min_atr_contraction_pct', 12.0)

    if contraction_pct < min_contraction:
        return None

    direction = 'LONG' if is_resistance else 'SHORT'
    pattern_desc = 'Поджатие к сопротивлению' if is_resistance else 'Поджатие к поддержке'

    return {
        'pattern': f'COMPRESSION_{direction}',
        'direction': direction,
        'description': f"{pattern_desc} {level['price']}, ATR -{int(contraction_pct)}%",
        'slope': round(safe_float(slope), 8),
        'r_squared': round(safe_float(r_value ** 2), 2),
        'atr_contraction_pct': round(safe_float(contraction_pct), 1),
    }


# ═══════════════════════════════════════════════════════════════
# Legacy alias — keep backward compatibility
# ═══════════════════════════════════════════════════════════════
detect_volume_spike_at_level = detect_volume_explosion
