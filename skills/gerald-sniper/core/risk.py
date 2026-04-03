from typing import Dict, Any
from utils.safe_math import safe_float

def calculate_stop_and_target(
    sym_data: dict, symbol: str, level: dict, direction: str, config: Any, score: int = 50, bd: dict = None
) -> Dict[str, float]:
    """
    Рассчитывает стоп, цель и размер позиции на основе реального ATR монеты.
    """
    # Fallback: если нет данных M15
    if not sym_data or len(sym_data.get('m15', [])) < 14:
        return {
            'stop_pct': 1.5,
            'stop_price': 0.0,
            'target_price': 0.0,
            'position_size': 0.0,
            'atr_value': 0.0,
            'rr_ratio': 2.0,
            'calculable': False,
        }
    
    # Считаем ATR(14) на M15
    candles = sym_data['m15'][-14:]
    ranges = [safe_float(c['high']) - safe_float(c['low']) for c in candles]
    atr = sum(ranges) / len(ranges)
    
    current_price = safe_float(sym_data['m15'][-1]['close'])
    if current_price == 0:
        return {
            'stop_pct': 1.5, 'stop_price': 0.0, 'target_price': 0.0,
            'position_size': 0.0, 'atr_value': 0.0, 'rr_ratio': 2.0, 'calculable': False
        }
    
    # Стоп = уровень ± (ATR × multiplier)
    atr_stop_multiplier = getattr(config.risk, 'atr_stop_multiplier', 0.5) if hasattr(config, 'risk') else 0.5
    stop_distance = atr * atr_stop_multiplier
    stop_pct = (stop_distance / current_price) * 100
    
    # Клэмп: минимум 0.3%, максимум max_stop_pct из конфига
    max_stop = getattr(config.risk, 'max_stop_pct', 3.0) if hasattr(config, 'risk') else 3.0
    stop_pct = max(0.3, min(max_stop, stop_pct))
    stop_distance = current_price * (stop_pct / 100)
    
    rr = getattr(config.risk, 'default_rr_ratio', 2.0) if hasattr(config, 'risk') else 2.0
    
    # FIXED: Stop/target based on entry price, not level price.
    # Level-based stop was inaccurate when price drifted far from level.
    if direction == 'LONG':
        stop_price = current_price - stop_distance
        target_price = current_price + (stop_distance * rr)
    else:
        stop_price = current_price + stop_distance
        target_price = current_price - (stop_distance * rr)
    
    # Агрессивное масштабирование позиции (Risk scaling)
    from core.neural import aggressive_risk
    bd = bd or {}
    neural_confidence = score / 100.0
    sentiment_alignment = bd.get('sentiment_bullish', 50)/100 if direction == 'LONG' else bd.get('sentiment_bearish', 50)/100
    risk_usd_base = getattr(config.risk, 'risk_per_trade_usd', 20) if hasattr(config, 'risk') else 20
    
    # [HIVE MIND] Extract swarm state from breakdown
    swarm_state = "NONE"
    if bd.get('pattern', '').startswith('SWARM_SYNC'):
        swarm_state = "SYNC"
    elif bd.get('pattern', '').startswith('SWARM_CONFLICT'):
        swarm_state = "CONFLICT"
        # Для скальпа при конфликте ставим очень короткий стоп и быстрый тейк
        stop_pct *= 0.5
        stop_distance = current_price * (stop_pct / 100)
        rr = 1.2
        if direction == 'LONG':
            stop_price = current_price - stop_distance
            target_price = current_price + (stop_distance * rr)
        else:
            stop_price = current_price + stop_distance
            target_price = current_price - (stop_distance * rr)
            
    volatility = getattr(sym_data.get('radar'), 'atr_pct', 2.0) if sym_data.get('radar') else 2.0
    
    final_risk_usd = aggressive_risk.calculate_position_size(
        neural_confidence=neural_confidence,
        sentiment_alignment=sentiment_alignment,
        volatility=volatility,
        base_risk_usd=risk_usd_base,
        swarm_state=swarm_state
    )
    
    position_size = final_risk_usd / (stop_pct / 100) if stop_pct > 0 else 0
    
    return {
        'stop_pct': round(stop_pct, 2),
        'stop_price': round(stop_price, 8),
        'target_price': round(target_price, 8),
        'position_size': round(position_size, 0),
        'atr_value': round(atr, 8),
        'rr_ratio': rr,
        'calculable': True,
    }
