import math
from utils.safe_math import safe_float
from core.neural import neural_processor, cyber_sentiment

async def calculate_final_score(
    sym_data: dict, 
    level: dict,
    trigger: dict,
    btc_ctx: dict,
    config: dict
) -> tuple[int, dict]:
    """
    NEURAL SCORING ENGINE v2.0
    Returns (score, breakdown) where score is 0-100 and breakdown contains ML context.
    """
    breakdown = {}
    radar = sym_data.get('radar')
    symbol = radar.symbol if radar else sym_data.get('symbol', 'UNKNOWN')
    
    direction = trigger.get('direction')
    if not direction:
        direction = 'LONG' if 'LONG' in trigger.get('pattern', '') or level.get('type', '') == 'RESISTANCE' else 'SHORT'

    # Retrieve current baseline market sentiment
    from core.realtime.sentiment import market_sentiment
    current_market_sent = market_sentiment.score if not market_sentiment.is_stale else 0
    
    # 1. Base Neural Confidence (0.0 - 1.0)
    trigger_type = trigger.get('type')
    base_confidence = neural_processor.predict_signal_probability(sym_data, current_market_sent, trigger_type)
    breakdown['neural_base'] = round(base_confidence * 100, 1)
    
    # 2. Cyber Sentiment Fusion Engine
    sentiment_vector = await cyber_sentiment.get_market_sentiment_vector(symbol, current_market_sent)
    breakdown['sentiment_bullish'] = round(sentiment_vector['bullish_probability'] * 100, 1)
    breakdown['sentiment_bearish'] = round(sentiment_vector['bearish_probability'] * 100, 1)
    breakdown['smart_money'] = round(sentiment_vector['smart_money_flow'] * 100, 1)
    
    if direction == "LONG":
        confidence_multiplier = sentiment_vector['bullish_probability']
    else:
        confidence_multiplier = sentiment_vector['bearish_probability']
        
    volatility_multiplier = 1.0 + (sentiment_vector['volatility_expectation'] * 0.5)
    smart_money_multiplier = sentiment_vector['smart_money_flow']
    
    # Sentiment Augmented Signal Logic
    final_confidence = (base_confidence * 
                        confidence_multiplier * 
                        volatility_multiplier * 
                        smart_money_multiplier)
                        
    # Aggressive approach: If sentiment is incredibly strong, override technicals
    if sentiment_vector['bullish_probability'] > 0.85 and direction == 'LONG':
        final_confidence = max(final_confidence, 0.95)
        breakdown['override'] = "EXTREME_BULL_SENTIMENT"
    elif sentiment_vector['bearish_probability'] > 0.85 and direction == 'SHORT':
        final_confidence = max(final_confidence, 0.95)
        breakdown['override'] = "EXTREME_BEAR_SENTIMENT"
        
    # Scale back to 0-100 for legacy pipeline compatibility
    total = int(max(0, min(100, round(final_confidence * 100))))
    breakdown['final_score'] = total
    
    return total, breakdown
