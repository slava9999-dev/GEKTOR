from loguru import logger
from typing import Tuple

class TradeMemoryEngine:
    """
    Learning Layer from AI V2 Architecture.
    Evaluates past performance of specific symbols, directions, or level types
    to dynamically adjust the score of new signals based on historical win rates.
    """
    def __init__(self, db=None):
        self.db = db
        # simple in-memory cache to avoid hammering DB for the same pattern repeatedly
        self.cache = {}

    async def get_historical_penalty(self, symbol: str, direction: str, level_type: str = None) -> Tuple[int, str]:
        """
        Returns (penalty_score, reason)
        If the combination has a bad historical record, penalty_score is positive (amount to subtract).
        """
        if not self.db:
            return 0, ""
            
        cache_key = f"{symbol}_{direction}_{level_type}"
        # We could implement TTL on cache, but for now it's fine since we don't expect 1000s of requests
        
        try:
            # We don't have a direct "pattern performance" method in DB yet.
            # We can use get_symbol_alert_history and then analyze results.
            history = await self.db.get_symbol_alert_history(symbol, limit=20)
            if not history:
                return 0, ""
                
            match_alerts = [a for a in history if a['direction'] == direction]
            if len(match_alerts) < 3: # Need at least 3 trades of this direction to judge
                return 0, ""
                
            # Filter by result (assuming position_tracker updates status to 'WIN' or 'LOSS' or 'TIMEOUT')
            losses = [a for a in match_alerts if a.get('status') == 'LOSS']
            wins = [a for a in match_alerts if a.get('status') == 'WIN']
            
            total_resolved = len(losses) + len(wins)
            if total_resolved < 3:
                return 0, ""
                
            win_rate = len(wins) / total_resolved * 100
            
            if win_rate < 30.0 and total_resolved >= 3:
                penalty = 25
                reason = f"Исторический WinRate по {direction} всего {win_rate:.0f}% ({len(wins)}W/{len(losses)}L)"
                logger.debug(f"🧠 Trade Memory applied: -{penalty} score to {symbol} {direction} ({reason})")
                return penalty, reason
                
            return 0, ""
        except Exception as e:
            logger.error(f"Learning Layer DB query failed: {e}")
            return 0, ""

trade_memory = TradeMemoryEngine()
