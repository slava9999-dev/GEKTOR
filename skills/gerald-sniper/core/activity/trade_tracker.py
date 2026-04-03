from collections import defaultdict, deque
from datetime import datetime
from typing import Dict
from .models import TradeEvent, MarketActivity


class TradeTracker:
    """
    Tracks micro-trade events for velocity/intensity calculation.
    Matches Digash logic for coin activity filtering.
    """
    MAX_TRADES = 5000
    WINDOW_MINUTES = 35

    def __init__(self):
        # symbol -> deque
        self._store: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.MAX_TRADES)
        )

    def record(self, event: TradeEvent) -> None:
        """Adds a new trade event to the store."""
        self._store[event.symbol].append(event)

    def get_activity(
        self,
        symbol: str,
        atr_pct: float
    ) -> MarketActivity:
        """
        Calculates Digash-style market activity metrics for a symbol.
        """
        trades = list(self._store.get(symbol, []))
        return MarketActivity.compute(
            symbol, trades, atr_pct
        )

    def cleanup(self) -> None:
        """Prunes trades older than WINDOW_MINUTES."""
        now_ts = datetime.utcnow().timestamp()
        cutoff = now_ts - self.WINDOW_MINUTES * 60
        
        for trades in self._store.values():
            while (trades
                   and trades[0].timestamp.timestamp()
                   < cutoff):
                trades.popleft()

# Global Instance
trade_tracker = TradeTracker()
