from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class TradeEvent:
    symbol: str
    price: float
    qty: float
    side: str          # "Buy" | "Sell"
    timestamp: datetime


@dataclass
class MarketActivity:
    symbol: str
    trades_1m: int
    trades_5m: int
    trades_30m_avg: float
    trade_velocity: float       # trades_1m / avg_1m_of_30m
    trade_intensity: float      # trades_5m / avg_5m_of_30m
    buy_pressure: float         # buy_vol / total_vol (0–1)
    volume_1m: float
    atr_pct: float
    is_active: bool

    @classmethod
    def compute(
        cls,
        symbol: str,
        trades: List[TradeEvent],
        atr_pct: float
    ) -> "MarketActivity":
        # Using UTC for consistency across deques
        now = datetime.utcnow()

        def since(minutes: int) -> List[TradeEvent]:
            cutoff = now.timestamp() - minutes * 60
            return [
                t for t in trades
                if t.timestamp.timestamp() > cutoff
            ]

        t1m = since(1)
        t5m = since(5)
        t30m = since(30)

        trades_1m = len(t1m)
        trades_5m = len(t5m)
        avg_per_min_30m = len(t30m) / 30 if t30m else 0.01

        # Digash Formulations
        velocity = trades_1m / avg_per_min_30m
        intensity = (trades_5m / 5) / avg_per_min_30m

        buys = sum(
            t.qty * t.price for t in t1m
            if t.side == "Buy"
        )
        total = sum(t.qty * t.price for t in t1m) or 0.01
        buy_pressure = buys / total

        vol_1m = sum(t.qty * t.price for t in t1m)

        # Activity Gate: Velocity > 1.5 AND ATR > 1.5%
        is_active = (
            velocity > 1.5
            and atr_pct > 1.2 # Slightly relaxed for broader detection
        )

        return cls(
            symbol=symbol,
            trades_1m=trades_1m,
            trades_5m=trades_5m,
            trades_30m_avg=avg_per_min_30m,
            trade_velocity=round(velocity, 2),
            trade_intensity=round(intensity, 2),
            buy_pressure=round(buy_pressure, 2),
            volume_1m=vol_1m,
            atr_pct=atr_pct,
            is_active=is_active
        )
