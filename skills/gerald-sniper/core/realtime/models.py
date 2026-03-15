import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from collections import deque
from utils.math_utils import EPS, safe_float

@dataclass
class SymbolLiveState:
    symbol: str
    current_price: float = 0.0
    bid_vol_usd: float = 0.0
    ask_vol_usd: float = 0.0
    
    # Bucketed data (existing)
    buckets: Dict[int, dict] = field(default_factory=dict)
    history_ts: deque = field(default_factory=lambda: deque(maxlen=300))
    
    # Liquidations history (timestamp, price, qty, side)
    liq_history: deque = field(default_factory=lambda: deque(maxlen=500))

    def get_orderbook_imbalance(self):
        return self.bid_vol_usd, self.ask_vol_usd

    def update(self, price: float, qty: float, side: str):
        now = int(datetime.utcnow().timestamp())
        self.current_price = price
        
        if now not in self.buckets:
            if len(self.history_ts) >= self.history_ts.maxlen:
                oldest = self.history_ts.popleft()
                self.buckets.pop(oldest, None)
            self.history_ts.append(now)
            self.buckets[now] = {'price': price, 'vol': 0.0, 'buy_vol': 0.0, 'trades': 0}
        
        b = self.buckets[now]
        b['price'] = price
        b['vol'] += price * qty
        if side.lower() == 'buy':
            b['buy_vol'] += price * qty
        b['trades'] += 1

    def get_price_change_pct(self, seconds: int) -> float:
        now = int(datetime.utcnow().timestamp())
        target_ts = now - seconds
        
        # Find closest bucket to target_ts
        start_price = None
        for ts in self.history_ts:
            if ts >= target_ts:
                start_price = self.buckets[ts]['price']
                break
        
        if not start_price or start_price < EPS: return 0.0
        return ((self.current_price - start_price) / max(start_price, EPS)) * 100

    def get_trade_count(self, seconds: int) -> int:
        now = int(datetime.utcnow().timestamp())
        cutoff = now - seconds
        return sum(b['trades'] for ts, b in self.buckets.items() if ts > cutoff)

    def get_volume_usd(self, seconds: int) -> float:
        now = int(datetime.utcnow().timestamp())
        cutoff = now - seconds
        return sum(b['vol'] for ts, b in self.buckets.items() if ts > cutoff)

    def get_orderflow_imbalance(self, seconds: int) -> float:
        now = int(datetime.utcnow().timestamp())
        cutoff = now - seconds
        buy_v = sum(b['buy_vol'] for ts, b in self.buckets.items() if ts > cutoff)
        total_v = sum(b['vol'] for ts, b in self.buckets.items() if ts > cutoff)
        
        sell_v = total_v - buy_v
        if sell_v < EPS: return 2.5 if buy_v > EPS else 1.0
        return buy_v / max(sell_v, EPS)

    def get_liquidation_volume(self, seconds: int) -> float:
        now = datetime.utcnow().timestamp()
        cutoff = now - seconds
        vol = 0.0
        for ts, p, q, _ in reversed(self.liq_history):
            if ts < cutoff: break
            vol += p * q
        return vol

    def get_acceleration(self) -> float:
        m1 = self.get_price_change_pct(30)
        # Previous 30s means from T-60 to T-30
        now = int(datetime.utcnow().timestamp())
        
        p_30, p_60 = None, None
        for ts in reversed(self.history_ts):
            if not p_30 and ts <= now - 30: p_30 = self.buckets[ts]['price']
            if not p_60 and ts <= now - 60: 
                p_60 = self.buckets[ts]['price']
                break
        
        if not p_30 or not p_60 or p_60 < EPS: return 0.0
        m2 = ((p_30 - p_60) / max(p_60, EPS)) * 100
        return m1 - m2

    def get_moving_average(self, seconds: int) -> float:
        now = int(time.time())
        cutoff = now - seconds
        prices = [b['price'] for ts, b in self.buckets.items() if ts > cutoff]
        if not prices: return self.current_price
        return sum(prices) / len(prices)

    def get_volatility(self, seconds: int) -> float:
        """Returns standard deviation of prices in the window."""
        now = int(time.time())
        cutoff = now - seconds
        prices = [b['price'] for ts, b in self.buckets.items() if ts > cutoff]
        if len(prices) < 2: return 0.0
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / (len(prices) - 1)
        return (variance ** 0.5) / max(mean, EPS) * 100 # In percentage
