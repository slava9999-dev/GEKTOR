import asyncio
import json
from loguru import logger
import time
import datetime
from collections import deque
from typing import Dict, List, Any, Optional
from data.bybit_rest import BybitREST
from utils.config import config
from utils.safe_math import safe_float
from core.level_store import LevelStore


class CandleCache:
    """Хранит стриминговые свечи, btc-контекст и вызывает TriggerPipeline при закрытии свечей."""
    
    def __init__(self, rest_client: BybitREST, db=None):
        self.rest = rest_client
        self.db = db
        
        # symbol -> { 'm5': list, 'm15': list, 'levels': list, 'last_alert': dict, 'radar': CoinRadarMetrics | None }
        self.data: Dict[str, Dict[str, Any]] = {}
        
        self.btc_ctx = {}
        try:
            self.btc_symbol = config.macro_filter.btc_symbol
        except AttributeError:
            self.btc_symbol = "BTCUSDT"
            
        self.level_store = LevelStore()
        self.price_filters: Dict[str, Any] = {}
            
    async def initialize(self):
        """ Gerald v6.0: Initial states now handled via DB and local stores. """
        from core.events.nerve_center import bus
        from core.events.events import RawWSEvent
        bus.subscribe(RawWSEvent, self.handle_raw_ws_event)
        logger.info("🕯️ [CandleCache] Initialized & Subscribed to EventBus")

    async def handle_raw_ws_event(self, event: Any):
        """Map RawWSEvent to internal handle_ws_message (Rule 6.2)"""
        msg = {
            "topic": event.topic,
            "data": event.data,
            "u": event.u
        }
        await self.handle_ws_message(msg)

    async def update_btc_context(self):
        """
        Вызывается из главного цикла каждые 5 минут.
        Рассчитывает тренд BTC на основе H1 свечей.
        """
        try:
            klines = await self.rest.get_klines(self.btc_symbol, "60", limit=10)
            if not klines: return
            
            # Simple H1 slope
            closes = [float(k[4]) for k in klines]
            delta = (closes[-1] - closes[0]) / closes[0] * 100
            
            trend = "FLAT"
            if delta > 0.5: trend = "BULLISH"
            elif delta < -0.5: trend = "BEARISH"
            
            self.btc_ctx = {"trend": trend, "delta": delta}
            logger.debug(f"BTC context updated: {trend} ({delta:.2f}%)")
        except Exception as e:
            logger.error(f"Failed to update BTC context: {e}")

    def get_candle_history(self, symbol: str, interval: str = "5") -> List[Dict]:
        if symbol not in self.data: return []
        return self.data[symbol].get(f"m{interval}", [])

    async def init_symbol(self, symbol: str):
        """Pre-warm candle cache for a new symbol."""
        if symbol in self.data: return
        
        self._ensure_symbol_state(symbol)
        
        try:
            klines_5 = await self.rest.get_klines(symbol, "5", limit=30)
            formatted_5 = []
            for k in klines_5:
                formatted_5.append({
                    "open_time": k[0],
                    "open": safe_float(k[1]),
                    "high": safe_float(k[2]),
                    "low": safe_float(k[3]),
                    "close": safe_float(k[4]),
                    "volume": safe_float(k[5])
                })
            self.data[symbol]['m5'] = formatted_5
            
            # Seed ATR history
            atr = self._compute_atr_abs_from_candles(formatted_5)
            if atr:
                 self.data[symbol]['atr_history_m5'].append(atr)
                 self.data[symbol]['atr_abs_m5'] = atr

            await asyncio.sleep(0.5)
            
            klines_15 = await self.rest.get_klines(symbol, "15", limit=30)
            formatted_15 = []
            for k in klines_15:
                formatted_15.append({
                    "open_time": k[0],
                    "open": safe_float(k[1]),
                    "high": safe_float(k[2]),
                    "low": safe_float(k[3]),
                    "close": safe_float(k[4]),
                    "volume": safe_float(k[5])
                })
            self.data[symbol]['m15'] = formatted_15
            
            if self.db:
                try:
                    recent = await self.db.get_symbol_alert_history(symbol, limit=10)
                    now_ts = time.time()
                    cooldown_sec = getattr(config.alerts, 'cooldown_hours', 4) * 3600
                    for alt in recent:
                        try:
                            dt = datetime.datetime.fromisoformat(alt['timestamp'].replace('Z', '+00:00'))
                            ts = dt.timestamp()
                            if now_ts - ts < cooldown_sec:
                                level_id = f"{symbol}_{alt['direction']}"
                                self.data[symbol]['last_alert'][level_id] = ts
                        except Exception:
                            pass
                except Exception as e:
                    logger.error(f"Failed to load DB cooldown history for {symbol}: {e}")
            
        except Exception as e:
            logger.error(f"Failed to init historical candles for {symbol}: {e}")

    def update_levels(self, symbol: str, levels: list, radar_metrics=None):
        if symbol not in self.data:
            return
            
        if radar_metrics:
            self.data[symbol]['radar'] = radar_metrics
            
        for level in levels:
            self.level_store.add_or_merge_level(
                symbol=symbol,
                price=level["price"],
                source=level.get("source", "UNKNOWN"),
                touches=level.get("touches", 1),
                score=level.get("score", level.get("strength", 50.0)),
                level_id=level.get("level_id", "")
            )

    def update_level_store(self, symbol: str, current_price: float, atr_abs: float) -> None:
        self.level_store.update_symbol(symbol, current_price, atr_abs)

    def get_triggerable_levels(self, symbol: str):
        return self.level_store.get_triggerable_levels(symbol, 0.0, 0.0) # Dummy placeholder for signature

    def cleanup_stale_symbols(self, active_symbols: list[str]):
        """Removes symbols that are no longer in the active watchlist to prevent memory leaks."""
        stale = [sym for sym in self.data if sym not in active_symbols]
        for sym in stale:
            del self.data[sym]
            logger.info(f"👁 Proximity monitor stopped: {sym}")

    def _ensure_symbol_state(self, symbol: str) -> None:
        if symbol not in self.data:
            self.data[symbol] = {
                "m5": [],
                "m15": [],
                "levels": [],
                "last_alert": {},
                "radar": None,
                "atr_history_m5": deque(maxlen=30),
                "bba": {"bid": 0.0, "ask": 0.0} # Task 9.1: Best Bid/Ask
            }

    def get_bba(self, symbol: str) -> Dict[str, float]:
        """Returns the last known Best Bid and Best Ask (Task 9.1)."""
        if symbol not in self.data:
            return {"bid": 0.0, "ask": 0.0}
        return self.data[symbol].get("bba", {"bid": 0.0, "ask": 0.0})

    def _compute_atr_abs_from_candles(self, candles: list, period: int = 14):
        if len(candles) < period + 1:
            return None

        trs = []
        prev_close = float(candles[0]["close"])
        for c in candles[1:]:
            high = float(c["high"])
            low = float(c["low"])
            close = float(c["close"])
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
            trs.append(tr)
            prev_close = close

        if len(trs) < period:
            return None

        return sum(trs[-period:]) / period

    def get_atr_abs(self, symbol: str, interval: str = "5"):
        if symbol not in self.data:
            return None
        return self.data[symbol].get(f"atr_abs_m{interval}")

    def get_atr_sma(self, symbol: str, period: int = 14) -> Optional[float]:
        """Returns the SMA of historical ATR values for the m5 interval."""
        if symbol not in self.data:
            return None
        history = self.data[symbol].get('atr_history_m5', [])
        if not history:
            return None
        window = list(history)[-period:]
        if len(window) < 5: 
            return None
        return sum(window) / len(window)

    def get_bba(self, symbol: str) -> Dict[str, float]:
        """Returns current Best Bid/Ask from memory (Task 9.1)."""
        if symbol in self.data:
            return self.data[symbol].get('bba', {"bid": 0.0, "ask": 0.0})
        return {"bid": 0.0, "ask": 0.0}

    async def handle_ws_message(self, msg: dict):
        """
        Processes real-time kline and ticker messages (Rule 5 & Task 9.1).
        """
        from core.events.nerve_center import bus
        from core.events.events import PriceUpdateEvent, DetectorEvent
        
        if 'topic' not in msg or 'data' not in msg:
            return
            
        topic = msg['topic']
        data = msg['data']
        symbol = data.get('symbol') if isinstance(data, dict) else (data[0].get('symbol') if isinstance(data, list) and data else None)
        
        # 1. Staleness Guard (Audit 15.5)
        # Block data delayed > 500ms
        ts = msg.get('ts')
        if ts:
            delay_ms = int(time.time() * 1000) - int(ts)
            if delay_ms > 500:
                logger.warning(f"⚠️ [STALENESS] Data Age: {delay_ms}ms on {topic}. Ignoring.")
                return

        # 2. Outlier (Fat Finger) Filter (Audit 15.9)
        # Prevents 99% drops from triggering false signals
        if topic.startswith('tickers') or topic.startswith('kline'):
            # Price extraction with list/dict safety (Audit 16.44)
            p_raw = None
            if isinstance(data, dict):
                p_raw = data.get('lastPrice') or data.get('close')
            elif isinstance(data, list) and data:
                p_raw = data[0].get('close') or data[0].get('lastPrice')

            if p_raw and symbol:
                if not self.price_filters.get(symbol):
                    from core.shield.outlier_filter import OutlierFilter
                    self.price_filters[symbol] = OutlierFilter()
                
                # [Task 2] Adaptive Volatility Logic: extract vola from Radar if present
                radar = self.data.get(symbol, {}).get('radar')
                vola = getattr(radar, 'vola', 0.0) if radar else 0.0
                
                if not self.price_filters[symbol].is_price_sane(symbol, float(p_raw), volatility=vola):
                    return # Block anomaly!
        
        # 3. Tickers Stream for BBA (Rule 9.1)
        if topic.startswith('tickers'):
            if symbol and symbol in self.data and isinstance(data, dict):
                self.data[symbol]['bba'] = {
                    "bid": float(data.get('bid1Price', 0.0)) or self.data[symbol]['bba']['bid'],
                    "ask": float(data.get('ask1Price', 0.0)) or self.data[symbol]['bba']['ask']
                }
            return

        parts = topic.split('.')
        if not topic.startswith('kline'): return

            
        interval = parts[1]
        symbol = parts[2]
        if symbol not in self.data: return
        if interval not in ("5", "15"): return
            
        candle_closed = False
        last_price = 0.0
            
        for k in msg['data']:
            start = str(k['start'])
            last_price = safe_float(k['close'])
            is_confirmed = k.get('confirm', False)

            if is_confirmed:
                candle_closed = True
                
            target_list = self.data[symbol]['m5'] if interval == "5" else self.data[symbol]['m15']
            
            updated_candle = {
                "open_time": start,
                "open": safe_float(k['open']),
                "high": safe_float(k['high']),
                "low": safe_float(k['low']),
                "close": last_price,
                "volume": safe_float(k['volume']),
                "confirmed": is_confirmed,
            }
            
            if not target_list:
                target_list.append(updated_candle)
            else:
                last_candle = target_list[-1]
                if last_candle['open_time'] == start:
                    target_list[-1] = updated_candle
                else:
                    target_list.append(updated_candle)
                    if len(target_list) > 100:
                        target_list.pop(0)

        # 1. Broadest update: Publish price for Trackers (Rule 2.3)
        asyncio.create_task(bus.publish(PriceUpdateEvent(prices={symbol: last_price})))

        if candle_closed:
            # Update ATR
            target_list = self.data[symbol]['m5'] if interval == "5" else self.data[symbol]['m15']
            atr_val = self._compute_atr_abs_from_candles(target_list)
            self.data[symbol][f"atr_abs_m{interval}"] = atr_val
            
            if interval == "5" and atr_val:
                # Update ATR History for dynamic regime (Task 4.2)
                self.data[symbol]['atr_history_m5'].append(atr_val)
                logger.debug(f"📐 [ATR] {symbol} m5 Updated: {atr_val:.4f}")
                
            if interval == "5":
                radar = self.data[symbol].get('radar')
                atr_pct = getattr(radar, 'atr_pct', 2.0) if radar else 2.0
                tier = getattr(radar, 'tier', 'C') if radar else 'C'
                
                self.level_store.update_symbol(symbol, last_price, atr_pct)
                lvl_hits = self.level_store.get_triggerable_levels(symbol, last_price, atr_pct)
                
                if lvl_hits:
                    for lvl in lvl_hits:
                        asyncio.create_task(bus.publish(DetectorEvent(
                            symbol=symbol,
                            detector="LEVEL_PROXIMITY",
                            direction="LONG" if last_price > lvl['price'] else "SHORT",
                            price=last_price,
                            liquidity_tier=tier,
                            payload={
                                "level_price": lvl['price'],
                                "dist_pct": lvl.get('dist', 0.1),
                                "radar_score": getattr(radar, 'score', 0) if radar else 0
                            }
                        )))
