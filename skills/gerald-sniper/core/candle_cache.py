import asyncio
import json
from loguru import logger
import time
import datetime
from typing import Dict, List, Any
from data.bybit_rest import BybitREST
from utils.config import config
from utils.safe_math import safe_float
from core.level_store import LevelStore


class CandleCache:
    """Хранит стриминговые свечи, btc-контекст и вызывает TriggerPipeline при закрытии свечей."""
    
    def __init__(self, rest_client: BybitREST, trigger_pipeline, db=None):
        self.rest = rest_client
        self.trigger_pipeline = trigger_pipeline
        self.db = db
        
        # symbol -> { 'm5': list, 'm15': list, 'levels': list, 'last_alert': dict, 'radar': CoinRadarMetrics | None }
        self.data: Dict[str, Dict[str, Any]] = {}
        
        self.btc_ctx = {}
        try:
            self.btc_symbol = config.macro_filter.btc_symbol
        except AttributeError:
            self.btc_symbol = "BTCUSDT"
            
        self.level_store = LevelStore()
            
    async def initialize(self):
        """Pre-load historical tracking states from the pipeline."""
        await self.trigger_pipeline.initialize()

    async def update_btc_context(self):
        """
        Вызывается из главного цикла каждые 5 минут.
        Рассчитывает тренд BTC на основе H1 свечей.
        """
        try:
            klines = await self.rest.get_klines("BTCUSDT", "60", limit=6)
            if not klines or len(klines) < 5:
                logger.warning("BTC context: not enough klines")
                return
            
            closes = [safe_float(k[4]) for k in klines]
            current = closes[-1]
            
            change_1h = ((current - closes[-2]) / closes[-2]) * 100 if closes[-2] > 0 else 0
            
            if len(closes) >= 5:
                change_4h = ((current - closes[-5]) / closes[-5]) * 100 if closes[-5] > 0 else 0
            else:
                change_4h = change_1h
            
            if change_4h > 0.5 or (change_4h > 0.2 and change_1h > 0.15):
                trend = 'STRONG_UP' if change_4h > 2.0 else 'UP'
            elif change_4h < -0.5 or (change_4h < -0.2 and change_1h < -0.15):
                trend = 'STRONG_DOWN' if change_4h < -2.0 else 'DOWN'
            else:
                trend = 'FLAT'
            
            self.btc_ctx.update({
                'trend': trend,
                'change_4h': round(change_4h, 2),
                'change_1h': round(change_1h, 2),
                'last_update': time.time(),
            })
            
            # v5.0: Update centralized MarketSentiment
            from core.realtime.sentiment import market_sentiment
            market_sentiment.update(self.btc_ctx)
            
            logger.info(
                f"📊 BTC context: {trend} | "
                f"1h: {change_1h:+.1f}% | 4h: {change_4h:+.1f}%"
            )
            
        except Exception as e:
            logger.error(f"BTC context update failed: {e}")

    async def init_symbol(self, symbol: str):
        """Fetch historical M5 and M15 candles lazily."""
        if symbol not in self.data:
            self.data[symbol] = {
                'm5': [],
                'm15': [],
                'levels': [],
                'last_alert': {},
                'radar': None
            }
            
        try:
            klines_5 = await self.rest.get_klines(symbol, "5", limit=60)
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
                    if self.data[symbol]['last_alert']:
                        logger.debug(f"Loaded {len(self.data[symbol]['last_alert'])} active cooldowns for {symbol} from DB")
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
        return self.level_store.get_triggerable_levels(symbol)

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
                "radar": None,
            }

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

        window = trs[-period:]
        return sum(window) / len(window)

    def get_atr_abs(self, symbol: str, interval: str = "5"):
        """
        Returns ATR in ABSOLUTE price units for requested symbol/interval.
        """
        if symbol not in self.data:
            return None
        return self.data[symbol].get(f"atr_abs_m{interval}")

    def update_closed_candle(self, candle) -> None:
        """
        Accept ONLY finalized closed candle objects.
        """
        symbol = candle.symbol
        interval = candle.interval
        
        self._ensure_symbol_state(symbol)
            
        target_list = self.data[symbol]['m5'] if interval == "5" else self.data[symbol]['m15']
        
        updated_candle = {
            "open_time": str(candle.start_ts),
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "confirmed": True,
        }
        
        if not target_list:
            target_list.append(updated_candle)
        else:
            last_candle = target_list[-1]
            if last_candle['open_time'] == updated_candle['open_time']:
                target_list[-1] = updated_candle
            else:
                target_list.append(updated_candle)
                if len(target_list) > 100:
                    target_list.pop(0)
                    
        atr_abs = self._compute_atr_abs_from_candles(target_list)
        self.data[symbol][f"atr_abs_m{interval}"] = atr_abs

    async def handle_ws_message(self, msg: dict):
        if 'topic' not in msg or 'data' not in msg:
            return
            
        topic = msg['topic']
        parts = topic.split('.')
        
        if not topic.startswith('kline'):
            return
            
        interval = parts[1]
        symbol = parts[2]
        
        if symbol not in self.data:
            return

        if interval not in ("5", "15"):
            return
            
        candle_closed = False
            
        for k in msg['data']:
            start = str(k['start'])
            
            is_confirmed = k.get('confirm', None)
            
            target_list = self.data[symbol]['m5'] if interval == "5" else self.data[symbol]['m15']
            
            if is_confirmed is None:
                if target_list and target_list[-1]['open_time'] != start:
                    is_confirmed = True
                else:
                    is_confirmed = False

            if is_confirmed:
                candle_closed = True
                
            updated_candle = {
                "open_time": start,
                "open": safe_float(k['open']),
                "high": safe_float(k['high']),
                "low": safe_float(k['low']),
                "close": safe_float(k['close']),
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

        if candle_closed and (interval == "5" or interval == "15"):
            logger.debug(f"✅ Candle closed: {symbol} {interval}m | close={updated_candle['close']}")
            
            if interval == "5":
                self.level_store.increment_candles_lived(symbol)
                
            atr_pct = getattr(self.data[symbol].get('radar', None), 'atr_pct', 2.0)
            self.level_store.update_symbol(symbol, updated_candle['close'], atr_pct)
            
            triggerable_levels = self.level_store.get_triggerable_levels(symbol, updated_candle['close'], atr_pct)
            self.data[symbol]['levels'] = triggerable_levels
            
            await self.trigger_pipeline.check_triggers(symbol, self.data[symbol], self.btc_ctx)
            
        if interval == "5":
            await self.trigger_pipeline.tracker.check_prices({symbol: updated_candle['close']})
