import asyncio
import json
from loguru import logger
import time
import datetime
from typing import Dict, List, Any
from data.bybit_rest import BybitREST
import core.trigger_detector as td
from core.radar import CoinRadarMetrics
from utils.config import config
from utils.telegram_bot import send_telegram_alert
from core.scoring import calculate_final_score
from core.risk import calculate_stop_and_target
from utils.safe_math import safe_float
from core.position_tracker import PaperTracker

# No dummy GLOBAL_BTC_CTX needed as it's tracked in CandleManager

class CandleManager:
    """Manages real-time candles and triggers for active watchlist."""
    
    def __init__(self, rest_client: BybitREST, db=None):
        self.rest = rest_client
        self.db = db  # DatabaseManager для persistence алертов
        
        # symbol -> { 'm5': list, 'm15': list, 'levels': list, 'last_alert': dict, 'radar': CoinRadarMetrics | None }
        self.data: Dict[str, Dict[str, Any]] = {}
        
        self.btc_ctx = {}
        try:
            self.btc_symbol = config.macro_filter.btc_symbol
        except AttributeError:
            self.btc_symbol = "BTCUSDT"
            
        self._alerts_sent_today = 0
        self._current_date = ""
        self._alert_timestamps_for_burst = []  # Burst tracker
        
        self.tracker = PaperTracker(self.db)
        
        self._failed_alerts = []
        self._failed_alerts_lock = asyncio.Lock()

    async def initialize(self):
        """Pre-load historical tracking states."""
        await self.tracker.load_active_positions()
        
    def _check_daily_limit(self) -> bool:
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        tz_str = config.timezone if hasattr(config, 'timezone') else 'Europe/Moscow'
        
        now = datetime.datetime.now(ZoneInfo(tz_str))
        date_str = now.strftime("%Y-%m-%d")
        
        if self._current_date != date_str:
            self._current_date = date_str
            self._alerts_sent_today = 0
            self._alert_timestamps_for_burst.clear()  # New for burst tracking
            
        limit = config.alerts.max_daily_alerts
        if self._alerts_sent_today >= limit:
            logger.warning(f"Daily alert limit reached: {self._alerts_sent_today}/{limit}")
            return False
            
        # --- Burst protection during extreme macros (STRONG_DOWN / STRONG_UP) ---
        current_ts = time.time()
        # Clean up old timestamps (> 15 mins)
        if not hasattr(self, '_alert_timestamps_for_burst'):
            self._alert_timestamps_for_burst = []
        
        self._alert_timestamps_for_burst = [
            ts for ts in self._alert_timestamps_for_burst 
            if current_ts - ts < 900  # 15 minutes window
        ]
        
        btc_trend = self.btc_ctx.get('trend', 'FLAT')
        if btc_trend in ('STRONG_DOWN', 'STRONG_UP'):
            # Allow max 3 alerts per 15 minutes during extreme trends to prevent spam
            if len(self._alert_timestamps_for_burst) >= 3:
                logger.warning(f"Burst limit reached! 3 alerts already sent in last 15m during BTC {btc_trend}.")
                return False
                
        self._alert_timestamps_for_burst.append(current_ts)
        return True
        
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
            
            closes = [safe_float(k[4]) for k in klines]  # k[4] = close
            current = closes[-1]
            
            # Изменение за 1 час (текущая vs предыдущая закрытая)
            change_1h = ((current - closes[-2]) / closes[-2]) * 100 if closes[-2] > 0 else 0
            
            # Изменение за 4 часа
            if len(closes) >= 5:
                change_4h = ((current - closes[-5]) / closes[-5]) * 100 if closes[-5] > 0 else 0
            else:
                change_4h = change_1h
            
            # Определение тренда
            if change_4h > 2.0:
                trend = 'STRONG_UP'
            elif change_4h > 0.5:
                trend = 'UP'
            elif change_4h < -2.0:
                trend = 'STRONG_DOWN'
            elif change_4h < -0.5:
                trend = 'DOWN'
            else:
                trend = 'FLAT'
            
            self.btc_ctx.update({
                'trend': trend,
                'change_4h': round(change_4h, 2),
                'change_1h': round(change_1h, 2),
                'last_update': time.time(),
            })
            
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
            
            # Persistent cooldown load (Sprint 2 task)
            if self.db:
                try:
                    recent = await self.db.get_symbol_alert_history(symbol, limit=10)
                    now_ts = time.time()
                    cooldown_sec = getattr(config.alerts, 'cooldown_hours', 4) * 3600
                    for alt in recent:
                        try:
                            # Handle datetime from database
                            dt = datetime.datetime.fromisoformat(alt['timestamp'].replace('Z', '+00:00'))
                            ts = dt.timestamp()
                            if now_ts - ts < cooldown_sec:
                                level_id = f"{symbol}_{alt['level_price']}"
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
        if symbol in self.data:
            self.data[symbol]['levels'] = levels
            # We also need the radar metrics for scoring
            if radar_metrics:
                self.data[symbol]['radar'] = radar_metrics

    def cleanup_stale_symbols(self, active_symbols: list[str]):
        """Removes symbols that are no longer in the active watchlist to prevent memory leaks."""
        stale = [sym for sym in self.data if sym not in active_symbols]
        for sym in stale:
            del self.data[sym]
            logger.info(f"🧹 Cleaned up stale symbol memory: {sym}")

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

        # CRITICAL FIX: Only process M5 and M15 candles.
        # H1 (kline.60) was corrupting M15 buffer — see audit item 0.1
        if interval not in ("5", "15"):
            return
            
        candle_closed = False  # НОВОЕ: флаг закрытия свечи
            
        for k in msg['data']:
            start = str(k['start'])
            
            # НОВОЕ: Проверяем закрытие свечи
            is_confirmed = k.get('confirm', None)
            
            target_list = self.data[symbol]['m5'] if interval == "5" else self.data[symbol]['m15']
            
            # Fallback: если confirm не приходит
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
                "confirmed": is_confirmed,  # НОВОЕ: сохраняем статус
            }
            
            if not target_list:
                target_list.append(updated_candle)
            else:
                last_candle = target_list[-1]
                if last_candle['open_time'] == start:
                    target_list[-1] = updated_candle  # Обновляем текущую
                else:
                    target_list.append(updated_candle)  # Новая свеча
                    if len(target_list) > 100:
                        target_list.pop(0)

        # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Триггеры ТОЛЬКО при закрытии свечи
        if candle_closed and (interval == "5" or interval == "15"):
            logger.debug(f"✅ Candle closed: {symbol} {interval}m | close={updated_candle['close']}")
            await self._check_triggers_for_symbol(symbol)
            
        # Постоянно сканируем Paper Trades в фоне
        if interval == "5":
            await self.tracker.check_prices({symbol: updated_candle['close']})


    async def _check_triggers_for_symbol(self, symbol: str):
        sym_data = self.data.get(symbol)
        if not sym_data or not sym_data['levels']:
            return

        now = time.time()
        
        # Diagnostic log (DEBUG level — won't show in production INFO logs)
        logger.debug(
            f"🔎 Checking triggers for {symbol}: "
            f"{len(sym_data['levels'])} levels, "
            f"M5: {len(sym_data.get('m5', []))}, "
            f"M15: {len(sym_data.get('m15', []))}"
        )
        
        def _to_dict(obj):
            if hasattr(obj, 'model_dump'): return obj.model_dump()
            if hasattr(obj, 'dict'): return obj.dict()
            return vars(obj)
        
        cfg = _to_dict(config.triggers)
        scoring_cfg = _to_dict(config.scoring)
        alerts_cfg = _to_dict(config.alerts)
            
        cooldown_seconds = config.alerts.cooldown_hours * 3600

        for level in sym_data['levels']:
            level_id = f"{symbol}_{level['price']}"
            
            triggers_found = []
            
            if len(sym_data['m5']) > 50:
                t_break = td.detect_breakout(sym_data['m5'], level, cfg.get('breakout', {}))
                if t_break:
                    t_break['type'] = 'breakout'
                    triggers_found.append(t_break)
                    
                t_vol = td.detect_volume_explosion(sym_data['m5'], level, cfg.get('volume_explosion', cfg.get('volume_spike', {})))
                if t_vol:
                    t_vol['type'] = 'volume'
                    triggers_found.append(t_vol)
                    
            if len(sym_data['m15']) >= 25:
                # BB Squeeze Fire — THE signal for large move starts
                sqz_cfg = cfg.get('squeeze', {})
                if sqz_cfg.get('enabled', True):
                    t_sqz = td.detect_squeeze_fire(sym_data['m15'], level, sqz_cfg)
                    if t_sqz:
                        t_sqz['type'] = 'squeeze'
                        triggers_found.append(t_sqz)

            if len(sym_data['m15']) >= 20:
                t_comp = td.detect_compression(sym_data['m15'], level, cfg.get('compression', {}))
                if t_comp:
                    t_comp['type'] = 'compression'
                    triggers_found.append(t_comp)

            if not triggers_found:
                continue
                
            trigger = triggers_found[0]
            if len(triggers_found) > 1:
                types_str = " + ".join([t['type'] for t in triggers_found])
                trigger['description'] = f"Мульти-сигнал ({types_str}): {trigger['description']}"
                trigger['multi_bonus'] = (len(triggers_found) - 1) * 5
                
            # MACRO BLOCK LOGIC
            direction = trigger.get('direction')
            if not direction:
                direction = "LONG" if level['type'] == 'RESISTANCE' else "SHORT"
                 
            macro_cfg = {}
            if hasattr(config, 'macro_filter'):
                macro_cfg = config.macro_filter.model_dump() if hasattr(config.macro_filter, 'model_dump') else config.macro_filter.__dict__
                
            is_btc_exempt = macro_cfg.get('btc_exempt_from_macro', True) and symbol == macro_cfg.get('btc_symbol', 'BTCUSDT')
            if macro_cfg.get('enabled', False) and not is_btc_exempt:
                btc_change_4h = self.btc_ctx.get('change_4h', 0.0)
                
                if direction == 'LONG' and btc_change_4h <= -macro_cfg.get('block_alt_longs_if_btc_drop_4h_pct', 3.0):
                    logger.info(f"🚫 MACRO BLOCK: Skipping LONG on {symbol} due to BTC dropping {btc_change_4h}%")
                    continue
                    
                if direction == 'SHORT' and btc_change_4h >= macro_cfg.get('block_alt_shorts_if_btc_pump_4h_pct', 3.0):
                    logger.info(f"🚫 MACRO BLOCK: Skipping SHORT on {symbol} due to BTC pumping {btc_change_4h}%")
                    continue
                    
            # Anti-spam: check if any level within 0.5% was alerted recently (handles KDE level drift)
            on_cooldown = False
            for alerted_level_id, last_ts in sym_data['last_alert'].items():
                parts = alerted_level_id.split('_')
                if len(parts) >= 2:
                    try:
                        alerted_price = safe_float(parts[1])
                        if abs(alerted_price - level['price']) / (level['price'] or 1.0) < 0.005:
                            if now - last_ts <= cooldown_seconds:
                                on_cooldown = True
                                break
                    except ValueError:
                        pass
            
            if not on_cooldown:
                
                # Calculate final score
                score = 0
                if sym_data.get('radar'):
                    score, bd = calculate_final_score(sym_data['radar'], level, trigger, self.btc_ctx, scoring_cfg)
                else:
                    score = 50 # Default if no radar
                    bd = {}
                    
                # Filter by score
                min_score = scoring_cfg.get('min_score_for_alert', 65)
                min_score_setup = scoring_cfg.get('min_score_for_setup_notify', 50)
                
                if score < min_score_setup:
                    continue # Noise, ignore
                    
                # Handle Priority & Sizing
                priority = "normal"
                if 'priority_tiers' in alerts_cfg:
                    for tier_name, tier_cfg in alerts_cfg['priority_tiers'].items():
                        if tier_cfg['min_score'] <= score <= tier_cfg['max_score']:
                            priority = tier_name
                
                risk_data = calculate_stop_and_target(sym_data, symbol, level, direction, config)
                max_stop = getattr(config.risk, 'max_stop_pct', 3.0) if hasattr(config, 'risk') else 3.0
                if risk_data['calculable'] and risk_data['stop_pct'] > max_stop:
                    logger.info(
                        f"Skip alert {symbol}: stop {risk_data['stop_pct']}% > max {max_stop}%"
                    )
                    continue
                
                if not self._check_daily_limit():
                    continue
                    
                self._alerts_sent_today += 1
                            
                sym_data['last_alert'][level_id] = now
                msg = self._format_trigger_alert(symbol, level, trigger, score, priority, risk_data)
                
                # Persist alert to database for analytics & AI calibration
                if self.db:
                    try:
                        radar = sym_data.get('radar')
                        entry_price = safe_float(sym_data['m5'][-1]['close']) if sym_data['m5'] else 0.0
                        db_kwargs = dict(
                            symbol=symbol,
                            direction=direction,
                            signal_type=trigger['pattern'],
                            level_price=level['price'],
                            entry_price=entry_price,
                            stop_price=risk_data.get('stop_price', 0),
                            target_price=risk_data.get('target_price', 0),
                            total_score=score,
                            score_breakdown=bd if sym_data.get('radar') else {},
                            rvol=radar.rvol if radar else 0,
                            delta_oi_pct=radar.delta_oi_4h_pct if radar else 0,
                            funding_rate=radar.funding_rate if radar else 0,
                            btc_trend=self.btc_ctx.get('trend', 'UNKNOWN'),
                        )
                        
                        # FIRE AND FORGET: Prevent SQLite I/O from blocking WS loop
                        async def _bg_db_insert(kwargs):
                            try:
                                alert_id = await self.db.insert_alert(**kwargs)
                                if alert_id and risk_data and risk_data.get('calculable'):
                                    self.tracker.add_position(
                                        alert_id=alert_id, symbol=symbol, direction=direction,
                                        entry_price=entry_price, stop_price=risk_data.get('stop_price', 0),
                                        target_price=risk_data.get('target_price', 0)
                                    )
                            except Exception as e:
                                logger.error(f"Background DB insert failed: {e}")
                                
                        asyncio.create_task(_bg_db_insert(db_kwargs))
                    except Exception as e:
                        logger.error(f"Failed to schedule DB alert insert: {e}")
                logger.warning(f"🚨 TRIGGER ({priority}, {score}): {symbol} near {level['price']}!")
                
                # Quiet hours check
                mute = False
                if alerts_cfg.get('quiet_hours_enabled'):
                    try:
                        try:
                            from zoneinfo import ZoneInfo
                        except ImportError:
                            from backports.zoneinfo import ZoneInfo
                        tz_str = config.timezone if hasattr(config, 'timezone') else 'Europe/Moscow'
                        tz = ZoneInfo(tz_str)
                        now_local = datetime.datetime.now(tz)
                        
                        curr_hr = now_local.hour
                        curr_min = now_local.minute
                        current_minutes = curr_hr * 60 + curr_min
                        
                        qh = alerts_cfg.get('quiet_hours', ["02:00", "07:00"])
                        start_parts = qh[0].split(':')
                        end_parts = qh[1].split(':')
                        start_minutes = int(start_parts[0]) * 60 + int(start_parts[1])
                        end_minutes = int(end_parts[0]) * 60 + int(end_parts[1])
                        
                        if start_minutes < end_minutes:
                            # Нормальный диапазон
                            mute = start_minutes <= current_minutes < end_minutes
                        else:
                            # Перехлёст через полночь
                            mute = current_minutes >= start_minutes or current_minutes < end_minutes
                            
                        if mute:
                            logger.debug(
                                f"Quiet hours active ({qh[0]}-{qh[1]} {tz_str}). "
                                f"Alert for {symbol} suppressed."
                            )
                    except Exception as e:
                        logger.warning(f"Quiet hours check failed: {e}")
                        mute = False
                        
                asyncio.create_task(
                    self._safe_send_alert(msg, silent=(mute or priority == 'normal'), symbol=symbol)
                )

    async def _safe_send_alert(self, msg: str, silent: bool, symbol: str):
        """Отправка с обработкой ошибок и логированием."""
        try:
            await send_telegram_alert(msg, disable_notification=silent)
            logger.info(f"📨 Alert sent: {symbol} ({'silent' if silent else 'loud'})")
        except Exception as e:
            logger.error(f"❌ Telegram alert failed for {symbol}: {e}. Queuing for retry.")
            async with self._failed_alerts_lock:
                self._failed_alerts.append({
                    'msg': msg, 'silent': silent, 'symbol': symbol, 'timestamp': time.time()
                })
            
    async def retry_failed_alerts_loop(self):
        while True:
            await asyncio.sleep(60)
            # Safely snapshot and clear under lock to prevent race conditions
            async with self._failed_alerts_lock:
                to_retry = list(self._failed_alerts)
                self._failed_alerts.clear()
            
            still_failed = []
            for alert in to_retry:
                try:
                    await send_telegram_alert(alert['msg'], alert['silent'])
                    logger.info(f"📨 Retry succeeded: {alert['symbol']}")
                except Exception:
                    if time.time() - alert['timestamp'] < 3600:
                        still_failed.append(alert)  # Keep for next retry
            
            if still_failed:
                async with self._failed_alerts_lock:
                    self._failed_alerts.extend(still_failed)


    def _format_trigger_alert(self, symbol: str, level: dict, trigger: dict, score: int, priority: str, risk_data: dict | None = None) -> str:
        direction = trigger.get('direction')
        if not direction:
            direction = "LONG" if level['type'] == 'RESISTANCE' else "SHORT"
        
        # Use pre-calculated risk_data if provided, otherwise calculate
        if risk_data is None:
            sym_data = self.data.get(symbol)
            risk_data = calculate_stop_and_target(sym_data, symbol, level, direction, config)

        # Priority-based header design
        if priority == "critical":
            header = "🚨 CRITICAL SIGNAL"
            score_bar = "🟢" * min(score // 10, 10)
        elif priority == "important":
            header = "🔥 SNIPER ALERT"
            score_bar = "🟡" * min(score // 10, 10)
        else:
            header = "📋 SETUP FORMING"
            score_bar = "⚪" * min(score // 10, 10)
        
        # Pattern emoji
        if trigger['pattern'].startswith('SQUEEZE'):
            pattern_emoji = "🔥"
        elif trigger['pattern'].startswith('COMP'):
            pattern_emoji = "🧨"
        elif trigger['pattern'].startswith('BREAKOUT'):
            pattern_emoji = "🚀"
        elif trigger['pattern'].startswith('VOL'):
            pattern_emoji = "💥"
        else:
            pattern_emoji = "⚡"
            
        dir_emoji = "📈" if direction == 'LONG' else "📉"
        dir_text = "ЛОНГ" if direction == 'LONG' else "ШОРТ"
        
        msg = (
            f"<b>{header} │ {score}/100</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{dir_emoji} <b>{symbol}</b> │ {dir_text}\n"
            f"{pattern_emoji} {trigger['pattern']}\n"
            f"📝 {trigger['description']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 Уровень: <code>{level['price']}</code>\n"
        )
        
        if risk_data['calculable']:
            msg += (
                f"🛑 Стоп: <code>{risk_data['stop_price']}</code> ({risk_data['stop_pct']}%)\n"
                f"🎯 Цель: <code>{risk_data['target_price']}</code> (R:R 1:{risk_data['rr_ratio']})\n"
            )
            
            sizing_enabled = getattr(config.risk, 'position_sizing_enabled', False) if hasattr(config, 'risk') else False
            if sizing_enabled:
                risk_usd = getattr(config.risk, 'risk_per_trade_usd', 20.0) if hasattr(config, 'risk') else 20.0
                msg += f"💰 Размер: ~${risk_data['position_size']:.0f} (риск ${risk_usd})\n"
        
        msg += (
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"{score_bar}\n"
            f"<i>Gerald Sniper 🎯 │ NFA</i>"
        )
        return msg
