import asyncio
import os
import sys
from loguru import logger
from dotenv import load_dotenv

# Add skills/gerald-sniper to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Add root folder to path (for Gerald AI Router & Telegram core)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from data.bybit_rest import BybitREST
from data.bybit_ws import BybitWSManager
from data.database import DatabaseManager
from utils.config import config

from core.radar import RadarScanner
from core.level_detector import detect_all_levels
from core.candle_manager import CandleManager
from core.ai_analyst import analyst
from core.telegram_tracker import TelegramOracle
from utils.telegram_bot import send_telegram_alert


def model_to_dict(obj) -> dict:
    """Pydantic v1/v2 compatible dict conversion. Eliminates 6+ repeated blocks."""
    if hasattr(obj, 'model_dump'):
        return obj.model_dump()
    if hasattr(obj, 'dict'):
        return obj.dict()
    return vars(obj)

async def radar_loop(rest_client: BybitREST, ws_manager: BybitWSManager, db_manager: DatabaseManager, candle_mgr: CandleManager):
    """Periodically fetches all tickers, ranks them, and updates WS subscriptions."""
    scanner = RadarScanner(rest_client, db_manager, config)
    _last_top3: list[str] = []  # Anti-spam: track top-3 to avoid duplicate AI insights
    
    while True:
        try:
            if not rest_client.is_healthy:
                logger.info("⏸️ Radar scan skipped — API circuit breaker open. Waiting for recovery...")
                await asyncio.sleep(config.radar.interval_minutes * 60)
                continue
            
            logger.info("📡 Executing Radar Scan...")
            top_results = await scanner.scan()
            
            if top_results:
                top_symbols = [r["metrics"].symbol for r in top_results]
                scores = [f"{r['metrics'].symbol}:{r['score']:.1f}" for r in top_results[:5]]
                logger.info(f"🏆 Top Hot Coins! Top 5 by Score: {', '.join(scores)}")
                
                # Update Hot Sectors for macro BTC context tracking
                hot_sector_coins = []
                sectors_cfg = getattr(config, 'sectors', None)
                if sectors_cfg and getattr(sectors_cfg, 'enabled', False):
                    min_c = getattr(sectors_cfg, 'min_coins_for_sector_momentum', 3)
                    defs = getattr(sectors_cfg, 'definitions', {})
                    for s_name, s_coins in defs.items():
                        intersection = [sym for sym in top_symbols if sym in s_coins]
                        if len(intersection) >= min_c:
                            hot_sector_coins.extend(s_coins)
                candle_mgr.btc_ctx['hot_sector_coins'] = list(set(hot_sector_coins))
                
                # AI Market Analysis (only if top-3 changed to avoid spam)
                current_top3 = [r["metrics"].symbol for r in top_results[:3]]
                if current_top3 != _last_top3:
                    _last_top3 = current_top3
                    insight = await analyst.analyze_hot_coins(top_results)
                    if insight:
                        from utils.telegram_bot import send_telegram_alert
                        asyncio.create_task(send_telegram_alert(insight))
                else:
                    logger.debug(f"AI Insight skipped: top-3 unchanged ({current_top3})")
                    
                # Update WS
                await ws_manager.update_subscriptions(top_symbols)
                
                # Setup CandleManager configs for new symbols
                for sym in top_symbols:
                    if sym not in candle_mgr.data:
                        asyncio.create_task(candle_mgr.init_symbol(sym))
                
                candle_mgr.cleanup_stale_symbols(top_symbols)
                
                # BTC context is now updated via dedicated btc_context_loop
                
                # Calculate Levels in background so it doesn't block Radar interval
                asyncio.create_task(run_levels_for_watchlist(rest_client, top_results, db_manager, candle_mgr))
                
            else:
                logger.warning("No hot coins found in radar scan. Market might be dead or thresholds too high.")
                
        except Exception as e:
            logger.error(f"Radar loop error: {e}")
            
        await asyncio.sleep(config.radar.interval_minutes * 60)

_last_logged_levels = {}

async def run_levels_for_watchlist(rest: BybitREST, radar_results: list[dict], db: DatabaseManager, candle_mgr: CandleManager):
    """Calculates KDE levels for the current watchlist using H1 klines (168 candles)."""
    logger.info("🔍 Calculating mathematical levels for the watchlist...")
    levels_found = 0
    symbols = [r["metrics"].symbol for r in radar_results]
    radar_map = {r["metrics"].symbol: r["metrics"] for r in radar_results}
    
    for i, symbol in enumerate(symbols):
        try:
            # Staggered start to avoid immediate rate limit strikes on the first run
            if i > 0 and i % 5 == 0:
                await asyncio.sleep(0.5)

            klines = await rest.get_klines(symbol, "60", limit=config.levels.candles_lookback)
            if len(klines) < 50:
                continue
                
            # Convert bybit response [start, open, high, low, close]
            formatted_klines = []
            for k in klines:
                formatted_klines.append({
                    "open_time": k[0],
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5])
                })
                
            current_price = formatted_klines[-1]['close']
            # Support both Pydantic v1 and v2 for dumping to dict
            level_cfg = model_to_dict(config.levels)
                
            levels = detect_all_levels(formatted_klines, current_price, level_cfg)
            
            # --- SOURCE-DIVERSE FILTER ---
            # Show top-3 with guaranteed KDE diversity when available.
            # Prevents ROUND_NUMBER from always hiding KDE (audit finding:
            # KDE with 22 touches hidden behind ROUND with 7 due to proximity scoring)
            levels = [l for l in levels if l['strength'] >= 50]
            levels = sorted(levels, key=lambda x: -x['strength'])
            
            kde_levels = [l for l in levels if 'KDE' in l.get('source', '')]
            other_levels = [l for l in levels if 'KDE' not in l.get('source', '')]
            
            # Best KDE first, then fill with others, max 3 total
            final_levels = []
            if kde_levels:
                final_levels.append(kde_levels[0])
            for l in other_levels:
                if len(final_levels) >= 3:
                    break
                final_levels.append(l)
            # If no KDE, just take top-3 overall
            if not final_levels:
                final_levels = levels[:3]
            levels = sorted(final_levels, key=lambda x: -x['strength'])[:3]
            
            if levels:
                levels_found += len(levels)
                top_level = levels[0]
                
                # Log top level for the coin (NO Telegram spam — triggers will send alerts)
                level_key = f"{symbol}_{round(top_level['price'], 6)}"
                last_score = _last_logged_levels.get(level_key, 0)
                current_score = top_level['strength']
                
                if current_score >= 40:
                    if abs(current_score - last_score) >= 5:
                        logger.info(
                            f"🎯 {symbol} Level: {top_level['type']} @ {top_level['price']} "
                            f"(Score: {top_level['strength']}, Source: {top_level.get('source', 'KDE')}, "
                            f"Touches: {top_level['touches']})"
                        )
                        _last_logged_levels[level_key] = current_score
                else:
                    logger.debug(f"Weak level skipped in logs: {symbol} @ {top_level['price']} (Score: {current_score})")
                
                # Wire levels to our real-time CandleManager for triggering!
                candle_mgr.update_levels(symbol, levels, radar_metrics=radar_map.get(symbol))
                
                # Save levels to db
                asyncio.create_task(db.insert_detected_levels(symbol, "H1", levels))
                
        except Exception as e:
            logger.error(f"Error calculating levels for {symbol}: {e}")
            
        await asyncio.sleep(0.5) # rate limit prevention
        
    logger.info(f"✅ Level calculation complete. Total {levels_found} actionable levels found.")

async def main():
    from utils.log_filter import SensitiveFilter
    log_filter = SensitiveFilter()
    logger.remove()  # Убираем дефолтный handler
    logger.add(sys.stderr, filter=log_filter, level="INFO")
    logger.add("logs/sniper.log", filter=log_filter, rotation="10 MB", retention="14 days", level="INFO")
    logger.info("🚀 Gerald Sniper STARTING - SPRINT 1")

    # DB Match to config
    db_path = config.storage.db_path if hasattr(config, "storage") else "./data_run/sniper.db"
    db_manager = DatabaseManager(db_path)
    await db_manager.initialize()

    rest_client = BybitREST(
        base_url=config.bybit.rest_url,
        fallback_url=config.bybit.rest_url_fallback,
    )
    # Ручная установка параметров, так как __init__ не принимает их
    rest_client._fallback_switch_threshold = getattr(config.bybit, 'fallback_switch_after_failures', 3)
    rest_client._primary_recheck_interval = getattr(config.bybit, 'fallback_primary_recheck_sec', 600)
    ws_manager = BybitWSManager(ws_url=config.bybit.ws_url, ws_url_fallback=config.bybit.ws_url_fallback)

    # Hook WS data to CandleManager
    candle_mgr = CandleManager(rest_client, db=db_manager)
    await candle_mgr.initialize()
    
    async def btc_context_loop(candle_manager):
        while True:
            if rest_client.is_healthy:
                await candle_manager.update_btc_context()
            else:
                logger.debug("⏸️ BTC context update skipped — API circuit breaker open")
            await asyncio.sleep(300)  # Every 5 minutes
            
    async def process_ws(data):
        if "topic" in data and data["topic"].startswith("kline"):
            await candle_mgr.handle_ws_message(data)
            
    ws_manager.on_message(process_ws)

    # Run tasks
    ws_task = asyncio.create_task(ws_manager.connect())
    radar_task = asyncio.create_task(radar_loop(rest_client, ws_manager, db_manager, candle_mgr))
    btc_task = asyncio.create_task(btc_context_loop(candle_mgr))

    retry_task = asyncio.create_task(candle_mgr.retry_failed_alerts_loop())

    async def db_cleanup_loop(db_mgr):
        """Runs DB cleanup once per day at 04:00 MSK (config.storage.cleanup_hour)."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        
        tz_str = config.timezone if hasattr(config, 'timezone') else 'Europe/Moscow'
        tz = ZoneInfo(tz_str)
        cleanup_hour = config.storage.cleanup_hour if hasattr(config.storage, 'cleanup_hour') else 4
        
        while True:
            try:
                from datetime import datetime as dt
                now = dt.now(tz)
                if now.hour == cleanup_hour and now.minute < 10:
                    logger.info("🧹 Starting scheduled DB cleanup...")
                    await db_mgr.cleanup_old_data(
                        watchlist_keep_days=config.storage.candle_cache_max_days if hasattr(config.storage, 'candle_cache_max_days') else 30,
                        candle_keep_days=config.storage.candle_cache_max_days if hasattr(config.storage, 'candle_cache_max_days') else 30,
                        alert_keep_days=config.storage.alert_history_max_days if hasattr(config.storage, 'alert_history_max_days') else 365,
                    )
                    # Sleep 1 hour to avoid re-running in the same hour
                    await asyncio.sleep(3600)
                else:
                    await asyncio.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.error(f"DB cleanup error: {e}")
                await asyncio.sleep(600)
    
    cleanup_task = asyncio.create_task(db_cleanup_loop(db_manager))

    async def weekly_report_loop(db_mgr):
        """Runs the weekly performance report generation algorithm once a week."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        tz_str = config.timezone if hasattr(config, 'timezone') else 'Europe/Moscow'
        tz = ZoneInfo(tz_str)
        
        while True:
            try:
                from datetime import datetime as dt
                now = dt.now(tz)
                # Sunday = 6. 20:00 (or config time)
                if now.weekday() == 6 and now.hour == 20 and now.minute < 10:
                    logger.info("📊 Generating weekly report...")
                    report = await db_mgr.get_weekly_summary()
                    await send_telegram_alert(report)
                    await asyncio.sleep(3600)  # Avoid running multiple times per hour
                else:
                    await asyncio.sleep(300) # Check every 5 mins
            except Exception as e:
                logger.error(f"Weekly report error: {e}")
                await asyncio.sleep(600)

    weekly_report_task = asyncio.create_task(weekly_report_loop(db_manager))

    # Initialize Telegram Oracle (only if sniper has its OWN bot token)
    # If SNIPER_BOT_TOKEN == GERALD_BOT_TOKEN, bridge_v2 is already polling
    # and will handle /win, /loss, /stats commands directly.
    sniper_token = os.getenv("SNIPER_BOT_TOKEN", "")
    gerald_token = os.getenv("GERALD_BOT_TOKEN", "")
    
    oracle_task = None
    if sniper_token and sniper_token != gerald_token:
        tg_oracle = TelegramOracle(db=db_manager)
        oracle_task = asyncio.create_task(tg_oracle.start())
    else:
        logger.info(
            "📡 Telegram Oracle SKIPPED: same token as bridge_v2. "
            "Commands (/win, /loss, /stats) are handled by bridge_v2."
        )
        tg_oracle = None

    tasks = [ws_task, radar_task, btc_task, retry_task, cleanup_task, weekly_report_task]
    if oracle_task:
        tasks.append(oracle_task)
    try:
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        logger.info("Shutting down gracefully... Canceling tasks.")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if tg_oracle is not None:
            try:
                await tg_oracle.stop()
            except Exception:
                pass  # Oracle may not have started polling
        await ws_manager.disconnect()
        await rest_client.close()
        logger.info("✅ Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
