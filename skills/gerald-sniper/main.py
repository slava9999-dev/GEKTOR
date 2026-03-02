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
from core.level_detector import detect_levels
from core.candle_manager import CandleManager
from core.ai_analyst import analyst

async def radar_loop(rest_client: BybitREST, ws_manager: BybitWSManager, db_manager: DatabaseManager, candle_mgr: CandleManager):
    """Periodically fetches all tickers, ranks them, and updates WS subscriptions."""
    scanner = RadarScanner(rest_client, db_manager, config)
    
    while True:
        try:
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
                
                # AI Market Analysis
                insight = await analyst.analyze_hot_coins(top_results)
                if insight:
                    from utils.telegram_bot import send_telegram_alert
                    asyncio.create_task(send_telegram_alert(insight))
                    
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

async def run_levels_for_watchlist(rest: BybitREST, radar_results: list[dict], db: DatabaseManager, candle_mgr: CandleManager):
    """Calculates KDE levels for the current watchlist using H1 klines (168 candles)."""
    logger.info("🔍 Calculating mathematical levels for the watchlist...")
    levels_found = 0
    symbols = [r["metrics"].symbol for r in radar_results]
    radar_map = {r["metrics"].symbol: r["metrics"] for r in radar_results}
    
    for symbol in symbols:
        try:
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
            level_cfg = {}
            if hasattr(config.levels, 'model_dump'):
                level_cfg = config.levels.model_dump()
            elif hasattr(config.levels, 'dict'):
                level_cfg = config.levels.dict()
            else:
                level_cfg = config.levels.__dict__
                
            levels = detect_levels(formatted_klines, current_price, level_cfg)
            
            if levels:
                levels_found += len(levels)
                # Log top level for the coin
                top_level = levels[0]
                logger.info(f"🎯 {symbol} Level Found: {top_level['type']} @ {top_level['price']} (Score: {top_level['strength']})")
                
                # Send to Telegram
                from utils.telegram_bot import send_telegram_alert, format_level_alert
                msg = format_level_alert(symbol, top_level)
                asyncio.create_task(send_telegram_alert(msg, disable_notification=True))
                
                # Wire levels to our real-time CandleManager for triggering!
                candle_mgr.update_levels(symbol, levels, radar_metrics=radar_map.get(symbol))
                
                # Save levels to db
                asyncio.create_task(db.insert_detected_levels(symbol, "H1", levels))
                
        except Exception as e:
            logger.error(f"Error calculating levels for {symbol}: {e}")
            
        await asyncio.sleep(0.5) # rate limit prevention
        
    logger.info(f"✅ Level calculation complete. Total {levels_found} actionable levels found.")

async def main():
    logger.add("logs/sniper.log", rotation="10 MB", retention="7 days", level="INFO")
    logger.info("🚀 Gerald Sniper STARTING - SPRINT 1")

    # DB Match to config
    db_path = config.storage.db_path if hasattr(config, "storage") else "./data_run/sniper.db"
    db_manager = DatabaseManager(db_path)
    await db_manager.initialize()

    rest_client = BybitREST(base_url=config.bybit.rest_url)
    ws_manager = BybitWSManager(ws_url=config.bybit.ws_url, ws_url_fallback=config.bybit.ws_url_fallback)

    # Hook WS data to CandleManager
    candle_mgr = CandleManager(rest_client)
    
    async def btc_context_loop(candle_manager):
        while True:
            await candle_manager.update_btc_context()
            await asyncio.sleep(300)  # Каждые 5 минут
            
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

    try:
        await asyncio.gather(ws_task, radar_task, btc_task, retry_task, cleanup_task)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down... Please wait.")
        await ws_manager.disconnect()
        await rest_client.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
