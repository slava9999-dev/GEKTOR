import aiohttp
import asyncio
import os
import sqlite3
import time
from datetime import datetime, timezone
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# Configuration
DB_PATH = "data_run/history.db"
BYBIT_REST_URL = "https://api.bybit.com"

def init_db():
    """Initializes the SQLite database for storing historical data."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS klines (
            symbol TEXT,
            interval TEXT,
            timestamp INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            turnover REAL,
            PRIMARY KEY (symbol, interval, timestamp)
        )
    ''')
    conn.commit()
    return conn

async def download_klines(symbol: str, interval: str, start_time_ms: int, end_time_ms: int, session: aiohttp.ClientSession, conn: sqlite3.connect):
    """
    Downloads historical klines from Bybit via pagination and saves to SQLite.
    interval: 1,3,5,15,30,60,120,240,360,720,D,M,W
    """
    limit = 1000  # Bybit max limit per request
    current_end = end_time_ms
    total_downloaded = 0
    proxy_url = os.getenv("PROXY_URL")

    cursor = conn.cursor()

    while current_end > start_time_ms:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "start": start_time_ms,
            "end": current_end,
            "limit": limit
        }

        try:
            logger.info(f"Downloading {symbol} {interval}m | Target end time: {datetime.fromtimestamp(current_end/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")
            async with session.get(f"{BYBIT_REST_URL}/v5/market/kline", params=params, proxy=proxy_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        klines = data.get("result", {}).get("list", [])
                        if not klines:
                            logger.info(f"No more data for {symbol} before {datetime.fromtimestamp(current_end/1000, tz=timezone.utc)}")
                            break
                        
                        records_to_insert = []
                        oldest_ts_in_batch = current_end
                        
                        for kline in klines:
                            ts = int(kline[0])
                            # kline format: [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
                            records_to_insert.append((
                                symbol, interval, ts, 
                                float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), 
                                float(kline[5]), float(kline[6])
                            ))
                            if ts < oldest_ts_in_batch:
                                oldest_ts_in_batch = ts
                                
                        cursor.executemany('''
                            INSERT OR IGNORE INTO klines (symbol, interval, timestamp, open, high, low, close, volume, turnover)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', records_to_insert)
                        conn.commit()
                        
                        total_downloaded += len(records_to_insert)
                        logger.info(f"Saved {len(records_to_insert)} candles. Total so far: {total_downloaded}")
                        
                        # Set next end_time to the oldest timestamp we just got - 1 millisecond
                        if oldest_ts_in_batch >= current_end:
                            logger.warning("Timestamp did not decrease! Breaking to avoid infinite loop.")
                            break
                        current_end = oldest_ts_in_batch - 1
                    else:
                        logger.error(f"Bybit API Error: {data.get('retMsg')}")
                        break
                elif resp.status == 429: # Rate limit
                    logger.warning("Rate limit hit, sleeping for 5 seconds...")
                    await asyncio.sleep(5)
                    continue
                else:
                    logger.error(f"HTTP Error {resp.status}")
                    break
        except Exception as e:
            logger.error(f"Request failed: {e}")
            await asyncio.sleep(2)
            
        # Small delay to not spam the API too much
        await asyncio.sleep(0.1)

    logger.success(f"✅ Finished downloading {total_downloaded} candles for {symbol} ({interval}m).")

async def get_coins_in_play(session: aiohttp.ClientSession, limit: int = 20) -> list[str]:
    """
    Получает список самых горячих 'монет в игре' (Top by Turnover) с Bybit.
    """
    proxy_url = os.getenv("PROXY_URL")
    url = f"{BYBIT_REST_URL}/v5/market/tickers"
    params = {"category": "linear"}
    
    logger.info(f"🔍 Fetching top {limit} coins in play (by 24h turnover)...")
    try:
        async with session.get(url, params=params, proxy=proxy_url) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("retCode") == 0:
                    tickers = data.get("result", {}).get("list", [])
                    # Сортируем по turnover24h по убыванию
                    tickers.sort(key=lambda x: float(x.get("turnover24h", 0)), reverse=True)
                    # Исключаем стейблкоины типа USDCUSDT если есть, и берем топ-limit
                    hot_coins = []
                    for t in tickers:
                        symbol = t.get("symbol", "")
                        if symbol.endswith("USDT") and symbol not in ["USDCUSDT", "BUSDUSDT", "TUSDUSDT", "DAIUSDT"]:
                            hot_coins.append(symbol)
                        if len(hot_coins) >= limit + 1: # +1 in case BTC is included and we want altcoins
                            break
                    logger.info(f"🔥 Coins in play detected: {hot_coins[:limit]}")
                    return hot_coins[:limit]
            logger.error("Failed to fetch tickers.")
    except Exception as e:
        logger.error(f"Error fetching top coins: {e}")
        
    # Дефолтный фолбэк
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"]

async def main():
    conn = init_db()
    
    async with aiohttp.ClientSession() as session:
        # 📌 Настройки загрузки
        # Загружаем топ-20 монет "в игре"
        symbols_to_download = await get_coins_in_play(session, limit=20)
        
        timeframe = "15" # 15 минутные свечи
        
        # Загружаем данные за последние Х дней
        days_to_download = 30
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (days_to_download * 24 * 60 * 60 * 1000)

        logger.info(f"Starting historical data downloader. Days: {days_to_download}, TF: {timeframe}m")

        for symbol in symbols_to_download:
            logger.info(f"--- Starting {symbol} ---")
            await download_klines(symbol, timeframe, start_ms, now_ms, session, conn)

    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
