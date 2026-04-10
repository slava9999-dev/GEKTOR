# scripts/clock_check.py
import time
import asyncio
import aiohttp
from loguru import logger

async def check_drift():
    url = "https://api.bybit.com/v5/market/time"
    logger.info("🕒 Checking clock sync with Bybit API...")
    
    async with aiohttp.ClientSession() as session:
        start_local = time.time() * 1000
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                end_local = time.time() * 1000
                
                # Check structure
                if data.get('retCode') != 0:
                    logger.error(f"Bybit API Error: {data}")
                    return

                res = data.get('result', {})
                exchange_ts = int(res.get('timeNano', 0)) // 1_000_000
                
                if exchange_ts == 0:
                    # Fallback to root 'time'
                    exchange_ts = data.get('time', 0)

                point_local = (start_local + end_local) / 2
                drift = point_local - exchange_ts
                rtt = end_local - start_local
                
                logger.info(f"📊 RTT (Latency): {rtt:.1f}ms")
                logger.info(f"📊 Server Time:   {int(point_local)}")
                logger.info(f"📊 Bybit Time:    {exchange_ts}")
                
                if abs(drift) > 500:
                    logger.critical(f"🚨 CLOCK DRIFT DETECTED: {drift:.1f}ms")
                    logger.info("👉 Run in PowerShell (Admin): w32tm /resync")
                else:
                    logger.success(f"⚖️ Clock Drift: {drift:.1f}ms (Within limits)")
                    
        except Exception as e:
            logger.error(f"❌ Failed to reach Bybit: {e}")

if __name__ == "__main__":
    asyncio.run(check_drift())
