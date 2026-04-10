import asyncio
import os
import sys
from pathlib import Path

# Fix path to load env
PROJECT_ROOT = Path(__file__).resolve().parents[2]
env_path = PROJECT_ROOT / '.env'
from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path, override=True)

from utils.config import config
import aiohttp

async def check_proxy_connection():
    bot_token = config.telegram.bot_token
    chat_id = config.telegram.chat_id
    
    print(f"[CHECK] Token loaded: {len(bot_token) if bot_token else 0} chars")
    print(f"[CHECK] Chat ID loaded: {chat_id}")
    
    if not bot_token:
        print("❌ Error: Bot token empty!")
        return

    proxy = getattr(config.telegram, "proxy_url", None) or os.getenv("PROXY_URL")
    if proxy and not proxy.startswith(("http://", "https://")):
        proxy = f"http://{proxy}"
    
    print(f"[CHECK] Using proxy: {proxy}")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "🧪 <b>Diagnostics Check</b>: Proxy and Network are functioning correctly.",
        "parse_mode": "HTML"
    }

    print("\n[CHECK] Sending request to Telegram...")
    try:
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.post(url, json=payload, proxy=proxy, timeout=10) as resp:
                print(f"[CHECK] HTTP Status: {resp.status}")
                text = await resp.text()
                print(f"[CHECK] Response Body: {text}")
                if resp.status == 200:
                    print("✅ SUCCESS: Telegram API reachable via proxy!")
                else:
                    print("❌ FAILED: Received non-200 status code.")
    except Exception as e:
        print(f"❌ FATAL ERROR: {type(e).__name__}: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(check_proxy_connection())
