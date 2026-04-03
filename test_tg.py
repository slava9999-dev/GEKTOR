import os
import asyncio
from dotenv import load_dotenv
from pathlib import Path
import aiohttp

async def test_telegram_link():
    print("[Comms] Initiating Telegram Uplink Test...")
    
    # 1. Жесткая загрузка окружения
    env_path = Path(__file__).parent / '.env'
    load_dotenv(dotenv_path=env_path, override=True)
    
    # ADJUSTED to match your actual .env keys:
    # GERALD_BOT_TOKEN or BRIDGE_BOT_TOKEN
    bot_token = os.getenv("GERALD_BOT_TOKEN") 
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    proxy_url = os.getenv("PROXY_URL") # Use if direct connection fails
    
    if not bot_token or not chat_id:
        print("[CRITICAL] GERALD_BOT_TOKEN or TELEGRAM_CHAT_ID not found in .env!")
        return

    # 2. Формирование тестового P0 Alert
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "[PROTOCOL SILENCE] COMMS CHECK: TEKTON_ALPHA telemetry link is ACTIVE. Awaiting signals.",
        "parse_mode": "HTML"
    }
    
    # 3. Асинхронный выстрел
    print(f"Link Targeted Chat ID: {chat_id}")
    try:
        async with aiohttp.ClientSession() as session:
            # First attempt: Direct Connection
            try:
                async with session.post(url, json=payload, timeout=5) as resp:
                    if resp.status == 200:
                        print("[SUCCESS] Telegram Ping delivered via Direct Link. Phone should beep NOW.")
                        return
                    else:
                        error_text = await resp.text()
                        print(f"[API Warning] Direct link returned HTTP {resp.status}: {error_text}")
            except Exception as e:
                print(f"[Network Warning] Direct link failed: {e}")

            # Second attempt: Proxy Connection
            if proxy_url:
                print(f"[Proxy] Attempting connection via {proxy_url}...")
                try:
                    async with session.post(url, json=payload, proxy=proxy_url, timeout=10) as resp:
                        if resp.status == 200:
                            print("[SUCCESS] Telegram Ping delivered via PROXY. Phone should beep NOW.")
                        else:
                            error_text = await resp.text()
                            print(f"[API ERROR] Proxy returned HTTP {resp.status}: {error_text}")
                except Exception as proxy_e:
                    print(f"[CRITICAL] Proxy connection also failed: {proxy_e}")
            else:
                print("[CRITICAL] No PROXY_URL found in .env to attempt fallback.")

    except Exception as e:
         print(f"[RUNTIME ERROR] Comms check failed: {e}")

if __name__ == "__main__":
    # Bypassing Windows Proactor if needed
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_telegram_link())
