import asyncio
import os
from loguru import logger
from dotenv import load_dotenv
from skills.gerald_sniper.core.realtime.telegram_client import TelegramClient

async def test_tg():
    load_dotenv()
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    proxy = os.getenv("PROXY_URL")
    
    if not token or not chat_id:
        logger.error("❌ TG_BOT_TOKEN or TG_CHAT_ID not set.")
        return

    logger.info(f"🧪 Testing TG with token {token[:5]}... and chat_id {chat_id}")
    client = TelegramClient(token, chat_id, proxy=proxy)
    
    success = await client.send_message("🤖 GEKTOR v21.64.2: Telegram Notification Test SUCCESSFUL. System is ARMED in SWING mode. 🛡️🌍")
    if success:
        logger.success("🚀 TG Test Message SENT.")
    else:
        logger.error("❌ TG Test Message FAILED.")

if __name__ == "__main__":
    asyncio.run(test_tg())
