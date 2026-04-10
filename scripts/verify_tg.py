# scripts/verify_tg.py
import asyncio
import os
import sys
from loguru import logger

# Add project root to path to ensure imports work
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from src.infrastructure.telegram_notifier import TelegramRadarNotifier
    from src.infrastructure.config import settings
except ImportError as e:
    logger.error(f"❌ Import failed. Ensure you are in project root: {e}")
    sys.exit(1)

async def main():
    logger.info("📡 [TEST] Starting Telegram Verification...")
    
    # Use real settings from .env
    notifier = TelegramRadarNotifier(
        bot_token=settings.TG_BOT_TOKEN,
        chat_id=settings.TG_CHAT_ID,
        proxy_url=os.getenv("PROXY_URL")
    )
    
    try:
        await notifier.start()
        logger.info("⏳ Waiting for connection...")
        await asyncio.sleep(2)
        
        test_event = {
            "symbol": "GEKTOR-VERIFY",
            "price": 77777.77,
            "vpin": 0.9999,
            "timestamp": 1712696400000
        }
        
        logger.info("📤 Sending test alert...")
        notifier.notify_event(test_event)
        
        # Give it some time to process the queue
        await asyncio.sleep(5)
        logger.success("🟢 [TEST] Verification script finished. Check Telegram!")
        
    except Exception as e:
        logger.error(f"💥 [TEST] Telegram error: {e}")
    finally:
        await notifier.stop()

if __name__ == "__main__":
    asyncio.run(main())
