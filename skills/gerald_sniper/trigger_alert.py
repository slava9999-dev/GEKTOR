import asyncio
import time
import sys
from pathlib import Path

# Fix path to load env
PROJECT_ROOT = Path(__file__).resolve().parents[0]
env_path = PROJECT_ROOT / '.env'
from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path, override=True)

from core.events.events import AlertEvent
from core.events.nerve_center import bus

async def trigger():
    print("📡 Injecting Manual Test Alert to NerveCenter...")
    alert = AlertEvent(
        level="INFO",
        title="SYSTEM_DIAGNOSTIC",
        source="SYSTEM_SURGERY",
        message="🚀 [GEKTOR APEX v12.1] Тестовый сигнал! Инфраструктура Outbox -> Telegram функционирует штатно. Прокси изолирован! 🟢",
        timestamp=time.time()
    )
    await bus.publish(alert)
    print("✅ Alert published! Check your Telegram.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(trigger())
