import asyncio
import os
import sys
from loguru import logger
from dotenv import load_dotenv
from pathlib import Path

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SNIPER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
if SNIPER_ROOT not in sys.path:
    sys.path.append(SNIPER_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from data.database import DatabaseManager
from utils.telegram_bot import send_telegram_alert
from utils.config import config


async def generate_and_send_weekly_report():
    """Скрипт ручной генерации еженедельного отчета и его отправки в Telegram."""
    logger.info("📊 Запуск скрипта генерации еженедельного отчета (AI-отчёт)")
    
    # DB Match to config
    db_path = getattr(config.storage, 'db_path', str(Path(SNIPER_ROOT) / "data_run" / "sniper.db")) if hasattr(config, "storage") else str(Path(SNIPER_ROOT) / "data_run" / "sniper.db")
    db_manager = DatabaseManager(db_path)
    await db_manager.initialize()
    
    logger.info(f"💾 Подключено к локальной базе данных: {db_path}")
    
    try:
        # Извлекаем данные и скоринг из БД за последние 7 дней (и вшито внутри)
        report_text = await db_manager.get_weekly_summary()
        logger.info("\n" + report_text)
        
        # Отправляем в Telegram
        logger.info("📨 Отправка в Telegram...")
        msg_id = await send_telegram_alert(report_text)
        if msg_id:
            logger.info("✅ Отчет успешно отправлен в Telegram!")
        else:
            logger.error("❌ Не удалось отправить отчет в Telegram.")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при генерации отчета: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(generate_and_send_weekly_report())
