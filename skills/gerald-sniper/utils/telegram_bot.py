import aiohttp
from loguru import logger
from utils.config import config

async def send_telegram_alert(message: str, parse_mode: str = "HTML", disable_notification: bool = False) -> bool:
    """Sends a message via Telegram bot."""
    bot_token = config.telegram.bot_token
    chat_id = config.telegram.chat_id
    
    if not bot_token or not chat_id:
        logger.warning("Telegram Bot Token or Chat ID not configured. Skipping alert.")
        return False
        
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
        "disable_notification": disable_notification
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                if response.status == 200:
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to send Telegram message: {response.status} {error_text}")
                    return False
    except Exception as e:
        logger.error(f"Telegram API Exception: {e}")
        return False

def format_level_alert(symbol: str, level: dict) -> str:
    """Formats a beautiful Telegram message for a newly detected level."""
    # emojis
    emoji = "🔴" if level['type'] == 'RESISTANCE' else "🟢"
    
    msg = f"<b>{emoji} Gerald Sniper: Новый уровень!</b>\n"
    msg += f"<b>Монета:</b> #{symbol}\n"
    msg += f"<b>Уровень:</b> {level['price']} ({level['type']})\n"
    msg += f"<b>Дистанция:</b> {level['distance_pct']}%\n"
    msg += f"<b>Касаний:</b> {level['touches']} (Подд: {level['support_touches']}, Сопр: {level['resistance_touches']})\n"
    msg += f"<b>Score (Сила):</b> {level['strength']}/100\n\n"
    msg += f"<i>Взял на мушку. Жду триггер для входа... 🎯</i>"
    
    return msg
