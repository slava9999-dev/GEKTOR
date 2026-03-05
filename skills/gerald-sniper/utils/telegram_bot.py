import aiohttp
from loguru import logger
from utils.config import config
from utils.log_filter import mask_sensitive

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
        # Маскируем потенциальные утечки токена в ошибке
        err_str = mask_sensitive(str(e))
        if bot_token and len(bot_token) > 10:
            err_str = err_str.replace(bot_token, "***TOKEN***")
        logger.error(
            f"Telegram API Exception: {type(e).__name__}: {err_str}",
            exc_info=True
        )
        return False

def format_level_alert(symbol: str, level: dict) -> str:
    """Formats a Telegram message for a newly detected level."""
    emoji = "🔴" if level['type'] == 'RESISTANCE' else "🟢"
    level_type = "СОПРОТИВЛЕНИЕ" if level['type'] == 'RESISTANCE' else "ПОДДЕРЖКА"
    
    msg = (
        f"{emoji} <b>LEVEL │ {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 <code>{level['price']}</code> │ {level_type}\n"
        f"📏 Дистанция: {level['distance_pct']}%\n"
        f"✋ Касаний: {level['touches']} (S:{level['support_touches']} R:{level['resistance_touches']})\n"
        f"💪 Сила: {level['strength']}/100\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Gerald Sniper 🎯 │ На мушке</i>"
    )
    return msg
