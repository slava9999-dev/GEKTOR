"""
Gerald v4 — Telegram Bot Integration (Outbox-backed).

All messages go through the durable outbox queue by default.
Direct send is available as fallback for critical bootstrap messages.
"""
import os
import aiohttp
from loguru import logger
from utils.config import config
from utils.log_filter import mask_sensitive
from core.alerts.outbox import telegram_outbox, TelegramOutbox, MessagePriority


async def send_telegram_alert(
    message: str,
    parse_mode: str = "HTML",
    disable_notification: bool = False,
    reply_to_message_id: int | None = None,
    reply_markup: dict | None = None,
    priority: str = MessagePriority.NORMAL,
    alert_hash: str = "",
    use_outbox: bool = True,
    status: str = "pending",
) -> int | None:
    """
    Sends a message via Telegram bot.

    v4 Upgrade:
    - Default: enqueues to durable outbox (crash-safe, retries, dedup)
    - Fallback: direct send for bootstrap/emergency messages
    - Returns message_id if direct send, True/None for outbox
    """
    if use_outbox:
        import json
        markup_json = json.dumps(reply_markup) if reply_markup else None
        success = telegram_outbox.enqueue(
            text=message,
            priority=priority,
            disable_notification=disable_notification,
            parse_mode=parse_mode,
            reply_to_message_id=reply_to_message_id,
            reply_markup=markup_json,
            alert_hash=alert_hash,
            status=status,
        )
        if success:
            logger.debug(f"📬 Alert enqueued (priority={priority})")
        return None  # Outbox mode: actual msg_id comes later

    # Direct send (legacy/bootstrap)
    return await send_telegram_direct(
        message, parse_mode, disable_notification,
        reply_to_message_id, reply_markup,
    )


async def send_telegram_direct(
    message: str,
    parse_mode: str = "HTML",
    disable_notification: bool = False,
    reply_to_message_id: int | None = None,
    reply_markup: dict | None = None,
) -> int | None:
    """Direct Telegram send (no outbox). Use sparingly."""
    bot_token = config.telegram.bot_token
    chat_id = config.telegram.chat_id

    if not bot_token or not chat_id:
        logger.warning("Telegram Bot Token or Chat ID not configured. Skipping alert.")
        return None

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
        "disable_notification": disable_notification,
    }

    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        proxy = getattr(config.telegram, "proxy_url", None) or os.getenv("PROXY_URL")
        
        # Gektor Guard: Proxy schema fix
        if proxy and not proxy.startswith(("http://", "https://")):
            proxy = f"http://{proxy}"

        timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_read=10)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            async with session.post(url, json=payload, proxy=proxy) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        return data.get("result", {}).get("message_id")
                    else:
                        logger.error(f"TG API Error: {data.get('description')}")
                        return None
                else:
                    error_text = await response.text()
                    logger.error(
                        f"TG direct send failed: {response.status} {error_text[:200]}"
                    )
                    return None
    except Exception as e:
        err_str = mask_sensitive(str(e))
        if bot_token and len(bot_token) > 10:
            err_str = err_str.replace(bot_token, "***TOKEN***")
        logger.error(f"TG direct exception: {type(e).__name__}: {err_str}")
        return None


def format_level_alert(symbol: str, level: dict) -> str:
    """Formats a Telegram message for a newly detected level."""
    emoji = "🔴" if level["type"] == "RESISTANCE" else "🟢"
    level_type = "СОПРОТИВЛЕНИЕ" if level["type"] == "RESISTANCE" else "ПОДДЕРЖКА"

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
