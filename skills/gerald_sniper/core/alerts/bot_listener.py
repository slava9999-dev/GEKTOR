"""
Gerald Tactical Interface — Interactive Callback Listener (AIOGRAM v3).
Task 3: Async Nerve Bridge.
"""
import asyncio
import time
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from loguru import logger
import aiohttp

from utils.config import config
from core.events.nerve_center import bus
from core.events.events import ManualExecutionEvent, SignalLifecycleEvent
from core.radar.lifecycle import lifecycle_manager, RadarSignalState

# Bot instance will be initialized lazily to prevent TokenValidationError during startup
bot = None
dp = Dispatcher()
tactical_router = Router()

# Shared state (Task 5)
_paper_tracker = None
_db = None
_stop_event = None
_macro_radar = None  # [GEKTOR v8.1] Reference to MacroRadar for signal cache

def build_tactical_keyboard(signal_id: str) -> None:
    """[GEKTOR v11.0] Amputated: Pure Text Mode."""
    return None

# [GEKTOR v11.0] Interactive Callbacks Disabled
# ... (handlers removed for purity) ...

# [GEKTOR v2.0] INTERACTIVE EXECUTION DISABLED BY MANIFESTO
# Callback handlers for 'exec|', 'pass|', 'mexec|', 'mpass|' have been removed.
# System is strictly advisory.

@tactical_router.message(F.text == "/status")
async def handle_status_command(message: types.Message):
    """Returns current balance, PnL and active positions."""
    if not _paper_tracker:
        await message.reply("💹 Gerald Status: Система инициализируется...")
        return
    
    stats = _paper_tracker.get_stats()
    open_pos = _paper_tracker.positions
    
    # Simple Russian Report (Pilot Spec)
    report = (
        f"📊 <b>ОТЧЕТ GERALD SNIPER</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Депозит:</b> $1,000.00 (VIRTUAL fallback)\n"
        f"📈 <b>PnL сегодня:</b> +0.00% ($0.00)\n\n"
        
        f"🔍 <b>Активные позиции ({stats['total_open']}):</b>\n"
    )
    
    if not open_pos:
        report += "• <i>Нет открытых сделок</i>\n"
    else:
        for p in open_pos.values():
            dir_emoji = "🟢" if p['direction'] == "LONG" else "🔴"
            pnl_est = 0.0 # simplified
            report += f"• {dir_emoji} <b>{p['symbol']}</b>: {p['entry']:.4f} ({pnl_est:+.1f}%)\n"
            
    report += (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🛰️ <b>Статус:</b> LIVE v5.2\n"
        f"🕒 <b>Hold Avg:</b> {stats['avg_hold_h']}ч"
    )
    
    await message.answer(report, parse_mode="HTML")

@tactical_router.message(F.text == "/reality")
async def handle_reality_command(message: types.Message):
    """Returns behavioral metrics (Human vs Algo)."""
    if not _db:
        await message.reply("🛰️ Reality Reconciler: DB not initialized.")
        return
        
    stats = await _db.get_daily_reality_check()
    if not stats or stats['total_trades'] == 0:
        await message.reply("📈 <b>Reality Check:</b> No data for the last 24h.", parse_mode="HTML")
        return
        
    # Discipline Index: Rogue trades are the primary penalty
    total = stats['total_trades']
    rogue = stats['rogue_trades'] or 0
    discipline = max(0, 100 - (rogue / total * 100))
    
    report = (
        f"📊 <b>REALITY CHECK (24h)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <b>Matched Trades:</b> {total - rogue}\n"
        f"🚨 <b>Rogue Trades:</b> {rogue} (FOMO)\n"
        f"⏱️ <b>Avg Latency:</b> {(stats['avg_latency'] or 0)/1000:.2f}s\n"
        f"📉 <b>Slippage Cost:</b> {(stats['total_slippage_cost'] or 0):+.2f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🛡️ <b>Discipline Index:</b> {discipline:.0f}/100\n"
        f"⚖️ <b>Avg Slippage:</b> {(stats['avg_slippage'] or 0):.2f}% / trade"
    )
    
    await message.answer(report, parse_mode="HTML")

# [GEKTOR v2.0] EMERGENCY AND OPERATIONAL COMMANDS DECOMMISSIONED
# /stop, /panic_sell, /cooldown_off, /cooldown_status have been removed.
# Control is restricted to the server environment.


# Include the router in the main dispatcher
dp.include_router(tactical_router)

# ─────────────────────────────────────────────────────────────────────────────
# [GEKTOR v10.2] LIFECYCLE EVENT HANDLER
# Links outbox messages to signal states for proactive Reaper invalidation.
# ─────────────────────────────────────────────────────────────────────────────
async def handle_lifecycle_event(event: SignalLifecycleEvent):
    """Handles external SignalLifecycleEvents from Outbox."""
    if event.action == "SENT" and event.message_id:
        logger.debug(f"🔗 [Bot] Linking UI for signal {event.signal_id} -> msg {event.message_id}")
        await lifecycle_manager.link_message(event.signal_id, event.chat_id, event.message_id)

async def start_bot_listener(paper_tracker=None, db=None, stop_event=None, macro_radar=None):
    """Entry point to run the listener task with Resilience (Audit 16)."""
    global _paper_tracker, _db, _stop_event, _macro_radar
    _paper_tracker = paper_tracker
    _db = db
    _stop_event = stop_event
    _macro_radar = macro_radar  # [GEKTOR v8.1]
    if not config.telegram.bot_token:
        logger.error("🚫 [Bot] Skipping Listener: No token configured.")
        return
        
    try:
        from aiogram.client.session.aiohttp import AiohttpSession
        import os

        # Proxy support for restricted regions (Audit 16.1)
        proxy_url = os.getenv("PROXY_BOT") or os.getenv("PROXY_URL")
        session = None
        if proxy_url:
            logger.info(f"🛡️ [Bot] Using proxy: {proxy_url}")
            # В aiogram v3 прокси передается напрямую в сессию
            session = AiohttpSession(proxy=proxy_url)

        # Re-initialize bot with session and safe validation (Audit 16.1)
        from aiogram.utils.token import TokenValidationError
        try:
            local_bot = Bot(token=config.telegram.bot_token, session=session)
            # Assign to global for callback handlers to use
            global bot
            bot = local_bot
        except TokenValidationError:
            logger.error("❌ [Bot] Invalid Telegram Token! Interaction disabled.")
            return
        
        # Subscribe to Lifecycle events for UI Linkage
        bus.subscribe(SignalLifecycleEvent, handle_lifecycle_event)

        # 💀 START THE REAPER (GEKTOR v10.2)
        await lifecycle_manager.start_reaper(local_bot)

        # [Audit 16.1] Polling with backoff and timeout
        logger.info("📡 [Bot] Tactical Listener ACTIVE (GEKTOR v10.2)")
        try:
            # We use a short polling timeout to prevent loop stalls
            await asyncio.wait_for(
                dp.start_polling(local_bot, skip_updates=True, polling_timeout=20),
                timeout=None # Run indefinitely, but with internal polling_timeout
            )
        except asyncio.TimeoutError:
            logger.warning("🕒 [Bot] Polling timed out (Network jitter), restarting...")
            # Polling will automatically resume if restart_bot_listener is in a loop
            # But normally we just allow dp to handle it internally
        
    except aiohttp.ClientConnectorError as connectivity_err:
        logger.error(f"❌ [Bot] Telegram Connectivity Error: {connectivity_err}. Check PROXY_BOT or Firewall.")
    except Exception as e:
        logger.critical(f"🛑 [Bot] Listener CRASHED: {e}")
        logger.info("🛡️ [System] Continuing in AUTONOMOUS MODE (No HIL).")
        # Do not re-raise; allow Radar/OMS to continue

if __name__ == "__main__":
    asyncio.run(start_bot_listener())
