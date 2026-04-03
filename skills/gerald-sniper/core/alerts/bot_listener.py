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

from utils.config import config
from core.events.nerve_center import bus
from core.events.events import ManualExecutionEvent

# Bot instance will be initialized lazily to prevent TokenValidationError during startup
bot = None
dp = Dispatcher()
tactical_router = Router()

# Shared state (Task 5)
_paper_tracker = None
_db = None
_stop_event = None
_macro_radar = None  # [GEKTOR v8.1] Reference to MacroRadar for signal cache

def build_tactical_keyboard(signal_id: str, symbol: str, price: float) -> InlineKeyboardMarkup:
    """Generates inline buttons for trade execution/rejection."""
    builder = InlineKeyboardBuilder()
    # Формат callback_data: action|symbol|price|signal_id
    # Длина callback_data в Telegram ограничена 64 байтами
    cb_data = f"exec|{symbol}|{price:.6f}|{signal_id}"
    
    builder.button(text="⚡ ВХОД (Limit IOC)", callback_data=cb_data)
    builder.button(text="🚫 ПАС", callback_data=f"pass|{signal_id}")
    return builder.as_markup()

@tactical_router.callback_query(F.data.startswith("exec|"))
async def handle_execute_button(callback: CallbackQuery):
    """Handles [⚡ ВХОД] button click."""
    try:
        # data format: exec|SYMBOL|PRICE|SIGNAL_ID
        _, symbol, price_str, signal_id = callback.data.split("|")
        
        logger.info(f"🕹️ [Operator] Команда на ВХОД: {symbol} по {price_str} (ID: {signal_id})")
        
        # 1. Отвечаем Telegram, чтобы убрать часики загрузки
        await callback.answer("⏳ Сигнал передан в OMS...", show_alert=False)
        
        # 2. Модифицируем сообщение (убираем кнопку, чтобы не нажать дважды)
        await callback.message.edit_text(
            f"{callback.message.html_text}\n\n<i>[⚡ ЗАПРОС НА ИСПОЛНЕНИЕ ОТПРАВЛЕН]</i>",
            reply_markup=None,
            parse_mode="HTML"
        )
        
        # 3. Публикуем эвент в NerveCenter (ExecutionEngine его поймает)
        event = ManualExecutionEvent(
            signal_id=signal_id,
            action="EXECUTE",
            user_id=str(callback.from_user.id),
            timestamp=time.time()
        )
        await bus.publish(event)
        
    except Exception as e:
        logger.opt(exception=True).error(f"❌ Ошибка обработки кнопки Telegram: {e}")
        await callback.answer("❌ Внутренняя ошибка. См. логи.", show_alert=True)

@tactical_router.callback_query(F.data.startswith("pass|"))
async def handle_pass_button(callback: CallbackQuery):
    """Handles [🚫 ПАС] button click."""
    try:
        _, signal_id = callback.data.split("|")
        
        await callback.answer("Отклонено оператором.", show_alert=False)
        await callback.message.edit_text(
            f"{callback.message.html_text}\n\n<i>[🚫 ОТКЛОНЕНО ОПЕРАТОРОМ]</i>",
            reply_markup=None,
            parse_mode="HTML"
        )
        
        # Publish rejection
        event = ManualExecutionEvent(
            signal_id=signal_id,
            action="REJECT",
            user_id=str(callback.from_user.id),
            timestamp=time.time()
        )
        await bus.publish(event)
        
    except Exception as e:
        logger.error(f"❌ [Bot] Pass Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# [GEKTOR v8.1] MACRO RADAR — ONE-CLICK ENTRY HANDLERS
# ═══════════════════════════════════════════════════════════════════════════
# Callback format: mexec|SYMBOL|PRICE|DIRECTION(L/S)|SIGNAL_ID
# Callback format: mpass|SIGNAL_ID
# ═══════════════════════════════════════════════════════════════════════════

@tactical_router.callback_query(F.data.startswith("mexec|"))
async def handle_macro_execute(callback: CallbackQuery):
    """[GEKTOR v8.1] One-Click Entry from MacroRadar alert."""
    try:
        # Parse callback: mexec|WIFUSDT|2.5000|L|a1b2c3d4
        parts = callback.data.split("|")
        if len(parts) != 5:
            await callback.answer("❌ Неверный формат данных.", show_alert=True)
            return
        
        _, symbol, price_str, direction_short, signal_id = parts
        price = float(price_str)
        direction = "LONG" if direction_short == "L" else "SHORT"
        
        logger.info(
            f"🕹️ [Operator] MACRO ENTRY: {direction} {symbol} @ {price} "
            f"(Signal: {signal_id})"
        )
        
        # ══════════════════════════════════════════════════════════════════
        # [GEKTOR v8.4] SYSTEM MODE GUARD — RADAR_ONLY blocks all entries
        # ══════════════════════════════════════════════════════════════════
        import os as _os
        system_mode = _os.environ.get("_TEKTON_SYSTEM_MODE", "RADAR_ONLY")
        if system_mode != "LIVE":
            logger.info(
                f"🛡️ [MODE] RADAR_ONLY: Entry blocked for {symbol}. "
                f"Operator tapped button but execution engine is DISABLED."
            )
            await callback.answer(
                f"🛡️ Режим РАДАР: Исполнение отключено.\n"
                f"Сигнал {direction} {symbol} принят к сведению.\n"
                f"Для торговли: SYSTEM_MODE=LIVE + API ключи.",
                show_alert=True
            )
            return
        
        # ══════════════════════════════════════════════════════════════════
        # [GEKTOR v8.4] GLOBAL CIRCUIT BREAKER — Anti-FOMO Guard
        # After /panic_sell, all entry commands are BLOCKED for 2 hours.
        # ══════════════════════════════════════════════════════════════════
        try:
            from core.shield.amputation import AmputationProtocol
            cooldown = await AmputationProtocol.check_global_cooldown()
            if cooldown:
                remaining = cooldown.get('remaining_min', '?')
                logger.warning(
                    f"🧊 [COOLDOWN] ENTRY BLOCKED: {symbol} — "
                    f"Post-panic cooldown: {remaining} min remaining."
                )
                await callback.answer(
                    f"🧊 System Cooldown! Вход заблокирован.\n"
                    f"Осталось: {remaining} мин. Снять: /cooldown_off",
                    show_alert=True
                )
                return
        except Exception:
            pass  # Redis down → proceed (fail-open for cooldown check only)
        
        # ══════════════════════════════════════════════════════════════════
        # [GEKTOR v8.2] IDEMPOTENCY GUARD — Anti Double-Tap Liquidation
        # Redis SET NX: Atomic lock. First tap wins. Duplicates rejected.
        # Key lives 60 min to prevent replay attacks.
        # ══════════════════════════════════════════════════════════════════
        idempotency_key = f"tekton:exec_lock:{symbol}:{signal_id}"
        try:
            lock_acquired = await bus.redis.set(
                idempotency_key, "LOCKED", ex=3600, nx=True
            )
        except Exception as redis_err:
            # Redis down = FAIL-CLOSED. We do NOT execute without idempotency.
            logger.error(f"❌ [IDEMPOTENCY] Redis unavailable: {redis_err}. Rejecting execution.")
            await callback.answer("❌ Redis offline. Ордер заблокирован.", show_alert=True)
            return
        
        if not lock_acquired:
            logger.warning(
                f"🚫 [IDEMPOTENCY] Duplicate tap on {symbol}:{signal_id}. "
                f"Ignoring (anti-double-click)."
            )
            await callback.answer("⚠️ Ордер уже в работе! (защита от двойного клика)", show_alert=True)
            return
        
        # 1. Instant feedback — remove loading spinner
        await callback.answer(f"⚡ {direction} {symbol} → OMS...", show_alert=False)
        
        # 2. Disable buttons to prevent visual double-click
        try:
            await callback.message.edit_text(
                f"{callback.message.html_text}\n\n"
                f"<b>[⚡ {direction} ЗАПРОС ОТПРАВЛЕН В OMS]</b>\n"
                f"<i>Ожидай подтверждения...</i>",
                reply_markup=None,
                parse_mode="HTML"
            )
        except Exception:
            pass  # Message edit can fail if too old, non-critical
        
        # 3. Publish execution event to NerveCenter (guaranteed unique)
        event = ManualExecutionEvent(
            signal_id=signal_id,
            action="EXECUTE",
            user_id=str(callback.from_user.id),
            timestamp=time.time(),
            symbol=symbol,
            price=price,
            direction=direction,
            source="MACRO_RADAR",
        )
        await bus.publish(event)
        
        logger.info(f"✅ [Bot] Macro execution event published: {direction} {symbol}")
        
    except ValueError as e:
        logger.error(f"❌ [Bot] Macro callback parse error: {e}")
        await callback.answer("❌ Ошибка парсинга цены.", show_alert=True)
    except Exception as e:
        logger.opt(exception=True).error(f"❌ [Bot] Macro execute error: {e}")
        await callback.answer("❌ Внутренняя ошибка. См. логи.", show_alert=True)


@tactical_router.callback_query(F.data.startswith("mpass|"))
async def handle_macro_pass(callback: CallbackQuery):
    """[GEKTOR v8.1] Operator dismissed macro alert."""
    try:
        _, signal_id = callback.data.split("|")
        
        await callback.answer("🚫 Пропущено.", show_alert=False)
        
        try:
            await callback.message.edit_text(
                f"{callback.message.html_text}\n\n"
                f"<i>[🚫 ПРОПУЩЕНО ОПЕРАТОРОМ]</i>",
                reply_markup=None,
                parse_mode="HTML"
            )
        except Exception:
            pass
        
        event = ManualExecutionEvent(
            signal_id=signal_id,
            action="REJECT",
            user_id=str(callback.from_user.id),
            timestamp=time.time(),
            source="MACRO_RADAR",
        )
        await bus.publish(event)
        
        logger.info(f"🚫 [Bot] Macro alert {signal_id} dismissed by operator.")
        
    except Exception as e:
        logger.error(f"❌ [Bot] Macro pass error: {e}")

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

@tactical_router.message(F.text == "/stop")
async def handle_stop_command(message: types.Message):
    """Emergency Halt: Close all and stop."""
    await message.answer("🛑 <b>ЭКСТРЕННАЯ ОСТАНОВКА!</b>\nЗакрываю все позиции и выключаю систему...", parse_mode="HTML")
    logger.critical("🚨 [Bot] EMERGENCY STOP triggered via Telegram!")
    
    if _stop_event:
        _stop_event.set()


# ═══════════════════════════════════════════════════════════════════════════
# [GEKTOR v8.3] AMPUTATION PROTOCOL — /panic_sell Command
# ═══════════════════════════════════════════════════════════════════════════
# Two-stage confirmation: /panic_sell → Confirm button → Fan-Out liquidation
# ═══════════════════════════════════════════════════════════════════════════

@tactical_router.message(F.text == "/panic_sell")
async def handle_panic_sell_command(message: types.Message):
    """
    [GEKTOR v8.3] Amputation Protocol entry point.
    Stage 1: Show confirmation keyboard. No action until confirmed.
    """
    logger.warning(f"☢️ [PANIC] /panic_sell invoked by {message.from_user.id}")
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="☢️ ПОДТВЕРДИТЬ АМПУТАЦИЮ",
        callback_data="panic_confirm"
    )
    builder.button(
        text="❌ ОТМЕНА",
        callback_data="panic_cancel"
    )
    builder.adjust(1)  # Stack vertically for clear UI
    
    await message.answer(
        "☢️☢️☢️ <b>AMPUTATION PROTOCOL</b> ☢️☢️☢️\n\n"
        "⚠️ Это <b>ЭКСТРЕННАЯ ЛИКВИДАЦИЯ</b> всех позиций!\n\n"
        "<b>Будет выполнено:</b>\n"
        "1. ❌ Отмена ВСЕХ открытых ордеров\n"
        "2. 🔥 Market Close ВСЕХ позиций (параллельно)\n"
        "3. 🛑 Kill Switch — блокировка новых сделок\n\n"
        "⏱ Таймаут: 30 секунд\n\n"
        "<b>ТЫ УВЕРЕН?</b>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


@tactical_router.callback_query(F.data == "panic_confirm")
async def handle_panic_confirm(callback: CallbackQuery):
    """
    [GEKTOR v8.3] Stage 2: Execute the Amputation Protocol.
    Fan-Out parallel liquidation of all positions.
    """
    operator_id = str(callback.from_user.id)
    
    logger.critical(f"☢️☢️☢️ [PANIC] CONFIRMED by operator {operator_id}!")
    
    # Idempotency: prevent double-panic via Redis
    try:
        lock = await bus.redis.set("tekton:panic_lock", "ACTIVE", ex=120, nx=True)
        if not lock:
            await callback.answer("⚠️ Ампутация уже выполняется!", show_alert=True)
            return
    except Exception:
        pass  # Redis down → proceed anyway (panic takes priority)
    
    # Visual feedback
    await callback.answer("☢️ АМПУТАЦИЯ ЗАПУЩЕНА!", show_alert=True)
    try:
        await callback.message.edit_text(
            "☢️ <b>AMPUTATION PROTOCOL — ВЫПОЛНЯЕТСЯ...</b>\n"
            "⏳ Параллельная ликвидация всех позиций...\n"
            "⏱ Таймаут: 30 секунд",
            parse_mode="HTML"
        )
    except Exception:
        pass
    
    # Execute the protocol
    from core.shield.amputation import get_amputation_protocol
    protocol = get_amputation_protocol()
    
    if protocol is None:
        logger.error("❌ [PANIC] AmputationProtocol not initialized!")
        try:
            await callback.message.edit_text(
                "❌ <b>ОШИБКА:</b> Протокол ампутации не инициализирован.\n"
                "Закрывай позиции ВРУЧНУЮ через Bybit!",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return
    
    report = await protocol.execute(operator_id=operator_id)
    
    # Format and send report
    report_text = protocol.format_report_telegram(report)
    try:
        await callback.message.edit_text(
            report_text,
            parse_mode="HTML"
        )
    except Exception:
        # If edit fails, send as new message
        if bot:
            try:
                await bot.send_message(
                    callback.message.chat.id,
                    report_text,
                    parse_mode="HTML"
                )
            except Exception:
                pass
    
    # Release Redis lock
    try:
        await bus.redis.delete("tekton:panic_lock")
    except Exception:
        pass


@tactical_router.callback_query(F.data == "panic_cancel")
async def handle_panic_cancel(callback: CallbackQuery):
    """Cancel the panic sell confirmation."""
    logger.info(f"✅ [PANIC] Cancelled by operator {callback.from_user.id}")
    await callback.answer("✅ Отменено.", show_alert=False)
    try:
        await callback.message.edit_text(
            "✅ <b>Ампутация отменена.</b>\n"
            "Позиции остаются открытыми.",
            parse_mode="HTML"
        )
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════════════
# [GEKTOR v8.4] COOLDOWN CONTROL — Manual Override Commands
# ═══════════════════════════════════════════════════════════════════════════

@tactical_router.message(F.text == "/cooldown_off")
async def handle_cooldown_off(message: types.Message):
    """[GEKTOR v8.4] Manually disengage Global Circuit Breaker."""
    logger.warning(f"🔓 [COOLDOWN] /cooldown_off invoked by {message.from_user.id}")
    
    from core.shield.amputation import AmputationProtocol
    
    cooldown = await AmputationProtocol.check_global_cooldown()
    if not cooldown:
        await message.answer(
            "✅ Cooldown не активен. MacroRadar работает в штатном режиме.",
            parse_mode="HTML"
        )
        return
    
    removed = await AmputationProtocol.disengage_global_cooldown()
    if removed:
        await message.answer(
            "🔓 <b>Global Cooldown СНЯТ.</b>\n"
            "MacroRadar снова отправляет сигналы.\n"
            "⚠️ Будь осторожен — рынок может быть нестабилен.",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "❌ Не удалось снять cooldown. Проверь Redis.",
            parse_mode="HTML"
        )


@tactical_router.message(F.text == "/cooldown_status")
async def handle_cooldown_status(message: types.Message):
    """[GEKTOR v8.4] Check Global Circuit Breaker status."""
    from core.shield.amputation import AmputationProtocol
    
    cooldown = await AmputationProtocol.check_global_cooldown()
    if cooldown:
        remaining = cooldown.get('remaining_min', '?')
        reason = cooldown.get('reason', '?')
        await message.answer(
            f"🧊 <b>GLOBAL COOLDOWN ACTIVE</b>\n"
            f"Причина: {reason}\n"
            f"Осталось: <b>{remaining} мин</b>\n"
            f"Снять: /cooldown_off",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "✅ Cooldown не активен. MacroRadar работает.",
            parse_mode="HTML"
        )


# Include the router in the main dispatcher
dp.include_router(tactical_router)

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
        
        logger.info("📡 [Bot] Tactical Listener ACTIVE (AIOGRAM v3)")
        await dp.start_polling(local_bot, skip_updates=True)
        
    except Exception as e:
        logger.critical(f"🛑 [Bot] Listener CRASHED or failed to start: {e}")
        logger.info("🛡️ [System] Continuing in AUTONOMOUS MODE (No HIL).")
        # Do not re-raise; allow Radar/OMS to continue

if __name__ == "__main__":
    asyncio.run(start_bot_listener())
