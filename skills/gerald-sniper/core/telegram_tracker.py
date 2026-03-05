import asyncio
from loguru import logger
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandObject
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from utils.config import config
from data.database import DatabaseManager
import os


class TelegramOracle:
    """
    Listens to Telegram commands (/win, /loss, /skip, /stats, /report)
    and updates the Gerald Sniper database accordingly.
    """
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.token = getattr(config.telegram, 'bot_token', None)
        self.chat_id = getattr(config.telegram, 'chat_id', None)
        self.enabled = bool(self.token and self.chat_id)
        
        if self.enabled:
            # Setting default parse mode for the bot
            self.bot = Bot(token=self.token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            self.dp = Dispatcher()
            self._register_handlers()
        else:
            logger.warning("Telegram Bot Token or Chat ID not configured. Oracle disabled.")

    def _register_handlers(self):
        @self.dp.message(Command("win"))
        async def cmd_win(message: types.Message, command: CommandObject):
            if str(message.chat.id) != str(self.chat_id): return
            await self._handle_result(message, command, "WIN")

        @self.dp.message(Command("loss"))
        async def cmd_loss(message: types.Message, command: CommandObject):
            if str(message.chat.id) != str(self.chat_id): return
            await self._handle_result(message, command, "LOSS")

        @self.dp.message(Command("skip"))
        async def cmd_skip(message: types.Message, command: CommandObject):
            if str(message.chat.id) != str(self.chat_id): return
            await self._handle_result(message, command, "SKIP")

        @self.dp.message(Command("stats"))
        async def cmd_stats(message: types.Message):
            if str(message.chat.id) != str(self.chat_id): return
            try:
                stats = await self.db.get_alert_stats(days=30)
                msg_text = self._format_stats(stats)
                await message.answer(msg_text)
            except Exception as e:
                logger.error(f"Error serving /stats: {e}")

        @self.dp.message(Command("report"))
        async def cmd_report(message: types.Message):
            if str(message.chat.id) != str(self.chat_id): return
            try:
                summary = await self.db.get_weekly_summary()
                await message.answer(summary)
            except Exception as e:
                logger.error(f"Error serving /report: {e}")

    async def _handle_result(self, message: types.Message, command: CommandObject, result: str):
        """Processes /win, /loss, /skip with robust argument parsing."""
        if not command.args:
            await message.reply(f"❌ Формат: `/{result.lower()} [СИМВОЛ] [pnl_pct] [текст...]`\nПример: `/{result.lower()} BTCUSDT 5.2 Закрыл по тейку`", parse_mode="Markdown")
            return
            
        args = command.args.split()
        symbol = args[0].upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"

        pnl_pct = 0.0
        notes = ""
        
        if len(args) > 1:
            try:
                pnl_pct = float(args[1].replace(",", "."))
                notes = " ".join(args[2:])
            except ValueError:
                # If the second arg is not a number, it means everything else is notes (and pnl is 0)
                notes = " ".join(args[1:])
                
        # Find the latest alert for this symbol
        import aiosqlite
        async with aiosqlite.connect(self.db.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, timestamp, signal_type, level_price FROM alerts WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
                (symbol,)
            )
            row = await cursor.fetchone()
            
            if not row:
                await message.reply(f"❌ Алерты по <b>{symbol}</b> не найдены в базе данных.")
                return
                
            alert_id = row['id']
            await self.db.update_alert_result(alert_id, result, pnl_pct, notes)
            
            pnl_str = f" ({pnl_pct:+.2f}%)" if pnl_pct != 0 else ""
            emoji = "✅" if result == "WIN" else "❌" if result == "LOSS" else "⏭"
            
            await message.reply(f"{emoji} <b>{symbol}</b> отмечен как <b>{result}</b>{pnl_str}\n<i>Сигнал от {row['timestamp'][:16]} (Уровень {row['level_price']})</i>")

    def _format_stats(self, stats: dict) -> str:
        s = f"📊 <b>Self-Calibration Stats (Last {stats['period_days']}d)</b>\n\n"
        s += f"📈 Алертов всего: <b>{stats['total_alerts']}</b>\n"
        
        if stats['win_rate_pct'] is not None:
            wr_emoji = "🟢" if stats['win_rate_pct'] >= 50 else "🔴"
            s += f"{wr_emoji} Win Rate: <b>{stats['win_rate_pct']}%</b> (по {stats['evaluated_trades']} сделкам)\n"
        else:
            s += "⚪ Оценённых сделок пока нет.\n"
            
        for res, data in stats['results_breakdown'].items():
            s += f"  • {res}: {data['count']} (Общий PnL: {data['avg_pnl']}%)\n"
            
        if stats['top_symbols']:
            top3 = [f"{sym}" for sym, cnt in list(stats['top_symbols'].items())[:3]]
            s += f"\n🔥 Горячие монеты: {', '.join(top3)}\n"
            
        return s

    async def start(self):
        """Starts the aiogram polling loop. Auto-disables on persistent conflict."""
        if not self.enabled:
            return
        logger.info("🤖 Telegram Oracle started (polling commands: /win, /loss, /stats)...")
        try:
            # Drop pending updates to avoid spamming processed offline messages
            await self.bot.delete_webhook(drop_pending_updates=True)
            await self.dp.start_polling(self.bot)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            err_str = str(e)
            if "Conflict" in err_str or "terminated by other getUpdates" in err_str:
                logger.warning(
                    "⚠️ Telegram Oracle DISABLED: another bot instance (bridge_v2?) "
                    "is already polling this token. Sniper commands (/win, /loss, /stats) "
                    "will NOT work until the other instance is stopped."
                )
            else:
                logger.error(f"Telegram Oracle polling error: {e}")

    async def stop(self):
        """Gracefully stops the polling loop."""
        if self.enabled:
            await self.dp.stop_polling()
            await self.bot.session.close()
