import asyncio
import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
import requests

# Project Imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.shared.logger import logger
from src.infrastructure.llm.router import SmartRouter
from src.application.services.agent import GeraldAgent
from src.application.services.indexer import BackgroundIndexer

# Load security environment
BASE_DIR = r"c:\Gerald-superBrain"
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Configuration paths
BRIDGE_DIR = os.path.join(BASE_DIR, "bridge")
INBOX = os.path.join(BRIDGE_DIR, "inbox")
OUTBOX = os.path.join(BRIDGE_DIR, "outbox")
STATUS_FILE = os.path.join(BRIDGE_DIR, "status.json")


class GeraldBridgeV2:
    def __init__(self):
        # Configure API routes
        llm_config = {
            "sanitizer": True,
            "providers": {
                "deepseek": {
                    "enabled": False,  # Disabled due to insufficient balance
                    "api_key_env": "DEEPSEEK_API_KEY",
                    "base_url": "https://api.deepseek.com/v1",
                    "model": "deepseek-chat",
                    "rpm": 60,
                    "supports_strict_schema": False,
                    "supports_json_object": True
                },
                "gpt4o_mini": {
                    "enabled": True,
                    "api_key_env": "OPENROUTER_API_KEY",
                    "base_url": "https://openrouter.ai/api/v1",
                    "model": "openai/gpt-4o-mini",
                    "rpm": 60,
                    # Disabled strict checking because tool_args is a dynamic dict[str, Any], which OpenAI Strict Mode hates.
                    "supports_strict_schema": False,
                    "supports_json_object": True,
                    "extra_body": {
                        "provider": {
                            "order": ["OpenAI"],
                            "ignore": ["Azure"],
                            "allow_fallbacks": True,
                        },
                        "route": "fallback",
                    },
                    "extra_headers": {
                        "HTTP-Referer": "https://gerald-superbrain.local",
                        "X-Title": "Gerald-SuperBrain"
                    }
                },
                "local": {
                    "enabled": True,
                    # We reuse existing `LlamaEngine()` parameters from config.yaml under the hood
                }
            },
            "routing": {
                "simple": {"primary": "deepseek", "fallback": "gpt4o_mini", "emergency": "local"},
                "agent":  {"primary": "gpt4o_mini", "fallback": "deepseek", "emergency": "local"},
                "deep":   {"primary": "gpt4o_mini", "fallback": "deepseek", "emergency": "local"}
            }
        }
        
        self.llm_router = SmartRouter(llm_config)
        self.agent = GeraldAgent(self.llm_router)
        self.indexer = BackgroundIndexer(self.agent.vector_db)
        self.token = os.getenv("GERALD_BOT_TOKEN")
        self.chat_id = int(os.getenv("TELEGRAM_CHAT_ID", 0))
        self.offset = self._load_offset()
        self._consecutive_errors = 0
        self._semaphore = asyncio.Semaphore(2)

    def _load_offset(self):
        try:
            with open(STATUS_FILE, "r") as f:
                return json.load(f).get("tg_offset", 0)
        except Exception:

            return 0

    def _save_status(self, offset: int, status: str = "running"):
        try:
            with open(STATUS_FILE, "r") as f:
                data = json.load(f)
        except Exception:

            data = {"bridge_version": "2.0.0", "errors": []}

        data["tg_offset"] = offset
        data["status"] = status
        data["last_sync"] = datetime.now().isoformat()

        with open(STATUS_FILE, "w") as f:
            json.dump(data, f, indent=2)

    async def poll_telegram(self):
        if not self.token:
            logger.error("No Telegram token found in .env")
            return

        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        try:
            proxies = None
            proxy_url = os.getenv("PROXY_URL")
            if proxy_url:
                proxies = {"http": proxy_url, "https": proxy_url}
                
            # We use non-blocking request for simplicity or run in executor
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: requests.get(
                    url, params={"offset": self.offset, "timeout": 20}, timeout=25, proxies=proxies
                ),
            )
            updates = resp.json().get("result", [])
            
            # Reset error counter on successful response
            if self._consecutive_errors > 0:
                logger.info("TG connection restored.")
                self._consecutive_errors = 0

            for update in updates:
                self.offset = update["update_id"] + 1
                self._save_status(self.offset)

                if "message" in update and "text" in update["message"]:
                    msg = update["message"]
                    cid = msg["chat"]["id"]
                    text = msg["text"]

                    if cid == self.chat_id:
                        # Route sniper commands directly to DB
                        if text.startswith(("/win", "/loss", "/skip", "/stats", "/report")):
                            logger.info(f"Sniper command: {text}")
                            asyncio.create_task(self._handle_sniper_command(text))
                        else:
                            logger.info(f"TG Message from Slava: {text[:50]}...")
                            asyncio.create_task(self.handle_message(text))
                    else:
                        logger.warning(f"Unauthorized TG access from {cid}")
                        
                elif "callback_query" in update:
                    cb = update["callback_query"]
                    cid = cb.get("message", {}).get("chat", {}).get("id")
                    if cid == self.chat_id:
                        asyncio.create_task(self._handle_callback(cb))
                    else:
                        logger.warning(f"Unauthorized TG callback from {cid}")
        except Exception as e:
            self._consecutive_errors += 1
            backoff = min(15, 3 * self._consecutive_errors) # Reduced max backoff for standard network drops
            
            error_msg = str(e)
            silent_errors = ["Max retries exceeded", "Read timed out", "Connection aborted", "10054", "502 Bad Gateway", "SSLEOFError"]
            if any(term in error_msg for term in silent_errors):
                logger.warning(f"🛜 TG Poll network lag/proxy drop. Retry in {backoff}s...")
            else:
                logger.error(f"TG Poll error: {e}. Backing off for {backoff}s...")
            await asyncio.sleep(backoff)

    async def _handle_sniper_command(self, text: str):
        """Routes /win, /loss, /skip, /stats, /report to sniper DB."""
        try:
            import sys as _sys
            sniper_path = os.path.join(BASE_DIR, "skills", "gerald-sniper")
            if sniper_path not in _sys.path:
                _sys.path.insert(0, sniper_path)
            from data.database import DatabaseManager

            db_path = os.path.join(BASE_DIR, "skills", "gerald-sniper", "data_run", "sniper.db")
            db = DatabaseManager(db_path)
            await db.initialize()

            parts = text.strip().split()
            cmd = parts[0].lower()

            if cmd == "/stats":
                # 1. History Stats
                stats = await db.get_alert_stats(days=30)
                
                # 2. Open Positions
                import aiosqlite
                async with aiosqlite.connect(db_path) as conn:
                    conn.row_factory = aiosqlite.Row
                    cur = await conn.execute(
                        "SELECT symbol, direction, entry_price, timestamp FROM alerts WHERE result IS NULL ORDER BY timestamp DESC"
                    )
                    open_rows = await cur.fetchall()

                # 3. Format Response
                wr = stats.get('win_rate_pct')
                wr_str = f"{wr}%" if wr is not None else "N/A"
                
                reply = "📊 <b>GERALD SNIPER STATS (30d)</b>\n\n"
                reply += f"📉 Total Alerts: <b>{stats['total_alerts']}</b>\n"
                reply += f"🎯 Win Rate: <b>{wr_str}</b> ({stats['evaluated_trades']} trades)\n"
                
                res_str = ""
                for r, d in stats.get('results_breakdown', {}).items():
                    res_str += f"  • {r}: {d['count']} (avg {d['avg_pnl']}%)\n"
                if res_str: reply += res_str
                
                reply += f"\n📂 <b>Open Positions ({len(open_rows)}):</b>\n"
                if not open_rows:
                    reply += "  <i>No active positions</i>\n"
                else:
                    # Show only last 10 for brevity if too many
                    for row in open_rows[:15]:
                        dt = row['timestamp'][11:16] # HH:MM
                        reply += f"  • <code>{row['symbol']}</code> {row['direction']} @ {row['entry_price']} ({dt})\n"
                    if len(open_rows) > 15:
                        reply += f"  ... and {len(open_rows)-15} more\n"
                
                reply += f"\n<i>System Status: Operational 🟢</i>"

            elif cmd == "/report":
                reply = await db.get_weekly_summary()

            elif cmd in ("/win", "/loss", "/skip"):
                if len(parts) < 2:
                    reply = f"❌ Формат: {cmd} СИМВОЛ [pnl%]\nПример: {cmd} BTCUSDT 5.2"
                else:
                    import aiosqlite
                    symbol = parts[1].upper()
                    if not symbol.endswith("USDT"):
                        symbol += "USDT"
                    result = cmd.lstrip("/").upper()
                    pnl = 0.0
                    if len(parts) > 2:
                        try:
                            pnl = float(parts[2].replace(",", "."))
                        except ValueError:
                            pass
                    notes = " ".join(parts[3:]) if len(parts) > 3 else ""

                    async with aiosqlite.connect(db_path) as conn:
                        conn.row_factory = aiosqlite.Row
                        cur = await conn.execute(
                            "SELECT id, timestamp, level_price FROM alerts WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1",
                            (symbol,)
                        )
                        row = await cur.fetchone()

                    if not row:
                        reply = f"❌ Алерты по {symbol} не найдены."
                    else:
                        await db.update_alert_result(row["id"], result, pnl, notes)
                        emoji = "✅" if result == "WIN" else "❌" if result == "LOSS" else "⏭"
                        reply = f"{emoji} {symbol} → {result} ({pnl:+.1f}%)\nАлерт от {row['timestamp'][:16]}"
            else:
                reply = "❓ Неизвестная команда."

            # Send reply
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            proxies = None
            proxy_url = os.getenv("PROXY_URL")
            if proxy_url:
                proxies = {"http": proxy_url, "https": proxy_url}
                
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, lambda: requests.post(url, json={"chat_id": self.chat_id, "text": reply, "parse_mode": "HTML"}, timeout=10, proxies=proxies)
            )
        except Exception as e:
            logger.error(f"Sniper command error: {e}")

    async def _handle_callback(self, cb: dict):
        try:
            cb_id = cb["id"]
            data = cb.get("data", "")
            message = cb.get("message", {})
            chat_id = message.get("chat", {}).get("id")
            
            # 1. Answer callback
            url = f"https://api.telegram.org/bot{self.token}/answerCallbackQuery"
            proxies = None
            proxy_url = os.getenv("PROXY_URL")
            if proxy_url:
                proxies = {"http": proxy_url, "https": proxy_url}
                
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: requests.post(url, json={"callback_query_id": cb_id}, timeout=10, proxies=proxies))

            if data.startswith(("/take_", "/skip_")):
                parts = data.split("_", 1)
                if len(parts) == 2:
                    action = parts[0]
                    alert_id = int(parts[1])
                    result = "TAKEN" if action == "/take" else "SKIPPED"
                    
                    # Update DB
                    import sys as _sys
                    sniper_path = os.path.join(BASE_DIR, "skills", "gerald-sniper")
                    if sniper_path not in _sys.path:
                        _sys.path.insert(0, sniper_path)
                    from data.database import DatabaseManager
                    
                    db_path = os.path.join(sniper_path, "data_run", "sniper.db")
                    db = DatabaseManager(db_path)
                    await db.initialize()
                    await db.update_alert_result(alert_id, result, 0.0, "User interaction via button")

                    # Remove inline keyboard
                    import json as _json
                    edit_url = f"https://api.telegram.org/bot{self.token}/editMessageReplyMarkup"
                    await loop.run_in_executor(
                        None, lambda: requests.post(edit_url, json={
                            "chat_id": chat_id,
                            "message_id": message.get("message_id"),
                            "reply_markup": _json.dumps({"inline_keyboard": []})
                        }, timeout=10, proxies=proxies)
                    )

                    # Send confirmation
                    msg_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                    emoji = "✅" if result == "TAKEN" else "⏭️"
                    await loop.run_in_executor(
                        None, lambda: requests.post(msg_url, json={
                            "chat_id": chat_id,
                            "text": f"{emoji} Сигнал #{alert_id} отмечен как {result}.",
                            "reply_to_message_id": message.get("message_id")
                        }, timeout=10, proxies=proxies)
                    )
        except Exception as e:
            logger.error(f"Callback handling error: {e}")

    async def handle_message(self, text: str):
        async with self._semaphore:
            try:
                proxies = None
                proxy_url = os.getenv("PROXY_URL")
                if proxy_url:
                    proxies = {"http": proxy_url, "https": proxy_url}

                # Send typing indicator (non-blocking)
                typing_url = f"https://api.telegram.org/bot{self.token}/sendChatAction"
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: requests.post(
                        typing_url,
                        json={"chat_id": self.chat_id, "action": "typing"},
                        timeout=5,
                        proxies=proxies,
                    ),
                )

                # 1. Get response from Gerald
                response = await self.agent.chat(text)

                # 2. Send back to Telegram
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                payload = {
                    "chat_id": self.chat_id,
                    "text": f"🧠 Gerald (V2.0):\n\n{response}",
                }

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, lambda: requests.post(url, json=payload, timeout=10, proxies=proxies)
                )
                logger.info("Response sent to Telegram")
            except Exception as e:
                logger.error(f"Error handling message: {e}")

    async def run(self):
        logger.info("🚀 Gerald Bridge V2.0 Starting...")

        # Preload RAG Models (SentenceTransformers) onto CUDA in background 
        # so that the first message from Telegram doesn't take 15 extra seconds!
        self.agent.preload_models()

        # Start Indexer
        asyncio.create_task(self.indexer.scan_and_index())

        while True:
            await self.poll_telegram()
            await asyncio.sleep(2)


if __name__ == "__main__":
    bridge = GeraldBridgeV2()
    try:
        asyncio.run(bridge.run())
    except KeyboardInterrupt:
        logger.info("Bridge stopped by user")
