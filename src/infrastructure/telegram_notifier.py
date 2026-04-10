# src/infrastructure/telegram_notifier.py
import asyncio
import aiohttp
import json
import hashlib
import os
import time
from collections import deque
from datetime import datetime, timezone
from aiohttp_socks import ProxyConnector
from loguru import logger
from typing import Any, Optional

class TelegramRadarNotifier:
    """[GEKTOR v2.0] Institutional Telegram Client with PSR Protocol."""
    def __init__(self, bot_token: str, chat_id: str, proxy_url: Optional[str] = None):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.proxy_url = proxy_url
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        
        self._queue = asyncio.Queue(maxsize=100)
        self._live_allowed = asyncio.Event()
        self._live_allowed.set()
        
        # Idempotency: 1000 event IDs
        self._sent_cache = deque(maxlen=1000)
        self.fallback_file = "failed_alerts.jsonl"
        self._running = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._worker_task: Optional[asyncio.Task] = None

    def _generate_event_id(self, event_data: dict) -> str:
        raw = f"{event_data.get('symbol')}_{event_data.get('timestamp')}_{event_data.get('price')}"
        return hashlib.sha256(raw.encode()).hexdigest()

    async def start(self):
        self._running = True
        self._worker_task = asyncio.create_task(self._process_worker())
        logger.info("📡 [Telegram] Secure tunnel initialized. PSR Protocol ARMED.")

    async def _process_worker(self):
        connector = ProxyConnector.from_url(self.proxy_url, rdns=True) if self.proxy_url else None
        timeout = aiohttp.ClientTimeout(total=15.0) 
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self._session = session
            # 1. Recovery Phase
            await self._run_recovery_phase()
            
            while self._running:
                await self._live_allowed.wait()
                
                event_data = await self._queue.get()
                event_id = self._generate_event_id(event_data)
                
                if event_id not in self._sent_cache:
                    message = self._format_message(event_data)
                    success = await self._send_with_retry(message, event_data)
                    if success:
                        self._sent_cache.append(event_id)
                
                self._queue.task_done()

    async def _run_recovery_phase(self):
        if not os.path.exists(self.fallback_file) or os.path.getsize(self.fallback_file) == 0:
            return

        logger.warning("🔄 [Recovery] Запуск дренажа отложенных сигналов (JSONL)...")
        self._live_allowed.clear()
        
        remaining_lines = []
        try:
            with open(self.fallback_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                
            for line in lines:
                if not line.strip(): continue
                event_data = json.loads(line.strip())
                event_id = self._generate_event_id(event_data)
                
                if event_id in self._sent_cache: continue
                    
                message = self._format_message(event_data, is_delayed=True)
                success = await self._send_with_retry(message, event_data, is_recovery=True)
                
                if success:
                    self._sent_cache.append(event_id)
                    await asyncio.sleep(1.5) # Anti-Spam
                else:
                    remaining_lines.append(line)
        except Exception as e:
            logger.error(f"🚨 [Recovery] Ошибка парсинга дампа: {e}")
        finally:
            with open(self.fallback_file, "w", encoding="utf-8") as f:
                f.writelines(remaining_lines)
            
            if not remaining_lines:
                logger.success("🟢 [Recovery] Дренаж завершен. Файл очищен. Разморозка живой очереди.")
                self._live_allowed.set()

    async def _send_with_retry(self, message: str, raw_data: dict, is_recovery: bool = False) -> bool:
        if not self._session: return False
        
        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}
        for attempt in range(3):
            try:
                async with self._session.post(self.api_url, json=payload) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    logger.success("📱 [Telegram] Отчет доставлен.")
                    return True
            except Exception as e:
                logger.debug(f"⚠️ [Telegram] Retry {attempt+1} failed: {e}")
                await asyncio.sleep(2 ** attempt)

        if not is_recovery:
            self._dump_to_fallback(raw_data)
        return False

    def _dump_to_fallback(self, event_data: dict):
        event_data["_dump_ts"] = datetime.now(timezone.utc).isoformat()
        with open(self.fallback_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event_data) + "\n")

    def notify_event(self, event_data: dict):
        try:
            self._queue.put_nowait(event_data)
        except asyncio.QueueFull:
            logger.error("🚨 [Telegram] QUEUE FULL. Dumping to JSONL.")
            self._dump_to_fallback(event_data)

    def notify_manual(self, text: str):
        """Send critical system alert regardless of formatting/event structure."""
        logger.critical(f"🔔 [ALERT] Sending manual notification: {text}")
        # Construct a fake event for the formatter or just send raw
        asyncio.create_task(self._send_raw_text(text))

    async def _send_raw_text(self, text: str):
        # Wait up to 5 seconds for session if not initialized
        for _ in range(10):
            if self._session: break
            await asyncio.sleep(0.5)

        if not self._session:
            logger.error("❌ [Telegram] Cannot send manual alert: No active session.")
            return

        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with self._session.post(self.api_url, json=payload) as resp:
                resp.raise_for_status()
                logger.success("🔔 [Telegram] Manual alert sent.")
        except Exception as e:
            logger.error(f"❌ [Telegram] Manual alert failed: {e}")

    def _format_message(self, event: dict, is_delayed: bool = False) -> str:
        symbol = event.get('symbol', 'UNKNOWN')
        price = event.get('price', 0.0)
        vpin = event.get('vpin')
        vpin_str = f"{vpin:.4f}" if vpin else "N/A"
        
        ts_utc = datetime.fromtimestamp(event.get('timestamp', 0)/1000, tz=timezone.utc).strftime('%H:%M:%S')
        
        header = "⚠️ [DELAYED / RECOVERY]" if is_delayed else "⚡ [GEKTOR APEX] СТРУКТУРНЫЙ СДВИГ"
        
        return (
            f"<b>{header}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>Актив:</b> <code>{symbol}</code>\n"
            f"Цена: <code>{price:.4f}</code>\n"
            f"VPIN: <code>{vpin_str}</code>\n"
            f"Время: <code>{ts_utc} UTC</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Режим: ADVISORY ONLY</i>"
        )

    async def stop(self):
        self._running = False
        if self._worker_task: self._worker_task.cancel()
        logger.info("🔌 [Telegram] Offline.")
