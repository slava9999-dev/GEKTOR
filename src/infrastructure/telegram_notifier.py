# src/infrastructure/telegram_notifier.py
from sqlalchemy import text
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
from src.infrastructure.database import DatabaseManager
from src.domain.entities.events import ExecutionEvent, ConflatedEvent

class TelegramRadarNotifier:
    """[GEKTOR v2.0] Institutional Telegram Client with PSR Protocol."""
    def __init__(self, db_manager: DatabaseManager, bot_token: str, chat_id: str, event_bus: Any = None, proxy_url: Optional[str] = None):
        self.db = db_manager
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.proxy_url = proxy_url
        self.bus = event_bus
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        
        # [PSR 4.0] TRANSACTIONAL OUTBOX (Persistence > Speed)
        self._wakeup_event = asyncio.Event()
        self._live_allowed = asyncio.Event()
        self._live_allowed.set()
        self._is_throttled = False
        
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
        """[PSR 4.0] Background Egress Worker."""
        if self.proxy_url:
            masked_proxy = self.proxy_url.split('@')[-1] if '@' in self.proxy_url else self.proxy_url
            logger.info(f"📡 [Telegram] Worker process started using proxy: {masked_proxy}")
        else:
            logger.warning("📡 [Telegram] Worker process started WITHOUT proxy (Direct connection).")
            
        connector = ProxyConnector.from_url(self.proxy_url) if self.proxy_url else None
        timeout = aiohttp.ClientTimeout(total=20)
        
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                self._session = session
                logger.debug("📡 [Telegram] Session active. Running recovery...")
                await self._run_recovery_phase()
                
                while self._running:
                    try:
                        await asyncio.wait_for(self._wakeup_event.wait(), timeout=10.0)
                        self._wakeup_event.clear()
                    except asyncio.TimeoutError:
                        pass 
                    
                    await self._live_allowed.wait()
                    
                    if self._is_throttled:
                        await asyncio.sleep(5)
                        continue
                        
                    # Fetch PENDING alerts
                    async with self.db.engine.connect() as conn:
                        result = await conn.execute(text(
                            "SELECT id, message, priority FROM outbox "
                            "WHERE status = 'PENDING' "
                            "ORDER BY priority ASC, created_at ASC LIMIT 10"
                        ))
                        pending_alerts = result.all()

                    if pending_alerts:
                        logger.debug(f"📡 [Telegram] Found {len(pending_alerts)} pending alerts.")
                        for alert_id, message, priority in pending_alerts:
                            logger.info(f"📡 [Telegram] Attempting delivery of alert {alert_id} (Priority {priority})...")
                            success = await self._send_with_retry(message, {"id": alert_id})
                            if success:
                                await self.db.push_query(
                                    "UPDATE outbox SET status = 'SENT' WHERE id = :id",
                                    {"id": alert_id}
                                )
                                logger.success(f"✅ [Telegram] Alert {alert_id} DELIVERED.")
                                await asyncio.sleep(0.3)
                            else:
                                logger.error(f"❌ [Telegram] Alert {alert_id} DELIVERY FAILED.")
                                if self._is_throttled:
                                     break
                    
                    await asyncio.sleep(1.0) 
        except Exception as e:
            logger.opt(exception=True).error(f"☠️ [Telegram] Worker CRASHED: {e}")
        finally:
            logger.warning("🔌 [Telegram] Worker stopped.")

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
                        self._is_throttled = True
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        logger.warning(f"🚨 [Telegram] Rate Limit! Backing off for {retry_after}s.")
                        await asyncio.sleep(retry_after)
                        return False # Worker will retry later
                    resp.raise_for_status()
                    self._is_throttled = False
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
        """[PSR 4.0] Transactional Atomic Alerting."""
        priority = 2 # Normal Entry
        if event_data.get("abort_mission"):
            priority = 1 # High Priority Exit
        
        message = self._format_message(event_data)
        
        # Write to Outbox (Atomic Disk Persistence)
        if self.bus:
            self.bus.publish_fire_and_forget(self.db.push_query(
                "INSERT INTO outbox (message, priority, status) VALUES (:msg, :pri, 'PENDING')",
                {"msg": message, "pri": priority}
            ))
        else:
            asyncio.create_task(self.db.push_query(
                "INSERT INTO outbox (message, priority, status) VALUES (:msg, :pri, 'PENDING')",
                {"msg": message, "pri": priority}
            ))
        self._wakeup_event.set()

    async def handle_execution_event(self, event: ExecutionEvent):
        """[EventBus Handler] Converts ExecutionEvent to Telegram notification."""
        data = event.to_dict()
        # Add metadata fields to top level for _format_message
        data.update(event.metadata)
        self.notify_event(data)

    async def handle_conflated_event(self, event: ConflatedEvent):
        """[EventBus Handler] Converts ConflatedEvent to Telegram notification."""
        logger.warning(f"🛡️ [Telegram] Processing CONFLATED signal for {event.symbol} ({event.tick_count} items).")
        data = event.to_dict()
        data["is_conflated"] = True
        self.notify_event(data)

    async def notify_manual(self, text: str):
        """[PSR 4.0] Transactional System Alert. Async awaitable."""
        logger.critical(f"🔔 [ALERT] Queueing manual notification: {text}")
        try:
            await self.db.push_query(
                "INSERT INTO outbox (message, priority, status) VALUES (:msg, 1, 'PENDING')",
                {"msg": text}
            )
            self._wakeup_event.set()
        except Exception as e:
            logger.error(f"Failed to queue manual alert: {e}")

    async def broadcast_offline_sync(self, reason: str) -> None:
        """
        Предсмертный крик. Вызывается из GlobalDeadMansSwitch.
        ОБЯЗАН быть await, никаких fire_and_forget.
        """
        logger.warning(f"🔌 [Telegram] Queueing terminal notification: {reason}")
        payload = f"🔌 [ОФФЛАЙН] Система завершает работу. Причина: {reason}"
        
        # Строгое ожидание записи в БД. Транзакция должна завершиться до остановки Loop'а.
        try:
            await self.db.push_query(
                "INSERT INTO outbox (message, priority, status) VALUES (:msg, 1, 'PENDING')",
                {"msg": payload}
            )
            self._wakeup_event.set()
        except Exception as e:
            logger.error(f"☠️ [FATAL] Failed to save terminal alert to Outbox: {e}")
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
        """[GEKTOR v2.0] Профессиональный формат торговых алертов (Локализация: RU)."""
        symbol = event.get('symbol', 'UNKNOWN')
        price = event.get('price', 0.0)
        vpin = event.get('vpin')
        vpin_str = f"{vpin:.4f}" if vpin is not None else "Н/Д"
        ts_utc = datetime.fromtimestamp(event.get('timestamp', time.time()*1000)/1000, tz=timezone.utc).strftime('%H:%M:%S')
        
        # [GEKTOR v2.1] Conflation Data Injection
        conflation_tag = ""
        if event.get("is_conflated"):
            count = event.get("tick_count", 0)
            duration = event.get("duration_ms", 0)
            conflation_tag = f"\n📦 <b>Агрегация:</b> <code>{count} тиков / {duration:.1f}ms</code>"

        # [TYPE 1: ABORT MISSION] Экстренный выход / Невалидная посылка
        if event.get("abort_mission"):
            reason = event.get("abort_reason", "Нарушение структуры")
            # Map technical reasons to user-friendly Russian
            reason_map = {
                "VPIN_DECAY": "Затухание токсичности / Потеря интереса",
                "STOP_LOSS": "Срабатывание ценового стопа",
                "VOLATILITY_EXPANSION": "Взрывной рост волатильности (Риск)",
                "TIME_DECAY": "Истечение времени актуальности"
            }
            friendly_reason = reason_map.get(reason, reason)
            
            return (
                f"🛡️ <b>[ЗАЩИТА КАПИТАЛА: СБРОС]</b>\n"
                f"Пресечение риска по активу <b>{symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
                f"📉 <b>Тип:</b> Снятие торговой гипотезы\n"
                f"🧬 <b>Причина:</b> <i>{friendly_reason}</i>\n"
                f"📊 <b>VPIN:</b> <code>{vpin_str}</code>\n"
                f"💵 <b>Цена:</b> <code>${price:,.2f}</code>\n"
                f"⏰ <b>Время:</b> <code>{ts_utc} UTC</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🛑 <b>СТАТУС: ВЫХОД / OFF-MARKET</b>"
            )

        # [TYPE 2: MICROSTRUCTURE IMPULSE] (OFI)
        if event.get("type") == "MICRO_IMPULSE":
            ofi = event.get("ofi", 0.0)
            side = event.get("side", "NEUTRAL")
            emoji = "📈" if side == "ACCUMULATION" else "📉"
            title = "НАКОПЛЕНИЕ" if side == "ACCUMULATION" else "РАСПРЕДЕЛЕНИЕ"
            description = (
                "Обнаружено доминирование лимитных покупателей (Iceberg/Aggressive)." 
                if side == "ACCUMULATION" else 
                "Обнаружено доминирование лимитных продавцов (Distribution/Pressure)."
            )
            
            return (
                f"{emoji} <b>[МИКРОСТРУКТУРА: {title}]</b>\n"
                f"{description}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
                f"📊 <b>OFI Delta:</b> <code>{ofi:+.2f}</code>\n"
                f"💵 <b>Цена:</b> <code>${price:,.2f}</code>\n"
                f"⏰ <b>Время:</b> <code>{ts_utc} UTC</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📡 <b>СТАТУС: МОНИТОРИНГ ПОДТВЕРЖДЕНИЯ</b>"
            )

        # [TYPE 3: ENTRY SIGNAL] (VPIN Anomaly) - Основной сигнал Альфы
        header = "⏳ [РЕЗЕРВНЫЙ КАНАЛ]" if is_delayed else "⚡ <b>ОБНАРУЖЕНА АНОМАЛИЯ (ALPHA)</b>"
        
        return (
            f"{header}\n"
            f"Институциональное смещение вероятности\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💎 <b>Актив:</b> <code>{symbol}</code>\n"
            f"🎯 <b>Сигнал:</b> ВЕРОЯТНЫЙ ИМПУЛЬС\n"
            f"📊 <b>VPIN (Информационный риск):</b> <code>{vpin_str}</code>\n"
            f"💵 <b>Цена:</b> <code>${price:,.2f}</code>{conflation_tag}\n"
            f"⏰ <b>Время:</b> <code>{ts_utc} UTC</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 <b>СТАТУС: ГОТОВНОСТЬ К ИСПОЛНЕНИЮ</b>"
        )

    async def stop(self):
        self._running = False
        if self._worker_task: self._worker_task.cancel()
        logger.info("🔌 [Telegram] Offline.")
