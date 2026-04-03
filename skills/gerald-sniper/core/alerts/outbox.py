"""
Gerald v4 — Durable Telegram Outbox Queue.

Architecture:
- Persistent SQLite outbox (survives crashes)
- Background worker with exponential retry (5s to 300s)
- Dead-letter queue (DLQ) with TTL (auto-archive after 2 hours)
- Dedup by alert_hash(symbol, type, ts_bucket_15min)
- Shared aiohttp.ClientSession with TCPConnector (keepalive)
- NO silent errors: Strict status validation & message_id check
- Graceful degradation: if TG is down, pipeline continues

Flow:
    Signal → outbox.enqueue(msg, priority) → SQLite
                                              ↓
                                OutboxWorker (async loop)
                                              ↓
                                TelegramTransport.send()
                                         ↓         ↓
                                      SUCCESS    FAILURE
                                         ↓         ↓
                                      DELETE    retry++ / DLQ
"""
import asyncio
import hashlib
import os
import sqlite3
import time
from dataclasses import dataclass, field
from enum import Enum
from loguru import logger
from typing import Optional, Any, List
import json

from core.events.nerve_center import bus
from core.events.events import SignalEvent
from core.alerts.formatters import format_signal_alert


from enum import IntEnum

class MessagePriority(IntEnum):
    LOW = 1       # Health reports, scans
    NORMAL = 2    # Shadow signals, level proximity
    CRITICAL = 3  # APPROVED SIGNAL (Action Required)

class MessageStatus(str, Enum):
    PENDING = "pending"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"
    DLQ = "dlq"
    SILENT_LOG = "silent_log"

@dataclass
class OutboxMessage:
    id: int = 0
    text: str = ""
    parse_mode: str = "HTML"
    disable_notification: bool = False
    reply_to_message_id: Optional[int] = None
    reply_markup: Optional[str] = None  # JSON string
    priority: int = MessagePriority.NORMAL
    status: str = MessageStatus.PENDING
    retries: int = 0
    max_retries: int = 5
    created_at: float = 0.0
    last_attempt: float = 0.0
    alert_hash: str = ""
    error_msg: str = ""
    telegram_msg_id: Optional[int] = None


class TelegramOutbox:
    """
    Durable outbox for Telegram messages with SQLite persistence,
    priority-based dropping (v6.14), and rate-limit guard.
    """

    DB_FILE = "./data_run/outbox.db"
    WORKER_INTERVAL = 2.0       # seconds between batch sends
    BATCH_SIZE = 10             # messages per batch
    DEDUP_WINDOW = 300          # 5 minutes dedup window
    MAX_RETRIES = 5
    TG_RATE_LIMIT_DELAY = 1.1   # Telegram allows ~30 msg/sec to same chat
    MAX_MESSAGES_PER_HOUR = 600 # v5.1: 10 msg/min safe zone

    def __init__(self):
        self._db_path = self.DB_FILE
        self._session = None
        self._running = False
        self._worker_task = None
        self._stats = {
            "sent": 0, "failed": 0, "deduped": 0,
            "dlq": 0, "retries": 0, "throttled": 0, "dropped": 0
        }
        self._hourly_sends: list[float] = [] 
        self._init_db()

    def _init_db(self):
        """Create outbox table if not exists."""
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        conn = sqlite3.connect(self._db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                parse_mode TEXT DEFAULT 'HTML',
                disable_notification INTEGER DEFAULT 0,
                reply_to_message_id INTEGER,
                reply_markup TEXT,
                priority INTEGER DEFAULT 2,
                status TEXT DEFAULT 'pending',
                retries INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 5,
                created_at REAL NOT NULL,
                last_attempt REAL DEFAULT 0,
                alert_hash TEXT DEFAULT '',
                error_msg TEXT DEFAULT '',
                telegram_msg_id INTEGER
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, priority, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_hash ON outbox(alert_hash, created_at)")
        conn.commit()
        conn.close()

    @staticmethod
    def compute_alert_hash(symbol: str = "", alert_type: str = "", extra: str = "") -> str:
        ts_bucket = int(time.time() // 300) * 300  # 5-min bucket
        raw = f"{symbol}|{alert_type}|{extra}|{ts_bucket}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def enqueue(
        self,
        text: str,
        priority: int = MessagePriority.NORMAL,
        disable_notification: bool = False,
        parse_mode: str = "HTML",
        reply_to_message_id: Optional[int] = None,
        reply_markup: Optional[str] = None,
        alert_hash: str = "",
        status: str = "pending",
    ) -> bool:
        """
        Add a message to the outbox.
        [Audit 6.14] Dropper Logic: If tokens are low, discard LOW priority messages.
        """
        now = time.time()
        
        # 1. Dropper Logic: Protect rate limits for CRITICAL signals
        # Purge old stamps to get accurate count
        while self._hourly_sends and self._hourly_sends[0] < now - 3600:
            self._hourly_sends.pop(0)
            
        consumption = len(self._hourly_sends)
        # If we reached 80% of hourly limit, drop LOW priority immediately
        if priority == MessagePriority.LOW and consumption > (self.MAX_MESSAGES_PER_HOUR * 0.8):
            self._stats["dropped"] += 1
            logger.debug(f"🗑️ [Dropper] Discarding LOW alert to save capacity ({consumption}/{self.MAX_MESSAGES_PER_HOUR})")
            return False

        # 2. Dedup check
        if alert_hash:
            conn = sqlite3.connect(self._db_path)
            cursor = conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE alert_hash = ? AND created_at > ?",
                (alert_hash, now - self.DEDUP_WINDOW),
            )
            count = cursor.fetchone()[0]
            conn.close()
            if count > 0:
                self._stats["deduped"] += 1
                logger.debug(f"♻️ Outbox dedup: hash={alert_hash}")
                return False

        # 3. Insert
        conn = sqlite3.connect(self._db_path)
        conn.execute(
            """INSERT INTO outbox
               (text, parse_mode, disable_notification, reply_to_message_id,
                reply_markup, priority, status, retries, max_retries,
                created_at, alert_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""",
            (
                text, parse_mode, int(disable_notification),
                reply_to_message_id, reply_markup,
                int(priority), status,
                self.MAX_RETRIES, now, alert_hash,
            ),
        )
        conn.commit()
        conn.close()
        return True

    def _fetch_pending(self, limit: int = 5) -> list[OutboxMessage]:
        """Fetch pending messages ordered by priority then age."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT * FROM outbox
               WHERE status IN ('pending', 'failed')
                 AND retries < max_retries
               ORDER BY priority DESC, created_at ASC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        conn.close()

        messages = []
        for r in rows:
            msg = OutboxMessage(
                id=r["id"],
                text=r["text"],
                parse_mode=r["parse_mode"],
                disable_notification=bool(r["disable_notification"]),
                reply_to_message_id=r["reply_to_message_id"],
                reply_markup=r["reply_markup"],
                priority=r["priority"],
                status=r["status"],
                retries=r["retries"],
                max_retries=r["max_retries"],
                created_at=r["created_at"],
                last_attempt=r["last_attempt"],
                alert_hash=r["alert_hash"],
                error_msg=r["error_msg"],
                telegram_msg_id=r["telegram_msg_id"],
            )
            messages.append(msg)
        return messages

    def _update_status(
        self,
        msg_id: int,
        status: str,
        error_msg: str = "",
        telegram_msg_id: Optional[int] = None,
        increment_retry: bool = False,
    ):
        """Update message status in DB."""
        conn = sqlite3.connect(self._db_path)
        if increment_retry:
            conn.execute(
                """UPDATE outbox SET status=?, error_msg=?, telegram_msg_id=?,
                   last_attempt=?, retries = retries + 1
                   WHERE id=?""",
                (status, error_msg, telegram_msg_id, time.time(), msg_id),
            )
        else:
            conn.execute(
                """UPDATE outbox SET status=?, error_msg=?, telegram_msg_id=?,
                   last_attempt=?
                   WHERE id=?""",
                (status, error_msg, telegram_msg_id, time.time(), msg_id),
            )
        conn.commit()
        conn.close()

    async def _send_one(self, msg: OutboxMessage) -> tuple[bool, str, Optional[int]]:
        """
        Actually send one message to Telegram.
        Returns (success, error_msg, telegram_message_id).
        """
        import aiohttp
        from utils.config import config

        bot_token = config.telegram.bot_token
        chat_id = config.telegram.chat_id

        if not bot_token or not chat_id:
            return False, "Bot token or chat_id not configured", None

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": msg.text,
            "parse_mode": msg.parse_mode,
            "disable_web_page_preview": True,
            "disable_notification": msg.disable_notification,
        }
        if msg.reply_to_message_id:
            payload["reply_to_message_id"] = msg.reply_to_message_id
        if msg.reply_markup:
            import json
            payload["reply_markup"] = json.loads(msg.reply_markup)

        try:
            if self._session is None or self._session.closed:
                from aiohttp import TCPConnector
                connector = TCPConnector(
                    limit=10, 
                    ttl_dns_cache=300, 
                    use_dns_cache=True,
                    keepalive_timeout=30
                )
                timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    trust_env=True
                )

            proxy = getattr(config.telegram, "proxy_url", None) or os.getenv("PROXY_URL")
            
            # Gektor Guard: Verify proxy syntax
            if proxy and not proxy.startswith(("http://", "https://")):
                proxy = f"http://{proxy}"

            async with self._session.post(url, json=payload, proxy=proxy, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ok"):
                        tg_msg_id = data.get("result", {}).get("message_id")
                        return True, "", tg_msg_id
                    else:
                        error_msg = data.get("description", "Unknown TG Error")
                        return False, f"TG API Error: {error_msg}", None
                elif resp.status == 429:
                    # Rate limited by Telegram
                    retry_after = 5
                    try:
                        data = await resp.json()
                        retry_after = data.get("parameters", {}).get("retry_after", 5)
                    except Exception:
                        pass
                    return False, f"TG rate limited (429). Retry after {retry_after}s", None
                else:
                    error_text = await resp.text()
                    return False, f"TG HTTP {resp.status}: {error_text[:200]}", None

        except asyncio.TimeoutError:
            return False, "TG request timeout", None
        except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, aiohttp.ClientDisconnectedError, OSError) as e:
            return False, f"TG network: {type(e).__name__}: {str(e)[:100]}", None
        except Exception as e:
            return False, f"TG unexpected: {type(e).__name__}: {str(e)[:100]}", None

    async def _worker_loop(self):
        """Background worker that processes the outbox."""
        logger.info("📬 Outbox Worker started")
        last_stats_log = 0

        while self._running:
            try:
                # Periodic stats logging (every 60s)
                if time.time() - last_stats_log > 60:
                    stats = self.get_stats()
                    logger.info(
                        f"📬 [Outbox Health] Pending: {stats['pending_in_queue']} | "
                        f"Sent: {stats['sent']} | DLQ: {stats['dlq_total']} | "
                        f"Hourly: {len(self._hourly_sends)}/{self.MAX_MESSAGES_PER_HOUR}"
                    )
                    last_stats_log = time.time()

                messages = self._fetch_pending(limit=self.BATCH_SIZE)
                if not messages:
                    await asyncio.sleep(self.WORKER_INTERVAL)
                    continue

                for msg in messages:
                    # v4.1: GLOBAL HOURLY CAP
                    now_ts = time.time()
                    # Purge entries older than 1 hour
                    while self._hourly_sends and self._hourly_sends[0] < now_ts - 3600:
                        self._hourly_sends.pop(0)
                    
                    if len(self._hourly_sends) >= self.MAX_MESSAGES_PER_HOUR:
                        # Only CRITICAL bypasses the hourly limit
                        if int(msg.priority) < MessagePriority.CRITICAL:
                            self._update_status(msg.id, MessageStatus.DLQ, error_msg="Hourly cap reached")
                            self._stats["throttled"] += 1
                            logger.debug(f"🚫 Outbox hourly cap: skipped #{msg.id} (priority={msg.priority})")
                            continue
                    # Rule 2.4: Exponential backoff check (Hardened 5s -> 300s)
                    if msg.retries > 0:
                        delay = min(300, (2 ** (msg.retries - 1)) * 10) # 10, 20, 40, 80, 160, 300
                        if now_ts - msg.last_attempt < delay:
                            continue
                    
                    success, error, tg_msg_id = await self._send_one(msg)

                    if success:
                        self._update_status(
                            msg.id, MessageStatus.SENT,
                            telegram_msg_id=tg_msg_id,
                        )
                        self._hourly_sends.append(time.time())
                        self._stats["sent"] += 1
                        logger.debug(f"📤 Outbox sent #{msg.id} (tg_id={tg_msg_id}, hour={len(self._hourly_sends)}/{self.MAX_MESSAGES_PER_HOUR})")
                    else:
                        new_retries = msg.retries + 1
                        if new_retries >= msg.max_retries:
                            self._update_status(
                                msg.id, MessageStatus.DLQ,
                                error_msg=error,
                                increment_retry=True,
                            )
                            self._stats["dlq"] += 1
                            logger.error(
                                f"💀 Outbox DLQ #{msg.id}: {error}"
                            )
                        else:
                            self._update_status(
                                msg.id, MessageStatus.FAILED,
                                error_msg=error,
                                increment_retry=True,
                            )
                            self._stats["retries"] += 1
                            logger.warning(
                                f"🔄 Outbox retry #{msg.id} "
                                f"({new_retries}/{msg.max_retries}): {error}"
                            )

                    # Respect TG rate limit
                    await asyncio.sleep(self.TG_RATE_LIMIT_DELAY)

            except Exception as e:
                logger.error(f"📬 Outbox Worker error: {e}")
                await asyncio.sleep(5)

    async def start(self):
        """Start the outbox background worker."""
        self.cleanup_stale_dlq()
        self._running = True
        self._worker_task = asyncio.create_task(
            self._worker_loop(), name="outbox_worker"
        )
        # Audit 24.5: Re-subscribing to SignalEvent for HIL approval
        from core.events.events import SignalEvent, AlertEvent
        bus.subscribe(SignalEvent, self._handle_signal_event)
        bus.subscribe(AlertEvent, self._handle_alert_event)
        logger.info("📬 [Outbox] Subscribed to SignalEvent & AlertEvent")

    async def _handle_signal_event(self, event: SignalEvent):
        """Automatically enqueues an alert when a signal is approved."""
        try:
            # Mobile Sniper Spec (v5.2): Strict Quality Guard
            score = event.confidence * 100 if event.confidence <= 1.0 else event.confidence
            score_int = int(score)
            
            # Gektor Rule: Active alerts ONLY for Score >= 90.
            if score_int < 90:
                logger.debug(f"🔇 [Outbox] Signal {event.symbol} score {score_int} < 90. Suppressed (Task 5).")
                return

            # Task 5.5: Imbalance Guard (> 1.2)
            imbalance = event.metadata.get('imbalance', 1.0)
            if imbalance < 1.2:
                logger.debug(f"🔇 [Outbox] Signal {event.symbol} imbalance {imbalance} < 1.2. Suppressed.")
                return
            
            from core.alerts.formatters import format_signal_alert
            msg, buttons = format_signal_alert(event)
            
            alert_hash = self.compute_alert_hash(
                event.symbol, "SIGNAL_MOBILE", str(score_int)
            )
            
            # For Score > 90, we always want sound and high priority
            self.enqueue(
                text=msg,
                priority=MessagePriority.CRITICAL,
                disable_notification=False,
                reply_markup=buttons,
                alert_hash=alert_hash,
                status="pending"
            )
            logger.info(f"📬 [Mobile Sniper] ALERT enqueued for {event.symbol} (Score: {score_int})")
        except Exception as e:
            logger.error(f"❌ [Outbox] Error handling SignalEvent: {e}")

    async def _handle_alert_event(self, event: Any):
        """Handler for General AlertEvents."""
        try:
            msg = f"<b>{event.title}</b>\n\n{event.message}"
            self.enqueue(
                text=msg,
                priority=getattr(event, 'priority', MessagePriority.NORMAL),
            )
            logger.info(f"📬 [Outbox] General Alert enqueued: {event.title}")
        except Exception as e:
            logger.error(f"❌ [Outbox] Error handling AlertEvent: {e}")

    def cleanup_stale_dlq(self):
        """Archives stale messages in DLQ to prevent blocking startup (TTL check)."""
        now = time.time()
        conn = sqlite3.connect(self._db_path)
        # Gektor Rule: DLQ items older than 2 hours are purged
        cursor = conn.execute(
            "DELETE FROM outbox WHERE status='dlq' AND last_attempt < ?",
            (now - 7200,)
        )
        count = cursor.rowcount
        # Also cleanup extremely old sent messages
        conn.execute("DELETE FROM outbox WHERE status='sent' AND created_at < ?", (now - 86400,))
        conn.commit()
        conn.close()
        if count > 0:
            logger.info(f"📬 [Outbox] Startup: cleared {count} stale DLQ messages (TTL Exceeded)")

    async def stop(self):
        """Graceful shutdown."""
        self._running = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("📬 Outbox Worker stopped")

    def get_stats(self) -> dict:
        """Returns outbox statistics for health monitoring."""
        conn = sqlite3.connect(self._db_path)
        pending = conn.execute(
            "SELECT COUNT(*) FROM outbox WHERE status='pending'"
        ).fetchone()[0]
        failed = conn.execute(
            "SELECT COUNT(*) FROM outbox WHERE status='failed'"
        ).fetchone()[0]
        dlq = conn.execute(
            "SELECT COUNT(*) FROM outbox WHERE status='dlq'"
        ).fetchone()[0]
        conn.close()

        return {
            **self._stats,
            "pending_in_queue": pending,
            "failed_in_queue": failed,
            "dlq_total": dlq,
            "dropped_total": self._stats.get("dropped", 0)
        }

    def cleanup_old(self, max_age_hours: int = 48):
        """Remove sent/DLQ messages older than max_age_hours."""
        cutoff = time.time() - (max_age_hours * 3600)
        conn = sqlite3.connect(self._db_path)
        conn.execute(
            "DELETE FROM outbox WHERE status IN ('sent', 'dlq') AND created_at < ?",
            (cutoff,),
        )
        conn.commit()
        conn.close()


# ─────────────────────────────────────────────────
#  Global singleton
# ─────────────────────────────────────────────────
telegram_outbox = TelegramOutbox()
