import asyncio
import sqlite3
import time
from loguru import logger
from typing import List, Dict, Optional

class PersistentSignalDispatcher:
    """
    [GEKTOR v14.2] Institutional Monitoring & Audit Core.
    Handles high-concurrency alerts with SQLite WAL and Batching.
    Implements 'Pulse' Heartbeat to prevent 'Silent Death' syndrome.
    """
    def __init__(self, bot_token: str, chat_id: str, db_path: str = "local_sentinel.db"):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.db_path = db_path
        self._init_db()
        
        self.queue = asyncio.Queue(maxsize=200)
        self._pending_signals: List[Dict] = []
        self._flush_event = asyncio.Event()
        self.is_running = True
        self.last_pulse_time = time.monotonic()

    def _init_db(self):
        """ Включение режима WAL для параллельной записи/чтения без блокировок """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;") # Оптимизация скорости при достаточной надежности
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alert_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_unix INTEGER,
                    symbol TEXT,
                    z_score REAL,
                    side TEXT,
                    conf REAL,
                    raw_data TEXT
                )
            """)

    async def start(self):
        """ Запуск контура оповещений v14.2 """
        logger.info("🛡️ [SENTINEL] Persistent Dispatcher ONLINE (Storage: SQLite WAL).")
        asyncio.create_task(self._main_loop())
        asyncio.create_task(self._pulse_worker())

    def publish_signal(self, anomaly: Dict):
        """ Мгновенная инъекция сигнала в очередь """
        try:
            self.queue.put_nowait(anomaly)
            # Если очередь начала набиваться слишком быстро (>10 за тик), форсируем сброс
            if self.queue.qsize() >= 10:
                self._flush_event.set()
        except asyncio.QueueFull:
            logger.critical("🚨 [OVERLOAD] Signal Queue FULL! History might be lost.")

    async def _main_loop(self):
        """ 
        [SMART BATCHING] 
        Consolidates signals using either Time-out or Event-trigger.
        """
        while self.is_running:
            try:
                # Ждем либо 3 секунды, либо сигнала о переполнении
                await asyncio.wait_for(self._flush_event.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
            
            # Собираем всё, что есть в очереди на данный момент
            to_process = []
            while not self.queue.empty():
                to_process.append(self.queue.get_nowait())
                self.queue.task_done()
            
            if to_process:
                await self._process_batch(to_process)
            
            self._flush_event.clear()

    async def _process_batch(self, batch: List[Dict]):
        # 1. Атомарная запись в Audit Log (в фоне, чтобы не тормозить цикл)
        await asyncio.to_thread(self._save_to_db, batch)
        
        # 2. Формирование и отправка кластера в Telegram
        await self._send_telegram_cluster(batch)
        
        self.last_pulse_time = time.monotonic()

    def _save_to_db(self, batch: List[Dict]):
        """ Батч-инсерт в SQLite (Audit Log) """
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT INTO alert_history (ts_unix, symbol, z_score, side, conf, raw_data) VALUES (?, ?, ?, ?, ?, ?)",
                [(int(time.time()), s['symbol'], s['z_score'], s['recommended_side'], s['confidence'], str(s)) for s in batch]
            )

    async def _send_telegram_cluster(self, batch: List[Dict]):
        if not batch: return
        
        # Контекстный анализ кластера
        sides = [s['recommended_side'] for s in batch]
        majority_side = max(set(sides), key=sides.count)
        sentiment = "🔴 BEARISH" if majority_side == "SHORT" else "🟢 BULLISH"
        
        header = f"🚨 *[GEKTOR APEX]* Anomaly Cluster ({len(batch)} assets)\n"
        header += f"🔥 Sentiment: {sentiment} ({len(batch)} assets)\n\n"
        
        lines = [f"• `{s['symbol']}` | {s['recommended_side']} | Z: `{s['z_score']:.2f}`" for s in batch[:25]]
        body = "\n".join(lines)
        if len(batch) > 25:
            body += f"\n_...and {len(batch)-25} more (check Dashboard)_"
        
        await self._dispatch_to_tg(header + body)

    async def _pulse_worker(self):
        """ 
        [HEALTH MONITORING] 
        Отправляет Heartbeat если сигналов не было долгое время.
        Предотвращает 'Тихое Умирание'.
        """
        while self.is_running:
            await asyncio.sleep(600) # Проверка каждые 10 минут
            if time.monotonic() - self.last_pulse_time > 1800: # 30 минут тишины
                await self._dispatch_to_tg("📡 [SYSTEM OK] Gektor is on watch. Markets: QUIET.")
                self.last_pulse_time = time.monotonic()

    async def _dispatch_to_tg(self, text: str):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=8) as resp:
                    if resp.status == 200:
                        logger.success("📤 [TG] Dispatch Success.")
        except Exception as e:
            logger.error(f"💀 [TG FAILED] {e}")

logger.info("🛡️ [Sentinel] Core v14.2 Persistent & Heartbeat READY.")
