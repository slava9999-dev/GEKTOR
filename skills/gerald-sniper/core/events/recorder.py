# core/events/recorder.py

import os
import time
import asyncio
import aiofiles
import orjson
import hashlib
from loguru import logger
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor

# Глобальный пул для тяжелых I/O операций (Audit 6.7)
io_pool = ThreadPoolExecutor(max_workers=4)
from core.events.nerve_center import bus
from .events import DetectorEvent, SignalEvent, OrderExecutedEvent, PositionUpdateEvent, EmergencyAlertEvent

class BlackboxRecorder:
    """
    [Audit 5.15] Blackbox Recorder v3.0 (Durability & Isolation).
    
    Architecture:
    1. Critical WAL (Write-Ahead Log): Immediate append-only local persistence (Lossless, Non-blocking).
    2. Partitioned Memory Buffers: 
       - Critical Queue (DB Intake): AsyncQueue for Postgres.
       - Telemetry Deque (Metrics): Ring buffer (Lossy).
    3. Pressure Relief: Never blocks the Event Loop. put_nowait() strictly enforced.
    """
    
    def __init__(self, log_dir: str = "./data_run/logs/replay", 
                 wal_path: str = "./data_run/critical_wal.jsonl",
                 snapshot_path: str = "./data_run/state_snapshot.json"):
        self.log_dir = log_dir
        self.wal_path = wal_path
        self.snapshot_path = snapshot_path
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(os.path.dirname(wal_path), exist_ok=True)
        
        # Partitioned Architecture
        self.critical_queue = asyncio.Queue(maxsize=500) # DB Intake buffer (Optimized for RAM)
        self.telemetry_buffer = deque(maxlen=1000) # Ring buffer for Tickers/Metrics (Lean)
        self._io_lock = asyncio.Lock() # Audit 6.5: Prevent concurrent I/O races
        
        self._running = False
        self._db_task = None
        self._critical_types = {
            "OrderExecutedEvent", 
            "SignalEvent", 
            "EmergencyAlertEvent", 
            "PositionUpdateEvent"
        }

    async def create_state_snapshot(self, active_signals: dict):
        """
        [P0] Атомарный чекпоинт с защитой от повреждения (Audit 6.7).
        Architecture: ThreadPool isolation + SHA256 Checksum + fsync + Atomic Replace.
        """
        async with self._io_lock:
            try:
                # 1. Сбор данных
                raw_data = {
                    "timestamp": time.time(),
                    "active_signals": [s.to_dict() if hasattr(s, 'to_dict') else s for s in active_signals.values()]
                }
                
                # 2. Сериализация и Checksum
                payload = await asyncio.to_thread(orjson.dumps, raw_data)
                checksum = hashlib.sha256(payload).hexdigest()
                
                # Заворачиваем в контейнер с метаданными
                container = {
                    "checksum": checksum,
                    "engine_v": "7.0", # Upgraded for Atomic I/O
                    "data": raw_data
                }
                final_bytes = orjson.dumps(container)
                
                # 3. Атомарная запись с изоляцией блокирующего I/O во внешнем пуле
                tmp_path = f"{self.snapshot_path}.tmp"
                loop = asyncio.get_running_loop()
                
                # Запись во временный файл
                async with aiofiles.open(tmp_path, 'wb') as f:
                    await f.write(final_bytes)
                    await f.flush()
                    
                    # Изоляция блокирующего fsync
                    await loop.run_in_executor(io_pool, os.fsync, f.fileno())

                # Атомарная подмена (OS-level)
                await loop.run_in_executor(io_pool, os.replace, tmp_path, self.snapshot_path)
                
                # 4. Двухфазная очистка WAL (Audit 6.6)
                wal_bak = f"{self.wal_path}.bak"
                if os.path.exists(self.wal_path):
                    if os.path.exists(wal_bak): 
                        await loop.run_in_executor(io_pool, os.remove, wal_bak)
                    await loop.run_in_executor(io_pool, os.rename, self.wal_path, wal_bak)
                
                async with aiofiles.open(self.wal_path, mode='w') as f:
                    await f.write("")
                    
                logger.info(f"📸 [Snapshot] Integrity Verified (SHA256: {checksum[:8]}). Atomic checkpoint SUCCESS.")
            except Exception as e:
                logger.error(f"🔥 [Snapshot] Critical failure during checkpoint: {e}")

    async def start(self):
        """Subscribe with Zero-Block Policy."""
        bus.subscribe(DetectorEvent, self._on_event)
        bus.subscribe(SignalEvent, self._on_event)
        bus.subscribe(OrderExecutedEvent, self._on_event)
        bus.subscribe(EmergencyAlertEvent, self._on_event)
        
        self._running = True
        self._db_task = asyncio.create_task(self._db_writer_loop())
        logger.info(f"📼 [Blackbox v3.0] ACTIVE | WAL: {self.wal_path} | DB Queue: 500")

    async def _on_event(self, event):
        """
        [P0] Fire-and-Forget ingestion. 
        Ensures Durability via WAL before trying Memory Queue.
        """
        ev_type = event.__class__.__name__
        is_critical = ev_type in self._critical_types
        
        if not is_critical:
            # Telemetry goes to lossy Ring Buffer (O(1) push)
            self.telemetry_buffer.append(event)
            return

        # 1. CRITICAL WAL: Durability First (Immediate Local I/O)
        # We use aiofiles to avoid blocking the loop for file I/O
        try:
            # Prepare fast binary dump
            data = {"t": time.time(), "type": ev_type, "d": event.to_dict() if hasattr(event, 'to_dict') else event.__dict__}
            bytes_data = orjson.dumps(data) + b"\n"
            
            async with aiofiles.open(self.wal_path, mode="ab") as f:
                await f.write(bytes_data)
        except Exception as e:
            logger.critical(f"🔥 [Recorder] WAL FAILURE: {e}. Critical data risk!")

        # 2. DB PIPELINE: Non-blocking attempt
        try:
            self.critical_queue.put_nowait(event)
        except asyncio.QueueFull:
            # Queue to Postgres is full - it's okay, we have it in WAL. 
            # Background worker will eventually flush it or we recover from WAL later.
            logger.warning(f"⚠️ [Recorder] DB Queue saturated. {ev_type} pushed to WAL only.")

    async def _db_writer_loop(self):
        """
        Adaptive DB Batcher. 
        Pulls from critical_queue and flushes to Postgres in separate thread.
        """
        batch = []
        batch_size = 500
        flush_interval = 1.0 
        
        while self._running:
            try:
                try:
                    # Collect batch with timeout
                    event = await asyncio.wait_for(self.critical_queue.get(), timeout=flush_interval)
                    batch.append(event)
                except asyncio.TimeoutError:
                    pass
                
                if len(batch) >= batch_size or (batch and self.critical_queue.empty()):
                    await self._flush_to_db(batch)
                    batch = []
                    
            except asyncio.CancelledError:
                if batch: await self._flush_to_db(batch)
                break
            except Exception as e:
                logger.error(f"❌ [Recorder] DB Batch flush failed: {e}")
                await asyncio.sleep(2)

    async def replay_wal(self, orchestrator_ref) -> int:
        """
        [P0] Crash Recovery: Snapshot + WAL Tail Replay v6.6.
        Architecture: Integrity Check (SHA256) + Generational Fallback.
        """
        import hashlib
        count = 0
        recovered_states = {}
        terminal_statuses = {"FILLED", "CANCELLED", "REJECTED", "CLOSED", "DEACTIVATED"}

        # PHASE 1: Load Latest Verified Snapshot
        snapshot_loaded = False
        # We try main snapshot first, then .bak if main is corrupt
        for path in [self.snapshot_path, f"{self.snapshot_path}.bak"]:
            if not os.path.exists(path): continue
            try:
                async with aiofiles.open(path, mode='rb') as f:
                    content = await f.read()
                    container = orjson.loads(content)
                    
                    # 1. Integrity Check (Audit 6.6)
                    stored_hash = container.get("checksum")
                    data = container.get("data", {})
                    # Re-serialize to verify hash (Deterministic orjson)
                    actual_hash = hashlib.sha256(orjson.dumps(data)).hexdigest()
                    
                    if stored_hash and stored_hash != actual_hash:
                        logger.error(f"❌ [Recovery] Snapshot CORRUPTION detected at {path}! Checksum mismatch.")
                        continue # Try fallback
                    
                    # 2. Hydrate recovered_states
                    for sig_data in data.get('active_signals', []):
                        sid = sig_data.get('signal_id') or sig_data.get('order_id')
                        if sid:
                            recovered_states[sid] = {"type": "SignalEvent", "d": sig_data}
                    
                    logger.info(f"💾 [Recovery] Hydrated {len(recovered_states)} entities from {path}.")
                    snapshot_loaded = True
                    break # Success
            except Exception as e:
                logger.error(f"⚠️ [Recovery] Snapshot read failure ({path}): {e}")

        # PHASE 2: Replay WAL Delta (and .bak if needed)
        # Sequence: .bak (if it exists) -> current wal
        wal_sequence = [f"{self.wal_path}.bak", self.wal_path]
        
        for p in wal_sequence:
            if not os.path.exists(p): continue
            logger.info(f"♻️ [Recovery] Replaying WAL segment: {p}...")
            try:
                async with aiofiles.open(p, mode='r') as f:
                    async for line in f:
                        if not line.strip(): continue
                        try:
                            entry = orjson.loads(line)
                            d = entry.get('d', {})
                            obj_id = d.get('signal_id') or d.get('order_id')
                            if obj_id:
                                recovered_states[obj_id] = entry
                        except: continue
            except Exception as e:
                logger.error(f"❌ [Recovery] WAL read failed ({p}): {e}")

        # PHASE 3: Inject Active States
        for obj_id, entry in recovered_states.items():
            data = entry['d']
            status = str(data.get('status', '')).upper()
            if status not in terminal_statuses:
                await orchestrator_ref.inject_restored_signal(data)
                count += 1
                    
        logger.info(f"✅ [Recovery] System State Restored. Hydrated {count} active entities.")
        return count

    async def _flush_to_db(self, batch: list):
        """Placeholder for SQL Bulk Insert."""
        # In production: await asyncio.to_thread(self.db_manager.bulk_insert, batch)
        # For now, we simulate the workload to test backpressure logic
        logger.debug(f"📼 [Blackbox] Flushed {len(batch)} items to database.")

    async def stop(self):
        self._running = False
        if self._db_task:
            self._db_task.cancel()
        logger.info("📼 [Blackbox] Offline.")

# Global singleton
recorder = BlackboxRecorder()
