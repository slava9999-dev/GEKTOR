# skills/gerald_sniper/core/realtime/persistence.py
import asyncio
import json
import hashlib
from typing import Optional
from loguru import logger
from sqlalchemy import text
from .dto import SignalDTO, TelegramFormatter

class InfrastructureStateRepo:
    """
    [GEKTOR v21.49] Репозиторий для атомарного сохранения математического стейта и сигналов.
    Реализует паттерн Transactional Outbox.
    """
    def __init__(self, db_manager):
        self.db = db_manager

    async def persist_state_and_signal(self, symbol: str, vpin: float, sigma: float, signal: Optional[SignalDTO]):
        """
        Атомарно сохраняет стейт VPIN и ставит сигнал в очередь.
        Гарантирует, что при краше мы восстановим точно ту же точку VPIN.
        """
        async with self.db.engine.begin() as conn:
            # 1. Институциональное сохранение стейта (Causal Recovery)
            await conn.execute(
                text("""
                    INSERT INTO radar_states (instrument, vpin, sigma, updated_at)
                    VALUES (:s, :v, :sig, NOW())
                    ON CONFLICT (instrument) DO UPDATE SET vpin = EXCLUDED.vpin, sigma = EXCLUDED.sigma, updated_at = NOW();
                """),
                {"s": symbol, "v": vpin, "sig": sigma}
            )

            # 2. Transactional Outbox
            if signal:
                payload = signal.model_dump()
                await conn.execute(
                    text("""
                        INSERT INTO outbox_events (symbol, event_type, payload, status, channel, prob, vol)
                        VALUES (:s, :t, :p, 'PENDING', 'telegram', :pr, :v)
                    """),
                    {
                        "s": signal.symbol,
                        "t": f"SIGNAL_{signal.side}",
                        "p": json.dumps(payload),
                        "pr": signal.accuracy,
                        "v": signal.sigma_pct / 100
                    }
                )
        logger.debug(f"📊 [Repo] State {symbol} persisted. VPIN: {vpin:.4f}")

class TelegramRelayWorker:
    """
    Отказоустойчивый воркер с механизмом дедупликации (Idempotency Guard).
    """
    def __init__(self, db_manager, tg_client):
        self.db = db_manager
        self.tg = tg_client

    async def run_daemon(self):
        logger.info("📡 [Relay] Outbox Daemon started with Idempotency Guard.")
        while True:
            try:
                await self._process_batch()
            except Exception as e:
                logger.error(f"❌ [Relay] Batch Error: {e}")
            await asyncio.sleep(0.5)

    async def _process_batch(self):
        """Обработка очереди с защитой от дублей."""
        async with self.db.engine.begin() as conn:
            # 1. Забираем сигнал
            result = await conn.execute(text("""
                SELECT id, symbol, payload FROM outbox_events 
                WHERE status = 'PENDING' 
                ORDER BY created_at ASC 
                LIMIT 1 FOR UPDATE SKIP LOCKED;
            """))
            record = result.mappings().fetchone()
            
            if not record:
                return

            event_id = record['id']
            payload = record['payload']
            
            try:
                dto = SignalDTO(**payload)
                
                # 2. ГЕНЕРАЦИЯ ИДЕМПОТЕНТНОГО КЛЮЧА
                # Создаем отпечаток: Символ + Сторона + Округленная цена + 5-минутное окно
                # Это гарантирует, что даже при обрыве сети мы не отправим дважды один и тот же импульс.
                time_window = int(asyncio.get_event_loop().time() / 300) 
                fingerprint_raw = f"{dto.symbol}:{dto.side}:{round(dto.price, 2)}:{time_window}"
                fingerprint = hashlib.sha256(fingerprint_raw.encode()).hexdigest()

                # 3. ПРОВЕРКА И ЛОК ФИНГЕРПРИНТА
                check = await conn.execute(
                    text("SELECT 1 FROM delivered_fingerprints WHERE fingerprint = :f"),
                    {"f": fingerprint}
                )
                
                if check.fetchone():
                    logger.warning(f"🚫 [Relay] Duplicate detected for {dto.symbol}. Skipping.")
                    await conn.execute(
                        text("UPDATE outbox_events SET status = 'SUPERSEDED' WHERE id = :id"),
                        {"id": event_id}
                    )
                    return

                # 4. РЕГИСТРАЦИЯ ОТПРАВКИ (BEFORE IO - At-Most-Once)
                await conn.execute(
                    text("INSERT INTO delivered_fingerprints (fingerprint, event_id) VALUES (:f, :eid)"),
                    {"f": fingerprint, "eid": event_id}
                )
                
                # 5. РЕАЛЬНАЯ ОТПРАВКА (IO)
                alert_text = TelegramFormatter.build_alert(dto)
                success = await self.tg.send_message(alert_text, parse_mode="HTML")
                
                if success:
                    await conn.execute(
                        text("UPDATE outbox_events SET status = 'DELIVERED', published_at = NOW() WHERE id = :id"),
                        {"id": event_id}
                    )
                    logger.success(f"✅ [Relay] Signal {event_id} delivered to Telegram.")
                else:
                    # Если Telegram вернул ошибку (например 429), мы НЕ удаляем фингерпринт, 
                    # чтобы не спамить при ретрае, но помечаем событие как FAILED
                    await conn.execute(
                        text("UPDATE outbox_events SET status = 'FAILED' WHERE id = :id"),
                        {"id": event_id}
                    )

            except Exception as e:
                logger.error(f"⚠️ [Relay] Critical failure for event {event_id}: {e}")
                await conn.execute(
                    text("UPDATE outbox_events SET status = 'ERROR' WHERE id = :id"),
                    {"id": event_id}
                )
