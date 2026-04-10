# core/db/outbox.py
import asyncio
import json
import logging
from datetime import datetime, timezone
import aiosqlite
from loguru import logger

class OutboxRepository:
    """
    [GEKTOR v21.2] Institutional Transactional Outbox Repository.
    Atomic side-effect registration.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def save_signal_with_outbox_and_context(
        self, 
        signal_data: dict, 
        outbox_event: Optional[dict], 
        context: dict,
        fencing_token: int # [GEKTOR v21.15.6] Protection against Zombie-Leader writes
    ):
        """
        Atomic Transaction with Fencing Token (Kleppmann Pattern).
        If another replica took over (Epoch > current_token), the database returns 0 affected rows,
        and this transaction will be rolled back.
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN TRANSACTION;")
            try:
                # 1. Verification Phase: Check if we are still the valid LEADER
                # This check ensures that even during a GC stall, the DB rejects stale writes.
                cursor = await db.execute(
                    "SELECT epoch FROM leadership_status WHERE row_id = 1 LIMIT 1;"
                )
                current_db_epoch = (await cursor.fetchone())[0]
                
                if current_db_epoch != fencing_token:
                    logger.critical(f"🤺 [Fencing] STALE LEADER WRITE DETECTED. Epoch: {fencing_token} < {current_db_epoch}")
                    await db.execute("ROLLBACK;")
                    return False

                # 2. Save Signal Stats (Only if still leading)
                await db.execute(
                    """
                    INSERT INTO signal_stats (signal_id, timestamp, symbol, state, entry_price, detectors)
                    VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        signal_data['id'],
                        datetime.now(timezone.utc).isoformat(),
                        signal_data['symbol'],
                        'PROBABLE',
                        signal_data.get('price'),
                        json.dumps(signal_data.get('detectors', {}))
                    )
                )

                # 3. Register Outbox Item (Kleppmann: Transactional Outbox)
                if outbox_event:
                    await db.execute(
                        """
                        INSERT INTO outbox (event_type, payload, status, epoch)
                        VALUES (?, ?, ?, ?);
                        """,
                        (
                            outbox_event['type'],
                            json.dumps(outbox_event['payload']),
                            'PENDING',
                            fencing_token
                        )
                    )

                # 4. Establish Monitoring Context (Rehydration entry)
                await db.execute(
                    """
                    INSERT INTO monitoring_contexts (signal_id, symbol, state, context)
                    VALUES (?, ?, ?, ?);
                    """,
                    (
                        signal_data['id'],
                        signal_data['symbol'],
                        'ACTIVE',
                        json.dumps(context)
                    )
                )

                await db.commit()
                logger.info(f"📬 [Outbox] Atomic Triple-Persist successful. Epoch: {fencing_token}")
                return True
            except Exception as e:
                await db.execute("ROLLBACK;")
                logger.error(f"📬 [Outbox] Triple-Persist failed or fenced: {e}")
                raise

class CompactingOutboxRelay:
    """
    [GEKTOR v21.15.5] Multi-Lane Outbox Relay with Batch Compactor.
    
    Protects against Telegram 429 (Too Many Requests) and Operator Overload.
    Uses 'Transactional Outbox' pattern (Kleppmann) to ensure At-Least-Once delivery.
    """
    def __init__(self, db_path: str, alert_handler=None, batch_window_ms: int = 500):
        self.db_path = db_path
        self.alert_handler = alert_handler
        self.batch_window = batch_window_ms / 1000.0
        self.storm_threshold = 3
        self.running = True

    async def run(self, shutdown_event: asyncio.Event):
        """Main Loop: Polls, Batches and Delivers."""
        logger.info(f"📬 [Outbox] Compacting Relay started. Window={self.batch_window}s.")
        
        while not shutdown_event.is_set():
            try:
                await self._process_batch_cycle()
                # Wait for interval or shutdown
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"📬 [Outbox] Relay cycle failed: {e}")
                await asyncio.sleep(5)

    async def _process_batch_cycle(self):
        """
        [GEKTOR v21.15.5] Micro-Batching with Storm Protection.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # 1. Peek at the first pending item
            async with db.execute("SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY id ASC LIMIT 1") as cursor:
                first_row = await cursor.fetchone()
            
            if not first_row: return

            # 2. Windowing: Wait for the storm (500ms) to accumulate signals
            await asyncio.sleep(self.batch_window)
            
            # 3. Harvest the full batch
            async with db.execute(
                "SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY id ASC LIMIT 20"
            ) as cursor:
                rows = await cursor.fetchall()
            
            if not rows: return
            
            # 4. Compaction Logic
            signals = []
            others = []
            
            for r in rows:
                payload = json.loads(r['payload'])
                if r['event_type'] == 'NEW_SIGNAL':
                    # Extract probability for sorting (Quant requirement)
                    prob = payload.get('probability', 0.5) if isinstance(payload, dict) else 0.5
                    signals.append({'id': r['id'], 'symbol': payload.get('symbol', '???'), 'msg': payload.get('msg', ''), 'prob': prob})
                else:
                    others.append(r)

            # 5. Delivery
            if len(signals) >= self.storm_threshold:
                # STORM DETECTED: Collapse into Digest
                await self._deliver_storm_digest(db, signals)
            else:
                # REGULAR MODE: Deliver individually
                for s in signals:
                    await self._deliver_single(db, s['id'], 'NEW_SIGNAL', s['msg'])
            
            # 6. Process Non-Signal Alerts (System alerts, etc.)
            for r in others:
                await self._deliver_single(db, r['id'], r['event_type'], json.loads(r['payload']).get('msg', ''))

    async def _deliver_single(self, db, item_id, event_type, text):
        if not self.alert_handler: return
        
        success = await self.alert_handler.send_text(text)
        if success:
            await db.execute(
                "UPDATE outbox SET status = 'SENT', processed_at = ? WHERE id = ?;",
                (datetime.now(timezone.utc).isoformat(), item_id)
            )
            await db.commit()
            logger.success(f"📬 [Outbox] Delivered individual {event_type} (ID:{item_id})")

    async def _deliver_storm_digest(self, db, signals):
        """HFT Quant: Sort by Confidence and send Digest."""
        if not self.alert_handler: return
        
        # Sort by probability (Top signals first)
        signals.sort(key=lambda x: x['prob'], reverse=True)
        top_5 = signals[:5]
        
        digest = f"🚨 *SYSTEMIC MARKET STORM: {len(signals)} SIGNALS*\n"
        digest += f"Top Alpha Targets:\n\n"
        for s in top_5:
            digest += f"🎯 *#{s['symbol']}* | Prob: `{s['prob']:.1%}`\n"
        
        digest += f"\n⚠️ _Remaining {len(signals)-5} signals suppressed for API safety._"
        
        success = await self.alert_handler.send_text(digest)
        if success:
            # Mark ALL signals in the storm as SENT
            ids = [s['id'] for s in signals]
            await db.execute(
                f"UPDATE outbox SET status = 'SENT', processed_at = ? WHERE id IN ({','.join(['?']*len(ids))});",
                [datetime.now(timezone.utc).isoformat()] + ids
            )
            await db.commit()
            logger.critical(f"🌪️ [Outbox] Storm Digest Delivered ({len(signals)} suppressed).")
