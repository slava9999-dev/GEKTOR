# core/scenario/signal_ranker.py

import asyncio
import time
from typing import List, Optional
from loguru import logger
from dataclasses import dataclass, field
import uuid

from core.events.events import SignalEvent
from core.events.nerve_center import bus

@dataclass
class BufferedSignal:
    event: SignalEvent
    radar_score: float
    confidence: float
    timestamp: float = field(default_factory=time.time)

class CrossAssetRanker:
    """
    Tumbling Window Signal Ranker (v6.15).
    Aggregates signals in a short (400ms) window to pick market-wide 'winners'.
    """
    def __init__(self, window_ms: int = 400):
        self.window_sec = window_ms / 1000.0
        self.buffer: List[SignalEvent] = []
        self._lock = asyncio.Lock()
        self._is_running = False
        self._flush_task: Optional[asyncio.Task] = None

    async def start(self):
        if self._is_running: return
        self._is_running = True
        self._flush_task = asyncio.create_task(self._run_loop())
        logger.info(f"⏱️ [Ranker] Tumbling Window initialized: {self.window_sec}s")

    async def stop(self):
        self._is_running = False
        if self._flush_task:
            self._flush_task.cancel()

    async def add_signal(self, event: SignalEvent):
        """Producer: Non-blocking ingestion with deduplication."""
        async with self._lock:
            # 🛡️ Deduplication: Prevent re-processing replayed Outbox events
            if any(s.signal_id == event.signal_id for s in self.buffer):
                logger.debug(f"⏩ [Ranker] Duplicate signal {event.symbol} ignored.")
                return
                
            self.buffer.append(event)
            logger.debug(f"📥 [Ranker] Buffered {event.symbol} (Conf: {event.confidence:.2f})")

    async def _run_loop(self):
        """Consumer: Processes batches every N ms."""
        while self._is_running:
            await asyncio.sleep(self.window_sec)
            
            async with self._lock:
                if not self.buffer:
                    continue
                
                batch = self.buffer.copy()
                self.buffer.clear()

            await self._process_batch(batch)

    async def _process_batch(self, batch: List[SignalEvent]):
        """Rank and publish."""
        # Sorting: Primary Confidence * Radar Score (stored in factors/metadata)
        # We assume confidence already includes Radar Score weight or we pull it from metadata
        batch.sort(key=lambda s: s.confidence, reverse=True)
        
        winner = batch[0]
        suppressed = batch[1:]
        
        # 🥇 Publish Winner with 'CRITICAL' flag in metadata for Outbox
        winner.metadata["priority"] = 3 # CRITICAL
        winner.metadata["is_winner"] = True
        winner.metadata["suppressed_count"] = len(suppressed)
        
        logger.info(f"🏆 [Ranker] WINNER: {winner.symbol} | Suppressed: {len(suppressed)}")
        from core.events.safe_publish import safe_publish
        await safe_publish(winner, session=None) # Best-effort publish with outbox logging on fail
        
        # 🥈 Runner-ups (Optional: Send with LOW priority or just log)
        for runner in suppressed:
            runner.metadata["priority"] = 1 # LOW
            runner.metadata["is_winner"] = False
            # Slightly delay runner-ups to ensure winner hits UI first
            await asyncio.sleep(0.05)
            await safe_publish(runner, session=None)

# Global instance
signal_ranker = CrossAssetRanker()
