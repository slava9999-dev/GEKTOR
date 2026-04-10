# src/application/sentinel.py
import asyncio
import sys
from loguru import logger

class BlackoutSentinel:
    """[GEKTOR v2.0] Heartbeat Monitor for Bridge Integrity."""
    def __init__(self, target_queue: asyncio.Queue, notifier_event: asyncio.Event, timeout_sec: int = 300):
        self.queue = target_queue
        self.notifier_event = notifier_event # Reference to _live_allowed
        self.timeout_sec = timeout_sec
        self.last_size = 0
        self.stagnation_time = 0
        self._running = False

    async def watch(self):
        self._running = True
        logger.info("🛡️ [Sentinel] Blackout Heartbeat Guard ARMED.")
        
        while self._running:
            await asyncio.sleep(10)
            current_size = self.queue.qsize()

            if current_size > 0 and current_size == self.last_size:
                self.stagnation_time += 10
                if self.stagnation_time >= self.timeout_sec:
                    self._trigger_local_alarm(current_size)
                    self.notifier_event.clear() # Emergency Freeze
            else:
                if self.stagnation_time >= self.timeout_sec:
                    logger.success("📡 [Sentinel] Bridge recovered. Resuming flow.")
                    # Thawing is handled by Recovery Phase logic, but we can set event here too
                    # if we are sure there's no retry loop. In PSR, we wait for drain.
                self.stagnation_time = 0
            
            self.last_size = current_size

    def _trigger_local_alarm(self, lost_messages: int):
        # System Bell
        sys.stdout.write('\a\a\a')
        sys.stdout.flush()
        
        logger.opt(raw=True).critical(
            f"\n{'='*60}\n"
            f"🚨 [!!! BRIDGE DEAD !!!] ТЕЛЕГРАМ-МОСТ УНИЧТОЖЕН.\n"
            f"Очередь заблокирована: {lost_messages} непереданных аномалий.\n"
            f"Включен протокол сброса в JSONL.\n"
            f"{'='*60}\n"
        )

    def stop(self):
        self._running = False
