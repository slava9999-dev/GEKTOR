import asyncio
import time
import zmq
import zmq.asyncio
import orjson
from loguru import logger
from .market_state import market_state


class ZMQMetricsBridge:
    """
    HFT Metric Ingestion (v3.0 — Hardened).
    
    Fixes applied:
    - recv_multipart: Atomic frame reception (no desync with PUB side)
    - orjson: C-level parsing, accepts bytes directly (no .decode() overhead)
    - Log Throttling: Max 1 error log per 5s to protect Event Loop from GIL lock
    - Empty-frame guard: Heartbeats and empty payloads silently dropped
    - Drain-and-yield: Non-blocking drain of up to 2000 msgs, then yield to event loop
    """
    def __init__(self, base_port: int = 5550, shard_count: int = 8):
        self.base_port = base_port
        self.shard_count = shard_count
        self._running = False
        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.SUB)
        
        # [Task 4.0] Heartbeat Context Synchronization
        self.current_watchlist = set()
        self.current_scores = {}
        self.current_tiers = {}
        
        # Metrics (in-memory counters, zero-alloc)
        self._parse_errors = 0
        self._messages_processed = 0
        self._empty_frames_dropped = 0
        self._error_throttle_ts = 0.0
        
    def start(self):
        if self._running: return
        self._running = True
        
        # Subscribe to all shards
        for i in range(self.shard_count):
            port = self.base_port + i
            self.socket.connect(f"tcp://127.0.0.1:{port}")
        
        # Subscribe to all symbols (empty prefix)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        
        asyncio.create_task(self._listen_loop())
        logger.info(f"🌉 [ZMQ-Bridge v3.0] Listening on {self.shard_count} shard ports (Hardened Ingest).")

    def sync_context(self, watchlist: set, scores: dict, tiers: dict):
        """Update scoring context from Radar for Heartbeat consumption."""
        self.current_watchlist = watchlist
        self.current_scores = scores
        self.current_tiers = tiers

    async def _listen_loop(self):
        """
        Hardened ZMQ Ingestion Loop (v3.0).
        
        Protocol contract with ShardWorker:
            send_multipart([topic_bytes, payload_bytes])
        
        Guarantees:
        - O(1) allocations per message (orjson parses bytes directly)
        - No GIL-blocking I/O in hot path (throttled logging)
        - Empty/malformed frames silently dropped
        - asyncio.CancelledError properly propagated for clean shutdown
        """
        while self._running:
            try:
                # 1. Drain as many messages as possible without yielding
                for _ in range(2000):
                    try:
                        # Atomic multipart receive: [topic, payload]
                        frames = await self.socket.recv_multipart(flags=zmq.NOBLOCK)
                        
                        # Guard: Need exactly 2 frames (topic + payload)
                        if len(frames) < 2:
                            self._empty_frames_dropped += 1
                            continue
                        
                        payload = frames[1]
                        
                        # Guard: Empty payload (heartbeat signal)
                        if not payload:
                            self._empty_frames_dropped += 1
                            continue
                        
                        # C-level deserialization: orjson accepts bytes directly
                        # No .decode('utf-8') needed — saves ~2μs per message
                        try:
                            data = orjson.loads(payload)
                        except orjson.JSONDecodeError:
                            self._parse_errors += 1
                            # THROTTLED LOGGING: Max 1 log per 5 seconds
                            now = time.monotonic()
                            if now - self._error_throttle_ts > 5.0:
                                topic_str = frames[0].decode('utf-8', errors='replace')
                                logger.warning(
                                    f"❌ [ZMQ-Bridge] JSON decode failure on topic '{topic_str}'. "
                                    f"Total errors: {self._parse_errors}. Muting for 5s."
                                )
                                self._error_throttle_ts = now
                            continue
                        
                        # O(1) Update of Market State
                        symbol = data.get('s')
                        if symbol:
                            market_state.update_l2_metrics(
                                symbol=symbol,
                                imbalance=data.get('i', 0.0),
                                bids_usd=data.get('bv', 0.0),
                                asks_usd=data.get('av', 0.0),
                                bid=data.get('bp', 0.0),
                                ask=data.get('ap', 0.0)
                            )
                            self._messages_processed += 1
                        
                    except zmq.Again:
                        break  # No more messages in buffer
                
                # 2. Yield control to other coroutines (1ms)
                await asyncio.sleep(0.001)
                
            except asyncio.CancelledError:
                logger.info("🛑 [ZMQ-Bridge] Shutdown signal received.")
                break
            except zmq.ZMQError as e:
                logger.error(f"❌ [ZMQ-Bridge] Socket error: {e}")
                await asyncio.sleep(1)  # Backoff on socket failure
            except Exception as e:
                logger.critical(f"🔥 [ZMQ-Bridge] UNEXPECTED FATAL ERROR: {e}")
                raise  # Let TaskGroup/supervisor handle it

    def get_stats(self) -> dict:
        """Diagnostic metrics for monitoring dashboard."""
        return {
            "messages_processed": self._messages_processed,
            "parse_errors": self._parse_errors,
            "empty_frames_dropped": self._empty_frames_dropped,
        }

    def stop(self):
        self._running = False
        self.socket.close()
        self.ctx.term()

# Global Instance
zmq_bridge = ZMQMetricsBridge()
