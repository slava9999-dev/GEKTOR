import asyncio
import os
import signal
import time
from multiprocessing import Process
from typing import List, Set, Dict, Any, Optional
from loguru import logger
import orjson
import zmq
import zmq.asyncio

from .ws_worker import WSWorker
from core.events.nerve_center import bus
from core.events.events import RawWSEvent, L2MetricsEvent
from core.realtime.l2_engine import L2LiquidityEngine

class ShardWorkerProcess(Process):
    """
    HFT Shard Worker (v2.6 ZMQ Edition).
    - Isolated OS Process.
    - ZeroMQ PUB socket for high-speed IPC.
    - Tier-Based Symbol Groups.
    """
    def __init__(self, shard_id: int, symbols: List[str], stream_url: str, zmq_port: int):
        super().__init__()
        self.shard_id = shard_id
        self.symbols = symbols
        self.stream_url = stream_url
        self.zmq_port = zmq_port
        self.daemon = True

    def run(self):
        """Entry point for the OS process."""
        logger.info(f"🧬 [Shard {self.shard_id}] Starting PID={os.getpid()} | symbols={len(self.symbols)} | port={self.zmq_port}")
        
        # 1. Start Event Loop
        asyncio.run(self._main_loop())

    async def _main_loop(self):
        ctx = zmq.asyncio.Context()
        socket = ctx.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 0) # [Audit 14.5] No LINGER: Immediate port release
        socket.bind(f"tcp://127.0.0.1:{self.zmq_port}")
        
        stop_event = asyncio.Event()
        
        # Engines
        engines: Dict[str, L2LiquidityEngine] = {
            s: L2LiquidityEngine(s) for s in self.symbols
        }

        # Subscriptions
        topics = set()
        for s in self.symbols:
            topics.add(f"orderbook.50.{s}")
            topics.add(f"publicTrade.{s}")

        # Local WS Worker (within this process)
        worker = WSWorker(self.shard_id, self.stream_url)
        worker.active_topics = topics
        worker.publish_to_bus = False # Bypassing global Redis
        worker.on_raw_event_cb = self.on_raw_event(engines, socket) # Local callback

        await worker.run()
        
        socket.close()
        ctx.term()

    def on_raw_event(self, engines: Dict[str, L2LiquidityEngine], socket: zmq.asyncio.Socket):
        async def _handler(event: RawWSEvent):
            topic = event.topic
            symbol = topic.split('.')[-1]
            if symbol not in engines: return
            
            engine = engines[symbol]
            data = event.data
            
            if topic.startswith("orderbook"):
                # Bybit L2 optimization: extract float directly from strings
                b_raw = data.get('b', [])
                a_raw = data.get('a', [])
                
                bids = [(float(b[0]), float(b[1])) for b in b_raw]
                asks = [(float(a[0]), float(a[1])) for a in a_raw]
                
                engine.apply_delta(bids, asks)
                imb = engine.calculate_micro_imbalance(depth=15)
                
                # ZeroMQ Publish: Atomic Multipart [topic, payload]
                # CRITICAL: send_multipart is ATOMIC — no frame desync possible
                payload = {
                    's': symbol,
                    'i': imb,
                    'bv': sum(b[0]*b[1] for b in bids[:10]),
                    'av': sum(a[0]*a[1] for a in asks[:10]),
                    'bp': bids[0][0] if bids else 0.0,
                    'ap': asks[0][0] if asks else 0.0,
                    'u': event.u
                }
                try:
                    await socket.send_multipart(
                        [symbol.encode(), orjson.dumps(payload)],
                        flags=zmq.NOBLOCK
                    )
                except zmq.Again:
                    pass  # Buffer full, skip update (Better than blocking)
                
            elif topic.startswith("publicTrade"):
                for t in data:
                    engine.apply_trade(float(t.get('p', 0)), float(t.get('v', 0)))

        return _handler
