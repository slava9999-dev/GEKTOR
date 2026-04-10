# core/scenario/heartbeat.py
import asyncio
import time
from loguru import logger
from core.events.events import DetectorEvent

async def detector_heartbeat_loop(market_state, antimiss_system, orchestrator):
    """
    [NERVE REPAIR v4.0]
    Decision Heartbeat Loop.
    Isolates ZMQ ingestion from heavy pattern detection.
    """
    logger.info("💓 [Heartbeat] Detector Heartbeat Loop started (Interval: 250ms)")
    ready_symbols = set()
    
    while True:
        try:
            # Wake up strictly every 250ms (HFT Quant Requirement)
            await asyncio.sleep(0.25) 
            
            # 1. Get stable snapshot (O(1) in MarketState)
            current_snapshot = market_state.get_latest_snapshot()
            if not current_snapshot:
                continue
                
            # 2. Get current Radar context (Watchlist, Scores, Tiers) from ZMQ Bridge
            from core.realtime.zmq_bridge import zmq_bridge
            
            watchlist = zmq_bridge.current_watchlist
            if not watchlist:
                continue
                
            # 3. [NERVE REPAIR v4.1] Filter by Warm-up Status
            active_and_ready = []
            for symbol in watchlist:
                state = current_snapshot.get(f"bybit:{symbol}")
                if not state: 
                    continue
                
                if state.is_ready:
                    active_and_ready.append(symbol)
                    if symbol not in ready_symbols:
                        ready_symbols.add(symbol)
                        logger.info(f"🔥 [Heartbeat] {symbol} is now READY (Warm-up complete)")
                else:
                    # Log warm-up progress occasionally
                    if time.time() % 60 < 0.25:
                        age = time.time() - (state.first_tick_ts or time.time())
                        logger.debug(f"🧊 [Heartbeat] {symbol} warming up: {age:.1f}/{state.required_warmup_sec}s")

            if not active_and_ready:
                continue
                
            # 4. Trigger scan only for READY symbols
            now_ts = time.time()
            hits = await antimiss_system.scan_market(
                symbols=set(active_and_ready),
                ts=now_ts,
                radar_scores=zmq_bridge.current_scores,
                radar_tiers=zmq_bridge.current_tiers
            )
            
        except asyncio.CancelledError:
            logger.warning("💓 [Heartbeat] Loop cancelled")
            break
        except Exception as e:
            logger.exception(f"💀 [Heartbeat] Loop crashed: {e}")
            await asyncio.sleep(1)
