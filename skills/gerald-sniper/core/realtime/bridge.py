import asyncio
from loguru import logger
from core.events.nerve_center import bus
from core.events.events import RawWSEvent, L2MetricsEvent
from .market_state import market_state
from .reconciler import reconciler
from utils.math_utils import safe_float

class RealtimeStateManager:
    """
    Bridge: Events (RawWS, L2Metrics) -> LiveMarketState (Memory).
    """
    def __init__(self):
        self._running = False

    def start(self):
        if self._running: return
        self._running = True
        bus.subscribe(RawWSEvent, self.handle_event)
        bus.subscribe(L2MetricsEvent, self.handle_metrics_event)
        logger.info("🌉 [Bridge] RealtimeStateManager ACTIVE (Batch-Ready + Gap Guard)")

    async def handle_metrics_event(self, event: L2MetricsEvent):
        """Handle pre-computed metrics from ShardWorker."""
        market_state.update_l2_metrics(
            symbol=event.symbol,
            imbalance=event.imbalance,
            bids_usd=event.bid_vol_usd,
            asks_usd=event.ask_vol_usd,
            bid=event.bid_price,
            ask=event.ask_price
        )

    async def handle_event(self, event: RawWSEvent):
        """
        [Audit 27.5] Ingestion with Gap Detection (Rule 6.5).
        """
        try:
            topic = event.topic
            data = event.data
            if not data or topic == "": return
            
            # [Audit v7.0] State Integrity Check (Delegated to Reconciler)
            await reconciler.reconcile(event)
            
            symbol = topic.split('.')[-1]
            u = event.u or 0
            is_snap = event.is_snapshot

            if topic.startswith("publicTrade"):
                for t in data:
                    market_state.update_trade(
                        symbol=symbol,
                        price=safe_float(t.get('p', 0)),
                        qty=safe_float(t.get('v', 0)),
                        side=t.get('S', 'Buy'),
                        ts_ms=int(t.get('T', 0))
                    )
            elif topic.startswith("orderbook"):
                bids = data.get('b', [])
                asks = data.get('a', [])
                # Optimized USD volume calculation (Top 10 instead of 20 for speed)
                b_v = sum(safe_float(b[0]) * safe_float(b[1]) for b in bids[:10])
                a_v = sum(safe_float(a[0]) * safe_float(a[1]) for a in asks[:10])
                market_state.update_orderbook(
                    symbol=symbol, 
                    bids_usd=b_v, 
                    asks_usd=a_v,
                    bids=bids,
                    asks=asks,
                    update_id=u,
                    is_snapshot=is_snap
                )
            elif topic.startswith("allLiquidation"):
                for liq in data:
                    market_state.update_liquidation(
                        symbol=liq.get('s'),
                        price=safe_float(liq.get('p', 0)),
                        qty=safe_float(liq.get('v', 0)),
                        side=liq.get('S')
                    )
        except Exception as e:
            logger.error(f"❌ [Bridge] Event error: {e}")

# Global instance
bridge = RealtimeStateManager()
