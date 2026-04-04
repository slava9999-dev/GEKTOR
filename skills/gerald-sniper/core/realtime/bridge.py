from core.realtime.conflator import conflator, L2Update, TradeUpdate
from .reconciler import reconciler
from utils.math_utils import safe_float

class RealtimeStateManager:
    """
    [GEKTOR v10.1] Advanced Bridge with StreamConflator.
    Events (Bus) -> Conflator (Buffer) -> MarketState (Memory).
    
    Ensures the Engine always processes only the most recent L2 state.
    """
    def __init__(self):
        self._running = False
        self._batch_task: Optional[asyncio.Task] = None

    def start(self):
        if self._running: return
        self._running = True
        bus.subscribe(RawWSEvent, self.handle_raw_event)
        bus.subscribe(L2MetricsEvent, self.handle_metrics_event)
        
        # Start the Background Conflation Flush Loop
        self._batch_task = asyncio.create_task(self._conflation_flush_loop())
        logger.info("🌉 [Bridge] RealtimeStateManager ACTIVE with StreamConflator (GEKTOR v10.1)")

    async def handle_metrics_event(self, event: L2MetricsEvent):
        """Pre-computed metrics go directly to state (already conflated by Shard)."""
        market_state.update_l2_metrics(
            symbol=event.symbol, imbalance=event.imbalance,
            bids_usd=event.bid_vol_usd, asks_usd=event.ask_vol_usd,
            bid=event.bid_price, ask=event.ask_price
        )

    async def handle_raw_event(self, event: RawWSEvent):
        """Fast Ingestion into Conflator."""
        try:
            topic = event.topic
            if not topic: return
            
            # Gap detection / Sequence monitoring
            await reconciler.reconcile(event)
            
            symbol = topic.split('.')[-1]
            now = time.time()

            if topic.startswith("publicTrade"):
                for t in event.data:
                    trade = TradeUpdate(
                        symbol=symbol,
                        price=safe_float(t.get('p', 0)),
                        qty=safe_float(t.get('v', 0)),
                        side=t.get('S', 'Buy'),
                        exchange_ts=int(t.get('T', 0))
                    )
                    await conflator.ingest_trade(trade)
            elif topic.startswith("orderbook"):
                l2 = L2Update(
                    symbol=symbol,
                    exchange_ts=event.exchange_ts or 0,
                    bids=event.data.get('b', []),
                    asks=event.data.get('a', []),
                    seq=event.u or 0
                )
                await conflator.ingest_l2(l2, local_recv_time=now)
        except Exception as e:
            logger.error(f"❌ [Bridge] Ingestion error: {e}")

    async def _conflation_flush_loop(self):
        """Consumes batches from conflator and applies to MarketState."""
        while self._running:
            try:
                l2_batch, trades_batch = await conflator.consume_batch()
                
                # 1. Apply L2 Snapshots (Replacement confluent)
                for symbol, l2 in l2_batch.items():
                    # Calculate depth USD volumes (Conflation benefit: only once per batch)
                    b_v = sum(safe_float(b[0]) * safe_float(b[1]) for b in l2.bids[:10])
                    a_v = sum(safe_float(a[0]) * safe_float(a[1]) for a in l2.asks[:10])
                    
                    market_state.update_orderbook(
                        symbol=symbol, bids_usd=b_v, asks_usd=a_v,
                        bids=l2.bids, asks=l2.asks,
                        update_id=l2.seq, ts_ms=l2.exchange_ts,
                        is_snapshot=False # Deltas conflated into snapshots
                    )

                # 2. Apply Trades (Aggregation confluent)
                for symbol, trades in trades_batch.items():
                    for t in trades:
                        market_state.update_trade(
                            symbol=symbol, price=t.price, qty=t.qty,
                            side=t.side, ts_ms=t.exchange_ts
                        )
                        
            except Exception as e:
                logger.error(f"❌ [Bridge] Flush error: {e}")
                await asyncio.sleep(0.01)

# Global instance
bridge = RealtimeStateManager()
