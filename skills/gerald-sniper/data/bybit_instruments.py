import asyncio
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP
from typing import Dict, Optional, Tuple, List
from loguru import logger
from pydantic import BaseModel

class InstrumentSpec(BaseModel):
    symbol: str
    tick_size: Decimal
    lot_step: Decimal
    min_qty: Decimal

class InstrumentsProvider:
    """
    In-memory L1 Cache for Bybit V5 quantization rules.
    Provides 0ms latency in the Hot Path (Execution).
    Task 3.3.1 implementation following David Beazley's L1 RAM constraint.
    """
    def __init__(self, rest_client, whitelist: Optional[List[str]] = None):
        self._rest_client = rest_client # BybitREST client
        self._cache: Dict[str, InstrumentSpec] = {}
        self._lock = asyncio.Lock()
        self._sync_task: Optional[asyncio.Task] = None
        self._is_ready = False
        self.whitelist = [s.upper() for s in whitelist] if whitelist else None

    async def start_background_sync(self, interval_seconds: int = 3600):
        """Starts the background worker and blocks until the first successful sync."""
        # Initial sync before starting loop to guarantee data for the first orders
        await self._force_sync()
        self._sync_task = asyncio.create_task(self._sync_loop(interval_seconds))
        logger.info(f"🌐 [InstrumentsProvider] Background sync started (Interval: {interval_seconds}s)")

    async def _sync_loop(self, interval_seconds: int):
        while True:
            await asyncio.sleep(interval_seconds)
            await self._force_sync()

    async def _force_sync(self):
        """Fetches fresh data from Bybit V5 REST API."""
        try:
            # Query GET /v5/market/instruments-info
            result = await self._rest_client.get_instruments_info(category="linear")
            if not result or 'list' not in result:
                logger.error("❌ [InstrumentsProvider] Invalid response from Bybit REST info.")
                return

            new_cache = {}
            for item in result['list']:
                symbol = item['symbol']
                
                # [Audit 25.18] Memory Guard: Only cache what we trade
                if self.whitelist and symbol.upper() not in self.whitelist:
                    continue

                try:
                    spec = InstrumentSpec(
                        symbol=symbol,
                        tick_size=Decimal(item['priceFilter']['tickSize']),
                        lot_step=Decimal(item['lotSizeFilter']['qtyStep']),
                        min_qty=Decimal(item['lotSizeFilter']['minOrderQty'])
                    )
                    new_cache[spec.symbol] = spec
                except (KeyError, TypeError, ValueError) as e:
                    continue # Skip malformed items
            
            # Atomic swap (thread-safe in GIL, coroutine-safe via reference swap)
            async with self._lock:
                self._cache = new_cache
                self._is_ready = True
            
            logger.info(f"🌐 [InstrumentsProvider] L1 Cache updated: {len(self._cache)} tokens (Filtered).")
        except Exception as e:
            logger.error(f"❌ [InstrumentsProvider] Sync failure: {e}")

    def quantize_order(self, symbol: str, price: float, qty: float, is_long: bool, is_maker: bool = False) -> Tuple[float, float]:
        """
        Asymmetric quantization for O(1) latency. 
        Synchronous (no await) to avoid Event Loop overhead.
        """
        spec = self._cache.get(symbol)
        if not spec:
            # Fallback to loose quantization if cache is miss (should not happen if start_background_sync was called)
            logger.warning(f"⚠️ [InstrumentsProvider] Cache miss for {symbol}. Using loose defaults.")
            return round(price, 4), round(qty, 4)

        # 1. Decimal conversion for precision
        d_price = Decimal(str(price))
        d_qty = Decimal(str(qty))

        # 2. Volume Quantization (Always ROUND_DOWN to stay within risk limits)
        q_qty = (d_qty / spec.lot_step).quantize(Decimal('1'), rounding=ROUND_DOWN) * spec.lot_step
        q_qty = max(q_qty, spec.min_qty)

        # 3. Price Quantization (Asymmetric)
        # If Taker (IOC): Pay more/Receive less to guarantee matching
        # If Maker: Passive limit order
        if not is_maker:
            # Taker: Long buys at higher tick, Short sells at lower tick
            rounding_rule = ROUND_UP if is_long else ROUND_DOWN
        else:
            # Maker: Long buys at lower tick, Short sells at higher tick
            rounding_rule = ROUND_DOWN if is_long else ROUND_UP

        q_price = (d_price / spec.tick_size).quantize(Decimal('1'), rounding=rounding_rule) * spec.tick_size

        return float(q_price), float(q_qty)

# Export as singleton placeholder (initialized in main)
instruments_provider = None
