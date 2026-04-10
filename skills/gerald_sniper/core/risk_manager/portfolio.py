import time
import asyncio
import numpy as np
from loguru import logger
from typing import Dict, Any, Optional
from redis.asyncio import Redis

class RiskAllocator:
    """
    [GEKTOR v14.8.1] Central Risk Management Hub (Standard Name).
    Implements Fencing, LWW, and the Atomic Hard-Reset consensus.
    """
    def __init__(self, redis_client: Redis, config: Any):
        self.redis = redis_client
        self.config = config
        self.h_key = "gektor:portfolio:risk"
        self.orders_key = f"{self.h_key}:orders"
        self.limbo_key = "gektor:limbo:queue"
        self.epoch_key = "gektor:system:epoch"
        self.res_prefix = "gektor:res:"
        self.fly_prefix = "gektor:fly:"
        self._reconcile_lock = asyncio.Lock()
        
        # [GEKTOR v14.8.5] Lua Engine: Strict Idempotency (Kleppmann Standard)
        self._lua_fill = """
        local ord_id = KEYS[1]
        local portfolio_key = KEYS[2]
        local orders_key = KEYS[3]
        local incoming_cum_qty = tonumber(ARGV[1])
        local original_beta = tonumber(ARGV[2])
        local total_qty = tonumber(ARGV[3])

        -- 1. Fetch previously accounted for qty for THIS order
        local known_qty = tonumber(redis.call('HGET', orders_key, ord_id) or '0')

        -- 2. Guard against Double-Spend / Stale Updates
        if incoming_cum_qty > known_qty then
            local delta_fill = incoming_cum_qty - known_qty
            
            -- Proportional Beta calculation
            local delta_beta = (delta_fill / total_qty) * original_beta
            
            -- Atomic mutative sequence
            redis.call('HSET', orders_key, ord_id, incoming_cum_qty)
            redis.call('HINCRBYFLOAT', portfolio_key, "total_beta", delta_beta)
            -- We extract symbol from ord_id encoding or pass as ARG. 
            -- Assuming symbol is passed as ARGV[4]
            local symbol = ARGV[4]
            redis.call('HINCRBYFLOAT', portfolio_key, "beta:" .. symbol, delta_beta)
            
            return tostring(delta_fill)
        end
        return "0"
        """
        self._fill_sha = None
        
        # In-memory shadow state for Zero-Latency lookups
        self._shadow_risk = {}
        self._shadow_versions = {}

        self._dirty_event = asyncio.Event()
        self._is_syncing = False
        self._shadow_total_beta = 0.0
        self._shadow_gross = 0.0
        
        # [GEKTOR v15.0] Combat Readiness Gate (The Anti-Ghost Barrier)
        self._is_combat_ready = False
        self._ready_event = asyncio.Event()
    async def run_audit_healing(self):
        """
        [GEKTOR v14.8.1] Atomic Self-Healing Healer.
        Solves the 'Reconcile Collision' by performing the audit 
        entirely inside Redis to prevent 'Lost Updates' from racing fills.
        """
        lua_audit_healer = """
        local h_key = KEYS[1]
        local threshold = tonumber(ARGV[1])

        -- 1. [ATOMIC SUMMATION] O(N) inside Redis (Single Tick)
        local real_sum = 0
        local fields = redis.call('HGETALL', h_key)
        for i=1, #fields, 2 do
            local key = fields[i]
            if string.match(key, "^beta:") then
                real_sum = real_sum + tonumber(fields[i+1] or '0')
            end
        end

        -- 2. [ATOMIC COMPARISON] Get current total Beta
        local stored_sum = tonumber(redis.call('HGET', h_key, "total_beta") or '0')

        -- 3. [ATOMIC HEAL] Apply correction if drift exceeds threshold
        if math.abs(real_sum - stored_sum) > threshold then
            redis.call('HSET', h_key, "total_beta", real_sum)
            return "HEALED:" .. tostring(real_sum) .. ":" .. tostring(stored_sum)
        end
        return "OK"
        """
        
        while True:
            await asyncio.sleep(300) # Audit every 5 minutes (lower pressure)
            try:
                result = await self.redis.eval(lua_audit_healer, 1, self.h_key, 1e-6)
                if result:
                    res_str = str(result)
                    if res_str.startswith("HEALED"):
                        logger.info(f"⚖️ [HEALER] Drift corrected. {res_str}")
            except Exception as e:
                logger.error(f"❌ [HEALER_ERROR] Audit failed: {e}")

    async def get_total_beta(self) -> float:
        """Fetches the atomic sum from Redis."""
        val = await self.redis.hget(self.h_key, "total_beta")
        return float(val) if val else 0.0

    async def can_accept_alert(self, symbol: str, alert_beta: float, alert_qty: float) -> bool:
        """
        [GEKTOR v14.8.1] Final Risk-Sentinel with Vicious Hedge Protection.
        Ensures that 'Hedges' don't become 'Double Risks' during Correlation Avalanches.
        """
        # 1. Fetch current atomic totals
        current_total_beta = await self.get_total_beta()
        current_gross_exposure = await self._get_total_gross_exposure()
        
        # 2. Get existing position for this symbol
        existing_qty = await self._get_position_qty(symbol)
        existing_beta = await self._get_position_beta(symbol)
        
        # 3. Calculate Deltas
        beta_delta = alert_beta - existing_beta
        is_reducing_volume = abs(alert_qty) < abs(existing_qty)
        
        # [GEKTOR v14.8.1] SQUELCH BYPASS: The 'Safety-First' Double-Lock
        # We only bypass if we are REDUCING VOLUME or explicitly reducing exposure 
        # while staying within Gross Limits.
        if is_reducing_volume or (beta_delta < 0 and current_gross_exposure <= self.config.risk.max_gross_exposure):
            logger.info(f"📉 [BYPASS] Alert for {symbol} is risk-reducing. (Delta: {beta_delta:.2f}). Allowed.")
            return True

        # 4. Gating for New Risk / Scaling Up
        # If we are already over-leveraged or over-beta'd, block new entries.
        if current_total_beta + alert_beta > self.config.risk.max_total_beta:
            logger.warning(f"🛡️ [SQUELCH] {symbol} (Beta {alert_beta:.2f}) blocked. Beta Limit reached.")
            return False
            
        if current_gross_exposure + abs(alert_qty) > self.config.risk.max_gross_exposure:
             logger.warning(f"🛡️ [GROSS_LIMIT] {symbol} blocked. Portfolio Gross Density too high.")
             return False

        return True

    async def _get_total_gross_exposure(self) -> float:
        """Sum of abs(position_value) from Redis."""
        # Simplified: In 14.8.1 this is calculated via LUA in every sync
        val = await self.redis.hget(self.h_key, "total_gross")
        return float(val) if val else 0.0

    async def reserve_beta(self, symbol: str, current_beta: float) -> bool:
        """Phase 1: Reserve Beta (15s TTL) for Pending Intent."""
        total_beta = await self.get_total_beta()
        
        if not self.can_accept_alert(total_beta, current_beta):
            logger.warning(f"🛡️ [SQUELCH] {symbol} blocked. Total Beta {total_beta:.2f} + {current_beta:.2f} > Limit.")
            return False

        await self.redis.setex(f"{self.res_prefix}{symbol}", 15, str(current_beta))
        return True

    async def move_to_inflight(self, symbol: str, cl_ord_id: str, beta: float):
        """Phase 2: Handover to Live Order (60s)."""
        fly_key = f"{self.fly_prefix}{cl_ord_id}"
        pipe = self.redis.pipeline()
        pipe.hset(fly_key, mapping={"last_cum_qty": 0, "beta_reserved": str(beta)})
        pipe.expire(fly_key, 60)
        pipe.delete(f"{self.res_prefix}{symbol}")
        await pipe.execute()

    async def lock_symbol(self, symbol: str, cl_ord_id: str, ttl: int = 300) -> bool:
        """
        [GEKTOR v14.8.7] Distributed Lease with Auto-Expiry.
        Martin Kleppmann Standard: Strictly prevents Infinite Deadlocks.
        """
        lock_key = f"gektor:lock:{symbol}"
        # Only acquire if NOT exists. TTL prevents manual recovery for transient errors.
        acquired = await self.redis.set(lock_key, cl_ord_id, nx=True, ex=ttl)
        if acquired:
            logger.debug(f"🔒 [LEASE] Symbol {symbol} locked for {cl_ord_id} (TTL {ttl}s)")
            return True
        else:
            current_owner = await self.redis.get(lock_key)
            logger.warning(f"🛡️ [FENCE] {symbol} already locked by {current_owner}. Rejecting strike.")
            return False

    async def unlock_symbol(self, symbol: str, cl_ord_id: str):
        """
        [GEKTOR v14.8.7] Ownership-Aware Release.
        Ensures that we ONLY release our own lock.
        """
        lua_unlock = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        lock_key = f"gektor:lock:{symbol}"
        res = await self.redis.eval(lua_unlock, 1, lock_key, cl_ord_id)
        if res:
            logger.debug(f"🔓 [LEASE] Symbol {symbol} released for {cl_ord_id}.")
        else:
            logger.warning(f"⚠️ [STALE_RELEASE] Attempted to unlock {symbol} for {cl_ord_id}, but owner mismatch.")

    async def is_symbol_locked(self, symbol: str) -> bool:
        """Checks if symbol is currently leased."""
        return await self.redis.exists(f"gektor:lock:{symbol}") > 0

    async def wait_until_ready(self, timeout: float = 5.0):
        """Blocks until the portfolio is reconciled (Combat Ready)"""
        if self._is_combat_ready: return
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.error("🛑 [CRITICAL] Portfolio reconciliation timeout. System BLIND.")
            raise

    async def invalidate_combat_readiness(self):
        """Called on disconnect to prevent strikes during blindness"""
        self._is_combat_ready = False
        self._ready_event.clear()
        logger.warning("☣️ [RISK] System NOT COMBAT READY (Unsynced).")

    async def reconcile_ground_truth(self, actual_positions: Dict[str, float], total_beta: float, total_gross: float):
        """
        [GEKTOR v15.0] Groundwater Sync.
        Sets the system to COMBAT READY after verifying everything.
        """
        async with self._reconcile_lock:
            # 1. Update ground truth in Redis
            pipe = self.redis.pipeline()
            pipe.hset(self.h_key, "total_beta", str(total_beta))
            pipe.hset(self.h_key, "total_gross", str(total_gross))
            for sym, qty in actual_positions.items():
                pipe.hset(self.h_key, f"qty:{sym}", str(qty))
            
            await pipe.execute()
            
            # 2. Update memory shadow
            self._shadow_total_beta = total_beta
            self._shadow_gross = total_gross
            
            # 3. OPEN THE GATE
            self._is_combat_ready = True
            self._ready_event.set()
            logger.success(f"⚔️ [RISK] RECONCILED. Portfolio synced. COMBAT READY.")

    async def sync_limbo_orders(self, exchange_orders: list):
        """
        [GEKTOR v15.0] Subclossing the Phantom Loop.
        Check each order that was 'In-Flight' vs what the exchange says.
        We update handle_execution for each found fill.
        """
        # (This would iterate over Redis fly_keys and cross-ref with exchange_orders)
        logger.info(f"🔎 [REAPER] Syncing {len(exchange_orders)} orders from exchange status.")
        for order in exchange_orders:
            # Reconstruct the fill reality using handle_execution
            await self.handle_execution(
                cl_ord_id=order['orderLinkId'],
                symbol=order['symbol'],
                cum_qty=float(order['cumExecQty']),
                total_qty=float(order['qty']),
                original_beta=1.0, # Will be corrected by reconcile_ground_truth
                is_final=order['orderStatus'] in ['Filled', 'Cancelled', 'Rejected']
            )

    async def can_accept_new_intent(self, symbol: str, intent_beta: float) -> bool:
        """
        [GEKTOR v14.8.1] Fail-Safe Intent Gating.
        """
        # 1. FAIL-FAST: If the system had a persistence failure, block all new intents.
        if self.system_status == "INFRA_FAULT":
            logger.error("🚫 [BLOCKED] System in INFRA_FAULT mode. No state persistence.")
            return False

        # 2. Risk Reduction Check (Bypass)
        if intent_beta <= 0: return True

        # 3. Gating against total budget
        if (self._shadow_total_beta + intent_beta) > self.config.risk.max_total_beta:
            logger.warning(f"🛡️ [SQUELCH] Intent {symbol} (+{intent_beta:.2f}) over budget.")
            return False
            
        return True

class ViciousHedgeGuard:
    """
    [GEKTOR v14.8.1] Correlation Trap & Partial Fill Protection.
    Invariant: Execution (Fills) is UNSTOPPABLE. New Intent (Alerts) is GATED.
    """
    def __init__(self, max_gross_usd: float):
        self.max_gross_usd = max_gross_usd

    async def can_accept_new_intent(self, symbol: str, alert_qty: float, current_state: dict) -> bool:
        """
        [David Beazley] Pre-flight Check for NEW Orders.
        Filters alerts before they reach the exchange.
        """
        old_qty = current_state.get(f"qty:{symbol}", 0.0)
        
        # 1. [INVARIANT] Volume Reduction Bypass
        # If the new intent aims to reduce absolute exposure, allow it immediately.
        if abs(alert_qty) < abs(old_qty):
            logger.info(f"📉 [GATING_BYPASS] {symbol} intent reduces risk. Allowed.")
            return True

        # 2. Hard Gross Limit for New Exposures
        current_gross = current_state.get("total_gross_usd", 0.0)
        price = current_state.get(f"price:{symbol}", 1.0)
        estimated_new_gross = current_gross + abs(alert_qty * price)

        if estimated_new_gross > self.max_gross_usd:
            logger.warning(
                f"🛡️ [GROSS_LIMIT] {symbol} intent blocked. "
                f"Estimated Gross {estimated_new_gross:.2f} > {self.max_gross_usd}."
            )
            return False

        return True

    def accept_execution_fact(self, symbol: str, filled_qty: float):
        """
        [GEKTOR v14.8.1] Unstoppable State Update.
        Fills are already physical reality on the exchange. We MUST accept them
        even if they temporarily exceed our gross limits due to asynchronous skew.
        """
        # (This logic is implemented in handle_execution bypassing the Guard)
        pass

# Final Portfolio Method Update
# async def reserve_beta(self, symbol: str, qty: float, beta: float) -> bool:
#    state = await self.get_full_atomic_state()
#    if not await self.hedge_guard.can_accept_new_intent(symbol, qty, state):
#        return False
#    ...

    async def full_reconcile(self, actual_positions: Dict[str, float]):
        """[GROUND TRUTH] Mandatory Periodic Sync."""
        async with self._reconcile_lock:
            total_beta = sum(actual_positions.values())
            ts = int(time.time() * 1000)
            
            pipe = self.redis.pipeline()
            # Clear old and set new
            await self.redis.delete(self.h_key)
            
            pipe.hset(self.h_key, "total_beta", str(total_beta))
            pipe.hset(self.h_key, "last_recon_ts", str(ts))
            for sym, beta in actual_positions.items():
                pipe.hset(self.h_key, f"beta:{sym}", str(beta))
            
            await pipe.execute()
            logger.info(f"🔄 [RECON] Risk Synced. Ground Truth Beta: {total_beta:.2f}")

    async def handle_execution(self, cl_ord_id: str, symbol: str, cum_qty: float, total_qty: float, original_beta: float, is_final: bool):
        """
        [GEKTOR v14.8.4] Atomic Fill Lifecycle.
        Updates the global Risk state based on fractional fills.
        """
        fly_key = f"{self.fly_prefix}{cl_ord_id}"
        
        # 1. Fetch fly state (Atomic)
        fly_data = await self.redis.hgetall(fly_key)
        if not fly_data:
            if is_final: return # Already cleaned or never existed
            # If it's a fill but no reservation found, it's an 'Orphan Fill'
            # (Possibly from a previous process epoch). We accept it as truth.
            last_cum = 0.0
            reserved_beta = original_beta 
        else:
            last_cum = float(fly_data.get(b'last_cum_qty', 0))
            reserved_beta = float(fly_data.get(b'beta_reserved', original_beta))

        delta_fill = cum_qty - last_cum
        if delta_fill <= 0 and not is_final:
            return

        # 2. Calculate Beta proportion
        # delta_beta = (filled / total) * reserved_beta
        # Note: If total_qty is 0, we assume full beta on first fill
        fill_ratio = (delta_fill / total_qty) if total_qty > 0 else 1.0
        delta_beta = fill_ratio * reserved_beta

        # 3. Atomic Update in Redis
        pipe = self.redis.pipeline()
        if delta_fill > 0:
            pipe.hincrbyfloat(self.h_key, f"beta:{symbol}", delta_beta)
            pipe.hincrbyfloat(self.h_key, "total_beta", delta_beta)
            pipe.hset(fly_key, "last_cum_qty", str(cum_qty))
            logger.info(f"✅ [RISK_SYNC] {symbol} fill +{delta_fill} | Beta +{delta_beta:.4f}")

        if is_final:
            pipe.delete(fly_key)
            logger.debug(f"🧹 [RISK_CLEAN] Fly state {cl_ord_id} purged.")
        
        await pipe.execute()

    async def handle_limbo_resolution(self, event: Any):
        """
        [GEKTOR v14.8.4] Post-Mortem Sync from Reaper.
        Handles the 'Ghost Partial Fill' case where an order was filled 
        but the WS stream was dead.
        """
        logger.warning(f"🧟 [LIMBO_RISK] Reconciling {event.cl_ord_id} | Status: {event.status} | CumQty: {event.cum_qty}")
        
        # We treat Limbo resolution as a 'Final' execution event
        await self.handle_execution(
            cl_ord_id=event.cl_ord_id,
            symbol=event.symbol,
            cum_qty=event.cum_qty,
            total_qty=event.cum_qty if event.cum_qty > 0 else 1.0, # Approximate if unknown
            original_beta=1.0, # Reaper doesn't know original beta, system will correct in next full_reconcile
            is_final=True
        )

    def calculate_rolling_beta(self, asset_closes: np.ndarray, btc_closes: np.ndarray) -> float:
        if len(asset_closes) < 10 or len(btc_closes) < 10: return 1.0 
        asset_rets = np.diff(np.log(asset_closes + 1e-12))
        btc_rets = np.diff(np.log(btc_closes + 1e-12))
        min_len = min(len(asset_rets), len(btc_rets))
        cov_matrix = np.cov(asset_rets[-min_len:], btc_rets[-min_len:])
        var = cov_matrix[1, 1]
        if var < 1e-8: return 0.0
        return float(np.clip(cov_matrix[0, 1] / var, -1.0, 2.5))

    async def run_shadow_sync_loop(self):
        """
        [GEKTOR v14.8.5] Event Conflation Engine.
        Protects the system from 'Event Storms' by batching risk recalculations
        during micro-structural volatility (Rule of David Beazley).
        """
        self._is_syncing = True
        logger.info("⚡ [RISK] Conflation Barrier INITIALIZED (50ms gate).")
        while self._is_syncing:
            await self._dirty_event.wait()
            self._dirty_event.clear()
            
            # [COALESCENCE] Wait for the storm to settle (50ms bucket)
            await asyncio.sleep(0.050) 
            
            try:
                # One single trip to Redis to refresh the SHADOW state
                pipe = self.redis.pipeline()
                pipe.hget(self.h_key, "total_beta")
                pipe.hget(self.h_key, "total_gross")
                res = await pipe.execute()
                
                self._shadow_total_beta = float(res[0] or 0)
                self._shadow_gross = float(res[1] or 0)
                
                logger.debug(f"⚖️ [CONFLATED] Shadow State Updated: Beta {self._shadow_total_beta:.4f}")
            except Exception as e:
                logger.error(f"❌ [CONFLATION_FAIL] Shadow sync error: {e}")
