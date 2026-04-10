import asyncio
import time
from loguru import logger
from redis.asyncio import Redis
from typing import Dict, Any, Optional
from core.events.events import SystemAmnesiaEvent, AlertEvent, SystemEpochChangeEvent

class StateReconciler:
    """
    [GEKTOR v14.8.1] Ground Truth Reconciliation Protocol.
    Solves Cross-Gateway Skew (Gateway A vs B) by atomic state deletion.
    """
    def __init__(self, redis_client: Redis, gateway: Any, risk_allocator: Any = None):
        self.redis = redis_client
        self.gateway = gateway
        self.risk_allocator = risk_allocator
        self.h_key = "gektor:portfolio:risk"
        self.status_key = "gektor:system:status"
        # [GEKTOR v14.7.2] State Folding: Map[cl_ord_id, data] ensures O(1) per order memory
        self.recovery_buffer: Dict[str, dict] = {}
        self._backoff = 1.0
        self._recovery_start = 0.0
        self._stale_monitor_task = None
        # [GEKTOR v14.8.0] Clock Drift Integrity
        self._last_heartbeat_ms = 0

    def buffer_event(self, cl_ord_id: str, data: dict):
        """Folding: Overwrite older partial fills with newest cumulative data."""
        self.recovery_buffer[cl_ord_id] = data

    async def initiate_recovery(self):
        """
        [GEKTOR v14.7.5] Fenced MtM Reconciliation Protocol.
        """
        import sys
        from core.events.nerve_center import bus
        
        # 1. Increment Epoch (Fencing Token)
        new_epoch = await self.redis.incr("gektor:system:epoch")
        await self.redis.set(self.status_key, "RECOVERY")
        
        self._recovery_start = time.time()
        self.recovery_buffer.clear()
        
        logger.warning(f"🚨 [RECOVERY] Epoch {new_epoch} Locked. Reconstructing reality...")
        
        # [GEKTOR v14.8.0] Signal AlphaCorrelator to kill Zombie calculations
        await bus.publish(SystemEpochChangeEvent(epoch=new_epoch), persistent=False)
        
        while True:
            try:
                # 2. REST Snapshot (The Ground Truth)
                raw_snap = await self.gateway.get_positions_snapshot()
                snapshot_ts = int(time.time() * 1000) # Use current time as version for REST snapshot
                
                # 3. [HFT Quant] Full Mark-to-Market Calculation (L2 Stable)
                # We rebuild the entire risk profile from scratch based on absolute REST state.
                # v14.8.0: Use mark_price (WMP equivalent) to eliminate LTP noise.
                total_beta = 0.0
                reconstructed_vals = {}
                
                for symbol, data in raw_snap.items():
                    # [GEKTOR v14.8.0] Solving the 'Orphan Order' Paradox
                    # If we find a position on Bybit that we don't recognize, 
                    # we assign it a Conservative Fallback Beta (2.0 per 100k) 
                    # until the AlphaCorrelator can provide the Ground-Truth Rolling Beta.
                    notional = abs(data['size'] * data.get('mark_price', 0.0))
                    
                    # 1.0 Notional = $100k value. High-conviction default = 1.5 Beta.
                    # We use a 1.5 multiplier to ensure we are OVER-estimating risk for orphans.
                    symbol_beta = (notional / 100000.0) * 1.5
                    
                    total_beta += symbol_beta
                    reconstructed_vals[f"beta:{symbol}"] = str(symbol_beta)
                    reconstructed_vals[f"ver:{symbol}"] = str(snapshot_ts) # Discovery Timestamp

                # [GEKTOR v14.8.0] Anti-Ghosting Sanity Check (The 'Exchange Lie' Guard)
                # We do not allow 'False Zero' updates if the drop in risk is massive 
                # without corresponding execution confirmations.
                current_total_beta_raw = await self.redis.hgetall(self.h_key)
                current_total_beta = float(current_total_beta_raw.get(b"total_beta", b"0"))
                
                # If risk drops by > 0.5 Beta but we didn't receive close events
                if total_beta < (current_total_beta - 0.5) and not self.recovery_buffer:
                    msg = f"☢️ [REST_HALT] Exchange reported 'False Zero' or massive risk drop! \n" \
                          f"Current: {current_total_beta:.2f} -> New: {total_beta:.2f}. \n" \
                          f"Refusing to sync to protect against API ghosts."
                    logger.critical(msg)
                    from core.events.nerve_center import bus
                    await bus.publish(AlertEvent(title="API INTEGRITY VIOLATION", message=msg), persistent=True)
                    # Stay in RECOVERY mode, do not commit the 'zero' state.
                    await asyncio.sleep(60)
                    continue

                # [GEKTOR v14.8.1] Atomic Hard-Reset (Skew Defense)
                lua_hard_reset_reconcile = """
                local h_key = KEYS[1]
                local epoch_key = KEYS[2]
                local target_epoch = tonumber(ARGV[1])

                -- 1. Fencing: Проверка эпохи
                local current_epoch = tonumber(redis.call('GET', epoch_key) or '0')
                if target_epoch < current_epoch then
                    return -1
                end

                -- 2. Atomic Wipe: Сжигаем старый High-Water Mark (Skew Defense)
                redis.call('DEL', h_key)

                -- 3. Write Snapshot: Записываем новые данные (MtM + Version Vectors)
                -- ARGV: [epoch, key1, val1, key2, val2, ...]
                for i=2, #ARGV, 2 do
                    redis.call('HSET', h_key, ARGV[i], ARGV[i+1])
                end

                -- 4. Finalize
                redis.call('SET', 'gektor:system:status', 'READY')
                return 1
                """
                
                keys = [self.h_key, "gektor:system:epoch"]
                args = [new_epoch]
                for k, v in reconstructed_vals.items():
                    args.extend([k, str(v)])

                success = await asyncio.wait_for(
                    self.redis.eval(lua_hard_reset_reconcile, 2, *keys, *args),
                    timeout=5.0
                )
                
                if success == -2:
                    logger.error(f"🛡️ [FENCED] Recovery iteration {new_epoch} superseded by new epoch. Aborting.")
                    return

                # 5. Update Shadow Mirror (Memory)
                if self.risk_allocator:
                    # Update shadow risk and versions from snapshot
                    for sym, data in raw_snap.items():
                         self.risk_allocator._shadow_risk[sym] = data['size']
                         self.risk_allocator._shadow_versions[sym] = snapshot_ts

                # 6. Drain Folded Buffer (Apply events that arrived during REST fetch)
                await self._drain_folded_buffer(new_epoch)
                
                # 7. [AMNESIA PROTOCOL]
                await bus.publish(SystemAmnesiaEvent(reason="FLUSH_ALL_BUFFERS"), persistent=True)
                
                logger.success(f"🛡️ [v14.7.5] ПОРТФЕЛЬ СИНХРОНИЗИРОВАН. Эпоха: {new_epoch} | Beta: {total_beta:.2f}")
                break

            except Exception as e:
                # Exponential Backoff for 429/Network failures
                self._backoff = min(self._backoff * 2, 60.0)
                logger.error(f"💀 [RECON] API Error: {e}. Backoff {self._backoff}s")
                await asyncio.sleep(self._backoff)

    async def _drain_folded_buffer(self, epoch: int):
        """Applies only the LATEST state for each order received during RECOVERY."""
        if not self.recovery_buffer or not self.risk_allocator:
            return
        
        count = len(self.recovery_buffer)
        logger.info(f"🌊 [RECON] Draining {count} folded execution updates for Epoch {epoch}...")
        
        # We apply the states sequentially to maintain Lua-monotonicity
        for cl_ord_id, data in self.recovery_buffer.items():
            try:
                await self.risk_allocator._apply_risk_handover(
                    cl_ord_id=cl_ord_id,
                    symbol=data['symbol'],
                    cum_qty=data['cum_qty'],
                    total_qty=data['total_qty'],
                    original_beta=data['original_beta'],
                    is_final=data['is_final'],
                    version=data.get('version', 0),
                    epoch=epoch, # [v14.7.5] Fencing Token
                    is_recovery=True
                )
            except Exception as e:
                logger.error(f"❌ [RECON] Failed to apply folded update for {cl_ord_id}: {e}")
        
        self.recovery_buffer.clear()
        logger.success(f"✅ [RECON] Drained {count} updates. Portfolio state is current.")

    async def is_ready(self) -> bool:
        """Checks if risk mutations are currently allowed."""
        status = await self.redis.get(self.status_key)
        return status == b"READY"

    async def start_stale_monitor(self):
        """[GEKTOR v14.8.0] Background Guard for 'Stuck' Orphan Risk."""
        if self._stale_monitor_task:
            return
        self._stale_monitor_task = asyncio.create_task(self._monitor_stale_risk_loop())
        logger.info("🕵️ [STALE_MONITOR] Watchdog for Stuck Risk activated.")

    async def _monitor_stale_risk_loop(self):
        from core.events.nerve_center import bus
        while True:
            try:
                await asyncio.sleep(3600) # Check every 60 minutes
                
                risk_hash = await self.redis.hgetall(self.h_key)
                now_ms = int(time.time() * 1000)
                
                stuck_symbols = []
                for key, val in risk_hash.items():
                    key_str = key.decode()
                    if key_str.startswith("ver:"):
                        symbol = key_str[4:]
                        ver_ts = int(val)
                        # If no update for > 60m (3,600,000 ms)
                        if now_ms - ver_ts > 3600000:
                            stuck_symbols.append(symbol)
                
                if stuck_symbols:
                    msg = f"⚠️ [STUCK_RISK] {len(stuck_symbols)} symbols have stale risk (Last update > 1hr).\n"
                    msg += f"Symbols: {', '.join(stuck_symbols)}\n"
                    msg += "Risk for these is LOCKED at 1.5x Conservative Fallback. Please check delistings or data-feed."
                    logger.warning(msg)
                    await bus.publish(AlertEvent(title="STALE RISK WARNING", message=msg), persistent=True)

            except Exception as e:
                logger.error(f"❌ [STALE_MONITOR] Error: {e}")
                await asyncio.sleep(60)

    async def verify_clock_integrity(self) -> bool:
        """
        [GEKTOR v14.8.0] Clock Jump Guard.
        If local time jumps backward, the LWW logic breaks.
        We detect this and trigger a full recovery.
        """
        now_ms = int(time.time() * 1000)
        
        if self._last_heartbeat_ms > 0 and now_ms < self._last_heartbeat_ms:
            logger.critical(f"⏰ [CLOCK_DRIFT] Detected backward clock jump! {self._last_heartbeat_ms} -> {now_ms}")
            # Trigger Amnesia because we can no longer trust our Version Vectors
            from core.events.nerve_center import bus
            await bus.publish(SystemAmnesiaEvent(reason="CLOCK_JUMP"), persistent=True)
            return False
            
        self._last_heartbeat_ms = now_ms
        return True
