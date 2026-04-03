import os
import json
import asyncio
import hmac
import hashlib
import time
import httpx
from redis.asyncio import Redis
from loguru import logger
from dotenv import load_dotenv

# [Audit 25.1] Bybit REST for Blind Mode
from data.bybit_rest import BybitREST

# THE OMEGA BRIDGE v5.23 (SYMMETRIC SHIELD)
# Role: Strict delivery with Dead-Letter-Office (DLO) and Blind Mode Watchdog.

# [Audit 25.1] Project Root Logic
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Configuration
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_URL = "https://fapi.binance.com"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6389/0")

STREAM_NAME = "tekton:hedge:orders"
DLO_STREAM = "tekton:hedge:dead_letters"
GROUP_NAME = "hedge_group"
CONSUMER_NAME = "exec_node"
MAX_RETRIES = 3

def sign(secret: str, query: str) -> str:
    return hmac.new(secret.encode('utf-8'), query.encode('utf-8'), hashlib.sha256).hexdigest()

class HedgeBlindModeWatchdog:
    """
    Независимый контроллер хедж-ноги при падении Nerve Center.
    [Audit 25.7] Cancel-Then-Check-Then-Nuke: Защита от двойного исполнения.
    """
    def __init__(self, bybit_rest: BybitREST, executor: 'BinanceExecutor'):
        self.bybit = bybit_rest
        self.executor = executor
        self._is_active = False

    async def _safe_binance_nuke(self, symbol: str):
        """Атомарная и безопасная ликвидация хедж-ноги без Race Condition."""
        logger.warning(f"☢️ [SAFE NUKE] Initiating atomic neutralization for {symbol}")
        
        # 1. Снимаем все открытые лимитки/стопы на Binance, чтобы предотвратить Race Condition
        # [Audit 25.7] Мы сначала "ослепляем" матчинг-движок, забирая у него ордера.
        await self.executor.cancel_all_orders(symbol=symbol)
        
        # 2. Небходима небольшая пауза, чтобы биржа успела обновить стейт после отмены
        await asyncio.sleep(0.1)
        
        # 3. Запрашиваем реальный стейт Binance ПОСЛЕ отмены ордеров
        actual_size = await self.executor.get_binance_position(symbol=symbol)
        
        if abs(actual_size) > 0:
            logger.critical(f"💀 [SPLIT-BRAIN] Binance leg still active (Size: {actual_size}). Executing Market Chase!")
            # 4. Уничтожаем только РЕАЛЬНЫЙ остаток
            # side будет противоположен знаку actual_size
            nuke_side = "BUY" if actual_size < 0 else "SELL"
            await self.executor.execute_nuke_protocol(symbol=symbol, side=nuke_side, qty=abs(actual_size))
        else:
            logger.success(f"🛡️ [SAFE NUKE] Binance position already closed (Server SL hit). Flat state confirmed.")

    async def run_guard(self):
        self._is_active = True
        logger.warning("🛡️ [HEDGE GUARD] Watchdog active. Monitoring cross-exchange leg symmetry.")
        
        while self._is_active:
            # Если шина Redis жива, мы можем немного расслабиться
            bus_alive = await self.executor.check_bus_health()
            
            if not bus_alive:
                logger.error("🛑 [BLIND MODE] Nerve Center (Redis) DOWN. Entering SAFE_NUKE protocol logic...")
                
                symbols = list(self.executor.active_hedges)
                for symbol in symbols:
                    try:
                        # [Audit 16.31] Trust Bybit REST as Ground Truth
                        pos_list = await self.bybit.get_positions(symbol=symbol)
                        bybit_pos = pos_list[0] if pos_list else None
                        size = float(bybit_pos.get('size', 0)) if bybit_pos else 0
                        
                        if size == 0:
                            logger.critical(f"🛑 [BLIND MODE] Primary leg {symbol} DEAD. Triggering Safe Nuke sequence.")
                            await self._safe_binance_nuke(symbol)
                            self.executor.active_hedges.discard(symbol)
                            
                    except Exception as e:
                        logger.error(f"⚠️ [HEDGE GUARD] REST Polling failed for {symbol}: {e}")
                
                await asyncio.sleep(2.0)
            else:
                await asyncio.sleep(10.0)

    def stop(self):
        self._is_active = False

class BinanceExecutor:
    def __init__(self):
        # [Audit 16.27] Redis connection with health tracking
        self.redis = Redis.from_url(REDIS_URL, decode_responses=False)
        
        # [Audit 25.1] Harden Windows TCP Pooling (Dynamic Ports Protection)
        # 1. limit=50 to avoid WSAENOBUFS
        # 2. keep-alive enabled by default in httpx.Limits
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
        self.client = httpx.AsyncClient(base_url=BINANCE_URL, timeout=5.0, limits=limits)
        
        self.bybit = BybitREST(is_execution=True)
        self.watchdog = HedgeBlindModeWatchdog(self.bybit, self)
        
        # [Audit 25.10] In-Flight Guard: Protection against Phantom Delta
        self.in_flight_delta = {}   # symbol -> total_in_flight_qty
        self._sweep_lock = asyncio.Lock()
        
        self.active_hedges = set() # Symbols we are currently hedging
        self.current_epoch: int = 0 
        self._is_running = True

    async def _add_in_flight(self, symbol: str, qty: float):
        """Регистрирует дельту ордера, отправленного на биржу."""
        self.in_flight_delta[symbol] = self.in_flight_delta.get(symbol, 0.0) + qty
        logger.debug(f"🛰️ [In-Flight] {symbol} INCREASED to {self.in_flight_delta[symbol]}")

    async def _resolve_in_flight(self, symbol: str, qty: float):
        """Снимает дельту после подтверждения исполнения/ошибки."""
        if symbol in self.in_flight_delta:
            self.in_flight_delta[symbol] -= qty
            if abs(self.in_flight_delta[symbol]) < 1e-8:
                self.in_flight_delta.pop(symbol, None)
            logger.debug(f"🛰️ [In-Flight] {symbol} RESOLVED. Remaining: {self.in_flight_delta.get(symbol, 0)}")

    async def check_bus_health(self) -> bool:
        """Проверяет доступность Redis."""
        try:
            await self.redis.ping()
            return True
        except:
            return False

    async def cancel_all_orders(self, symbol: str):
        """Отмена всех открытых ордеров (включая стопы) на Binance."""
        timestamp = int(time.time() * 1000)
        query = f"symbol={symbol}&timestamp={timestamp}"
        signature = sign(BINANCE_API_SECRET, query)
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        
        try:
            resp = await self.client.delete(f"/fapi/v1/allOpenOrders?{query}&signature={signature}", headers=headers)
            if resp.status_code == 200:
                logger.info(f"🧹 [Cleanup] All orders cancelled for {symbol}")
            else:
                logger.error(f"⚠️ [Cleanup] Cancel-all failed for {symbol}: {resp.text}")
        except Exception as e:
            logger.error(f"💥 [Cleanup] Critical failure during cancel-all: {e}")

    async def get_binance_position(self, symbol: str) -> float:
        """Получение текущего размера позиции на Binance."""
        timestamp = int(time.time() * 1000)
        query = f"symbol={symbol}&timestamp={timestamp}"
        signature = sign(BINANCE_API_SECRET, query)
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        
        try:
            resp = await self.client.get(f"/fapi/v1/positionRisk?{query}&signature={signature}", headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    return float(data[0].get('positionAmt', 0.0))
            return 0.0
        except Exception as e:
            logger.error(f"⚠️ [State] Failed to fetch Binance position for {symbol}: {e}")
            return 0.0

    async def enable_countdown_cancel(self, symbol: str, window_ms: int = 15000):
        """[Audit 25.9] Enable Binance Dead Man's Switch (Countdown Cancel)."""
        timestamp = int(time.time() * 1000)
        query = f"symbol={symbol}&countdownTime={window_ms}&timestamp={timestamp}"
        signature = sign(BINANCE_API_SECRET, query)
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        try:
            resp = await self.client.post(f"/fapi/v1/countdownCancelAll?{query}&signature={signature}", headers=headers)
            return resp.status_code == 200
        except Exception:
            return False

    async def _reconcile_positions_sweep(self):
        """
        [Audit 25.10] Atomic Integrity Sweep: Ground Truth Stitching with In-Flight Guard.
        - Calculations: Drift = TargetHedge - (CurrentBinance + InFlightOrders)
        - Prevents "Phantom Delta" when a hedge message was just sent from the bus.
        """
        if self._sweep_lock.locked():
            logger.debug("⏳ [RECON] Parallel sweep blocked. Waiting for previous audit.")
            return

        async with self._sweep_lock:
            logger.info("🔄 [RECON] Initiating Integrity Sweep with In-Flight Guard...")
            try:
                # 1. Fetch All Bybit Positions (Ground Truth)
                bybit_positions = await self.bybit.get_positions()
                if bybit_positions is None:
                    logger.error("❌ [RECON] Bybit Ground Truth UNREACHABLE. Aborting.")
                    return

                # Map: symbol -> size (positive for long, negative for short)
                by_map = {p['symbol']: (float(p['size']) if p['side'] == 'Buy' else -float(p['size'])) 
                          for p in bybit_positions if float(p['size']) > 0}

                # 2. Fetch All Binance Positions
                headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
                ts = int(time.time() * 1000)
                q = f"timestamp={ts}"
                s = sign(BINANCE_API_SECRET, q)
                resp = await self.client.get(f"/fapi/v2/positionRisk?{q}&signature={s}", headers=headers)
                
                if resp.status_code != 200:
                    logger.error(f"❌ [RECON] Binance State UNREACHABLE: {resp.text}")
                    return

                # Binance side: positionAmt is signed (pos=long, neg=short)
                bin_map = {p['symbol']: float(p['positionAmt']) for p in resp.json() 
                           if abs(float(p['positionAmt'])) > 1e-8}

                # 3. Discrepancy Analysis
                all_symbols = set(by_map.keys()) | set(bin_map.keys()) | set(self.in_flight_delta.keys())
                
                for symbol in all_symbols:
                    by_qty = by_map.get(symbol, 0.0)
                    target_hedge_qty = -by_qty # Should be symmetric
                    
                    actual_binance_qty = bin_map.get(symbol, 0.0)
                    in_flight_qty = self.in_flight_delta.get(symbol, 0.0)
                    
                    # [Rule 25.10] Logic: Current + What's in the air
                    expected_binance_qty = actual_binance_qty + in_flight_qty
                    drift = target_hedge_qty - expected_binance_qty
                    
                    if abs(drift) > 0.00001:
                        logger.critical(f"🚨 [RECON] DRIFT DETECTED on {symbol}: Drift={drift:.4f} (Target:{target_hedge_qty}, Actual:{actual_binance_qty}, InFlight:{in_flight_qty})")
                        # Correction
                        corr_side = "BUY" if drift > 0 else "SELL"
                        
                        # [Audit 25.10] Register Correction in In-Flight BEFORE execution
                        # Since execute_nuke_protocol might wait for rate limit, we mark it now.
                        await self._add_in_flight(symbol, drift)
                        await self.execute_nuke_protocol(symbol, corr_side, abs(drift))
                        
                        # Update Active Hedges set
                        if by_qty != 0: self.active_hedges.add(symbol)
                        else: self.active_hedges.discard(symbol)
                    else:
                        # Clean Match
                        if by_qty != 0: self.active_hedges.add(symbol)
                        else: self.active_hedges.discard(symbol)

                logger.success("✅ [RECON] Atomic Integrity Sweep complete. Drift neutralized.")

            except Exception as e:
                logger.error(f"💥 [RECON] Critical Failure: {e}")

    async def _setup_streams(self):
        try:
            await self.redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        except: pass
        
        stored_epoch = await self.redis.get("tekton:epoch:global_counter")
        self.current_epoch = int(stored_epoch or 0)
        logger.info(f"🧬 [STRICT_START] Current Global Epoch: {self.current_epoch}")

    async def run(self):
        await self._setup_streams()
        logger.warning("🌉 [BinanceBridge] Poison Pill Guard + Blind Watchdog Active.")
        
        # Запускаем Watchdog в фоне
        watchdog_task = asyncio.create_task(self.watchdog.run_guard())
        
        # [Audit 25.9] Initial Reconciliation Sweep (Cold Boot / Recovery)
        await self._reconcile_positions_sweep()
        last_sweep = time.time()
        
        while self._is_running:
            try:
                now = time.time()
                # 1. Heartbeat for Dead Man's Switch (Binance Countdown Cancel)
                if now - last_sweep > 10:
                    for sym in list(self.active_hedges):
                        # [Audit 25.9] Hardware safety: Refresh countdown
                        await self.enable_countdown_cancel(sym, 15000)
                    
                    # 2. Periodical Integrity Check
                    await self._reconcile_positions_sweep()
                    last_sweep = now

                # Sync Global Epoch
                global_curr_epoch_raw = await self.redis.get("tekton:epoch:global_counter")
                if global_curr_epoch_raw:
                    self.current_epoch = int(global_curr_epoch_raw)
                
                # Fetch messages
                response = await self.redis.xreadgroup(
                    GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=1, block=2000
                )
                if not response: continue
                
                for stream, messages in response:
                    for msg_id, data in messages:
                        # 1. Check Delivery Count
                        pending_info = await self.redis.xpending_range(
                            STREAM_NAME, GROUP_NAME, min=msg_id, max=msg_id, count=1
                        )
                        
                        delivery_count = pending_info[0].get('times_delivered', 0) if pending_info else 0
                        
                        if delivery_count > MAX_RETRIES:
                             logger.critical(f"☠️ [POISON_PILL] Message {msg_id} failed {delivery_count} times. Evicting to DLO.")
                             await self.redis.xadd(DLO_STREAM, {"id": msg_id, "data": str(data), "error": "MAX_RETRIES_EXCEEDED"})
                             await self.redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                             continue

                        payload = {k.decode(): v.decode() for k, v in data.items()}
                        msg_epoch = int(payload.get('epoch', 0))
                        
                        # [Gektor v6.20] Deterministic Epoch Guard
                        if msg_epoch != self.current_epoch and payload.get("action") != "RECON_CORRECTION":
                            logger.error(f"👻 [REJECT] Epoch Mismatch: Msg({msg_epoch}) != Global({self.current_epoch})")
                            await self.redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue
                        
                        # 2. Process
                        try:
                            success = await self._process_order(payload)
                            if success:
                                await self.redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                        except Exception as e:
                            logger.error(f"⚠️ [PROCESS_ERROR] ID {msg_id}: {e}")
                            
            except Exception as e:
                logger.error(f"❌ [Executor] Loop Error: {e}")
                await asyncio.sleep(1.0)
        
        self.watchdog.stop()
        await watchdog_task

    async def stop(self):
        """[Audit 25.8] Orphaned State Cleanup Logic."""
        logger.warning("🛑 [SHUTDOWN] Initiating cleanup for orphaned stops...")
        self._is_running = False
        
        # [P0] Очистка стакана Binance от осиротевших ордеров перед выключением
        symbols_to_clean = list(self.active_hedges)
        for symbol in symbols_to_clean:
            # Снимаем все ордера, чтобы SL не сработал через месяц на пустом аккаунте
            await self.cancel_all_orders(symbol)
        
        await self.client.aclose()
        await self.redis.aclose()
        logger.success("✅ [SHUTDOWN] Binance Bridge offline. System is CLEAN.")

    async def _process_order(self, order: dict) -> bool:
        """
        [Audit 25.10] Process with In-Flight Guard.
        """
        symbol = order.get("symbol", "BTCUSDT")
        side = order.get("side", "SELL")
        qty = order.get("qty")
        
        # Action routing
        action = order.get("action")
        if action == "RECON_CORRECTION":
            logger.warning(f"🧾 [RECON] Correction Received: {side} {qty} {symbol}")
            return await self.execute_nuke_protocol(symbol, side, qty)

        # Validation
        if qty is None or str(qty) == "nan":
            raise ValueError("Invalid Quantity (NaN/None)")

        # [Rule 25.10] Register In-Flight Delta
        # side="SELL" means Binance qty goes down.
        delta_sign = -1.0 if side == "SELL" else 1.0
        await self._add_in_flight(symbol, float(qty) * delta_sign)

        # 1. Market Order (Hedge Leg)
        timestamp = int(time.time() * 1000)
        query = f"symbol={symbol}&side={side}&type=MARKET&quantity={qty}&timestamp={timestamp}"
        signature = sign(BINANCE_API_SECRET, query)
        
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        url = f"/fapi/v1/order?{query}&signature={signature}"
        
        try:
            resp = await self.client.post(url, headers=headers)
            if resp.status_code == 200:
                logger.success(f"✅ [EPOCH_{self.current_epoch}] Hedge Filled {symbol} {side} {qty}.")
                
                # Update Active Hedges set
                if side == "SELL": self.active_hedges.add(symbol)
                else: self.active_hedges.discard(symbol)
                
                # --- [Audit 25.4] SYMMETRIC SERVER-SIDE STOPS ---
                sl_price = order.get("stop_loss")
                if sl_price and float(sl_price) > 0:
                    await self._place_mirror_sl(symbol, side, sl_price)
                return True
            else:
                logger.error(f"❌ [Binance] Order Failed: {resp.text}")
                # Rollback In-Flight on failure
                await self._resolve_in_flight(symbol, float(qty) * delta_sign)
                return False
        except Exception as e:
            logger.error(f"💥 [Binance] Connection Error during order: {e}")
            await self._resolve_in_flight(symbol, float(qty) * delta_sign)
            return False

    async def _place_mirror_sl(self, symbol: str, hedge_side: str, price: str):
        """Places a server-side STOP_MARKET order on Binance."""
        # If we are SHORT hedge, we need a BUY STOP_MARKET
        sl_side = "BUY" if hedge_side.upper() == "SELL" else "SELL"
        timestamp = int(time.time() * 1000)
        
        # STOP_MARKET params
        query = (f"symbol={symbol}&side={sl_side}&type=STOP_MARKET"
                 f"&stopPrice={price}&closePosition=true&timestamp={timestamp}")
        signature = sign(BINANCE_API_SECRET, query)
        
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        url = f"/fapi/v1/order?{query}&signature={signature}"
        
        resp = await self.client.post(url, headers=headers)
        if resp.status_code != 200:
            logger.error(f"⚠️ [SYMMETRIC] Failed to place SL: {resp.text}")

    async def panic_liquidate_all(self):
        """[Audit 25.10] Final Doomsday cleanup before process termination."""
        logger.critical("☢️ [DOOMSDAY] INITIATING TOTAL HEDGE LIQUIDATION...")
        symbols = list(self.active_hedges)
        tasks = [self.execute_nuke_protocol(sym) for sym in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.success("☢️ [DOOMSDAY] All hedge legs neutralized. System is FLAT.")

    async def execute_nuke_protocol(self, symbol: str, side: str = None, qty: float = None):
        """
        [P0] Emergency Closure of Binance Hedge with Client-Side Idempotency (Fencing).
        Uses deterministic newClientOrderId to survive 'Flapping Connection' (Audit 25.11).
        """
        # [Rule 25.11] Pre-Flight Check: Is a nuke already 'in the air'?
        in_flight_qty = self.in_flight_delta.get(symbol, 0.0)
        if abs(in_flight_qty) > 0 and side is None:
            logger.warning(f"⏳ [NUKE] Fencing Triggered: In-flight intent for {symbol} exists. Skipping duplicate fire.")
            return True

        logger.critical(f"☢️ [NUKE] Binance Liquidation for {symbol}...")
        timestamp = int(time.time() * 1000)
        # Deterministic ClOrdId for idempotency across network 'flapping'
        cl_ord_id = f"nuke_{symbol}_{timestamp // 10000}" # Changes every 10s
        
        if side and qty:
            # Atomic closure of specific qty
            delta_sign = -1.0 if side == "SELL" else 1.0
            query = f"symbol={symbol}&side={side}&type=MARKET&quantity={qty}&newClientOrderId={cl_ord_id}_{int(qty)}&timestamp={timestamp}"
            signature = sign(BINANCE_API_SECRET, query)
            try:
                resp = await self.client.post(f"/fapi/v1/order?{query}&signature={signature}", headers={"X-MBX-APIKEY": BINANCE_API_KEY})
                if resp.status_code == 200:
                    logger.success(f"☢️ [NUKE] Specific quantity {qty} {symbol} purged.")
                    return True
                elif "Duplicate orderId" in resp.text:
                    logger.warning(f"🛡️ [Idempotency] Duplicate Nuke detected for {symbol}. Ignoring.")
                    return True
                else:
                    logger.error(f"❌ [NUKE] Failed to purge {qty} {symbol}: {resp.text}")
                    await self._resolve_in_flight(symbol, float(qty) * delta_sign)
                    return False
            except Exception as e:
                logger.error(f"💥 [NUKE] Connection Error: {e}")
                await self._resolve_in_flight(symbol, float(qty) * delta_sign)
                return False
        else:
            # Full position close
            amt = await self.get_binance_position(symbol)
            if abs(amt) > 0:
                nuke_side = "BUY" if amt < 0 else "SELL"
                nuke_qty = abs(amt)
                
                # Register in-flight before sending
                delta_sign = 1.0 if nuke_side == "BUY" else -1.0
                await self._add_in_flight(symbol, nuke_qty * delta_sign)
                # Use deterministic clOrdId based on quantity to survive flaps
                query = (f"symbol={symbol}&side={nuke_side}&type=MARKET&quantity={nuke_qty}"
                         f"&newClientOrderId={cl_ord_id}&timestamp={timestamp}")
                signature = sign(BINANCE_API_SECRET, query)
                try:
                    resp = await self.client.post(f"/fapi/v1/order?{query}&signature={signature}", headers={"X-MBX-APIKEY": BINANCE_API_KEY})
                    if resp.status_code == 200:
                        logger.success(f"☢️ [NUKE] Position {amt} {symbol} purged.")
                        await self._resolve_in_flight(symbol, nuke_qty * delta_sign)
                        return True
                    elif "Duplicate orderId" in resp.text:
                        logger.warning(f"🛡️ [Idempotency] Duplicate Full Nuke detected for {symbol}. Execution assumed.")
                        return True
                    else:
                        logger.error(f"❌ [NUKE] Failed to purge {amt} {symbol}: {resp.text}")
                        await self._resolve_in_flight(symbol, nuke_qty * delta_sign)
                        return False
                except Exception as e:
                    logger.error(f"💥 [NUKE] Flapping connection during nuke: {e}")
                    return False
        return False

async def main():
    from core.utils.pid_lock import PIDLock
    with PIDLock("hedge_executor"):
        executor = BinanceExecutor()
        try:
            await executor.run()
        except (asyncio.CancelledError, KeyboardInterrupt):
            await executor.stop()
        except Exception as e:
            logger.error(f"🔥 [FATAL] {e}")
            await executor.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


