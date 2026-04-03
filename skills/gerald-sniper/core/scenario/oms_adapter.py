import time
import json
import hmac
import abc
import hashlib
import math
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from loguru import logger
from core.shield.execution_guard import OrderIntent

# Hardcoded rules for common Bybit Linear symbols (Task 3.3 temporary)
# Rule 3.3.1: Always pull from /v5/market/instruments-info in prod, use these for alpha test.
BYBIT_RULES = {
    "BTCUSDT": {"tick": "0.10", "lot": "0.001"},
    "ETHUSDT": {"tick": "0.01", "lot": "0.01"},
    "SOLUSDT": {"tick": "0.01", "lot": "0.1"},
    "ADAUSDT": {"tick": "0.0001", "lot": "1"},
    "DEFAULT": {"tick": "0.01", "lot": "0.01"},
}

def quantize_value(value: float, step: float, rounding: str = "DOWN", side: Optional[str] = None) -> float:
    """
    Квантует значение под шаг биржи (Tick/Lot Size).
    
    Для Qty (объема) всегда ROUND_DOWN (пессимизация объема во избежание нехватки маржи).
    Для Price (цены в Limit ордере):
      - Покупка (Buy/Long): ROUND_DOWN (пессимистичная цена, чтобы не переплатить лишний тик).
      - Продажа (Sell/Short): ROUND_UP (пессимистичная цена, чтобы не продать дешевле).
    """
    if not step or step <= 0:
        return value
    
    val_dec = Decimal(str(value))
    step_dec = Decimal(str(step))
    
    # Асимметричное квантование для цен (Аудит 10: Slippage Control)
    if side:
        mode = ROUND_DOWN if side.lower() in ["buy", "long"] else ROUND_UP
    else:
        # Для объемов всегда округляем ВНИЗ (Rule 3.3.2)
        mode = ROUND_DOWN if rounding == "DOWN" else ROUND_HALF_UP
    
    steps = (val_dec / step_dec).quantize(Decimal('1'), rounding=mode)
    res = steps * step_dec
    return float(res)

class IExchangeAdapter(abc.ABC):
    """Строгий контракт для интеграции любой криптобиржи в TEKTON_ALPHA (Audit 15)."""
    
    @abc.abstractmethod
    async def execute_order(self, symbol: str, side: str, qty: float, price: float, 
                            order_link_id: str, **kwargs) -> Optional[Dict]:
        """Стандартизированный метод исполнения ордера."""
        pass

    @abc.abstractmethod
    async def amend_order(self, symbol: str, order_id: str, **kwargs) -> bool:
        """Модификация параметров ордера (Stop-Loss, Price, etc)."""
        pass

    @abc.abstractmethod
    async def cancel_all_orders(self, symbol: str) -> bool:
        """Отмена всех активных ордеров по символу."""
        pass

    @abc.abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Отмена ордера."""
        pass

    @abc.abstractmethod
    async def get_position(self, symbol: str) -> Optional[Dict]:
        """Получение текущей позиции (размер, входная цена)."""
        pass

    @abc.abstractmethod
    async def get_all_positions(self) -> List[Dict]:
        """Получение всех открытых позиций."""
        pass

    @abc.abstractmethod
    async def hot_swap_credentials(self, new_key: str, new_secret: str) -> None:
        """Горячая замена API-ключей."""
        pass

class PaperTradingAdapter(IExchangeAdapter):
    """
    Paper Trading implementation using internal PaperTracker and DB.
    """
    def __init__(self, db_manager, paper_tracker):
        self.db = db_manager
        self.tracker = paper_tracker
        from core.metrics.execution_profiler import TradeExecutionProfiler
        self.profiler = TradeExecutionProfiler()

    async def execute_order(self, symbol: str, side: str, qty: float, price: float, 
                            order_link_id: str, **kwargs) -> Optional[Dict]:
        """Paper execution (always filled at requested price)."""
        params = kwargs.get('params', {})
        # Support both 'side' and 'direction' terminology
        direction = side.upper() if side.upper() in ["BUY", "SELL", "LONG", "SHORT"] else "LONG"
        if direction == "BUY": direction = "LONG"
        if direction == "SELL": direction = "SHORT"
        
        return await self.place_order(symbol, direction, qty, price, params)

    async def get_position(self, symbol: str) -> Optional[Dict]:
        """Returns position from PaperTracker."""
        return self.tracker.positions.get(symbol)

    async def get_all_positions(self) -> List[Dict]:
        """Returns all positions from PaperTracker."""
        return list(self.tracker.positions.values())

    async def hot_swap_credentials(self, new_key: str, new_secret: str) -> None:
        pass # Paper doesn't need real keys

    async def place_order(self, symbol: str, direction: str, qty: float, price: float, 
                          params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        In Paper, we simulate 100% Fill at requested price (Task 4.1 implementation).
        """
        # Extract metadata for PaperTracker.add_position
        stop_loss = params.get('stop_loss', 0.0)
        take_profit = params.get('take_profit', 0.0)
        tier = params.get('tier', 'C')
        
        # 1. Persist 'Initial Alert' in DB
        alert_id = await self.db.insert_alert(
            symbol=symbol,
            direction=direction,
            signal_type=params.get('signal_type', 'ORCHESTRATOR'),
            level_price=params.get('level_price', 0.0),
            entry_price=price,
            stop_price=stop_loss,
            target_price=take_profit,
            total_score=params.get('radar_score', 0),
            score_breakdown=params.get('score_breakdown', {}),
            rvol=params.get('rvol', 0.0),
            delta_oi_pct=0.0,
            funding_rate=0.0,
            btc_trend=params.get('btc_trend', 'FLAT'),
            liquidity_tier=tier
        )

        if alert_id:
            # 2. Add to PaperTracker
            self.tracker.add_position(
                alert_id=alert_id,
                symbol=symbol,
                direction=direction,
                entry_price=price,
                stop_price=stop_loss,
                target_price=take_profit,
                tier=tier
            )
            # Return new structured response (Task 4.1)
            return {
                "status": "FILLED", 
                "order_id": str(alert_id), 
                "filled_qty": qty, 
                "avg_price": price
            }
        
        return {"status": "REJECTED", "error": "DB_PERSISTENCE_ERROR", "filled_qty": 0.0, "avg_price": 0.0}

    async def amend_order(self, symbol: str, order_id: str, **kwargs) -> bool:
        return True # Paper always allows modification

    async def cancel_all_orders(self, symbol: str) -> bool:
        return True

    async def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        for pos in self.tracker.positions.values():
            if pos['symbol'] == symbol:
                return pos
        return None

    async def get_bbo(self, symbol: str) -> Optional[Any]:
        """Paper BBO from last price in tracker."""
        last_px = self.tracker.get_last_price(symbol)
        if last_px:
            class BBO:
                def __init__(self, p):
                    self.bid = p
                    self.ask = p
            return BBO(last_px)
        return None

    async def place_order(self, symbol: str, qty: float, price: float, side: str, 
                          time_in_force: str = "GTC", **kwargs) -> Dict:
        """Paper wrapper for nuke/reconciliation."""
        # Normalize side
        norm_side = "BUY" if side.upper() == "BUY" else "SELL"
        return await self.execute_order(symbol, norm_side, qty, price, 
                                        kwargs.get("order_link_id", "paper_nuke"))

try:
    from prometheus_client import Gauge
    # Метрики для Sentinel Dashboard (Task Final: Monitoring)
    LATENCY_EWMA_GAUGE = Gauge('gektor_latency_ewma_ms', 'Current EWMA of Bybit API Latency')
except ImportError:
    LATENCY_EWMA_GAUGE = None

class LatencyGuard:
    """
    Лимитатор задержек (Task Final: Circuit Breaker).
    Вычисляет EWMA RTT (Round Trip Time) для защиты от лагов биржи.
    """
    def __init__(self, alpha: float = 0.2, threshold_ms: int = 400, halt_threshold: int = 20):
        self.alpha = alpha
        self.threshold_ms = threshold_ms
        self.halt_threshold = halt_threshold
        self.ewma_rtt = 50.0 # Начальное значение (50мс)
        self.is_degraded = False
        self.consecutive_failures = 0
        self.is_halted = False

    def record_failure(self):
        """
        [P1] DMS Trigger: Record a terminal network failure (500/Timeout).
        If we hit the threshold (20), the entire Execution kontur is HALTED.
        """
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.halt_threshold:
            if not self.is_halted:
                self.is_halted = True
                logger.critical(f"🛑 [DMS-HALT] Global Circuit Breaker Tripped! {self.consecutive_failures} consecutive API failures.")
        
    def record_success(self):
        """Reset consecutive failures. Recover from Halt automatically on any success."""
        if self.consecutive_failures > 0:
            self.consecutive_failures = 0
            if self.is_halted:
                logger.info("🟢 [DMS-RECOVER] API connectivity restored. Circuit Breaker RESET.")
                self.is_halted = False

    def update(self, rtt_ms: float):
        """ Обновление скользящей средней задержки и экспорт метрик """
        self.record_success() 
        self.ewma_rtt = (self.alpha * rtt_ms) + (1.0 - self.alpha) * self.ewma_rtt
        self.is_degraded = self.ewma_rtt > self.threshold_ms
        
        # Экспорт в Prometheus/Grafana (Sentinel Integration)
        if LATENCY_EWMA_GAUGE:
            LATENCY_EWMA_GAUGE.set(self.ewma_rtt)
            
        if self.is_degraded:
            logger.warning(f"📉 [LatencyGuard] Exchange Lag Detected! EWMA RTT: {self.ewma_rtt:.1f}ms")

    def check_safe(self) -> bool:
        """Global safety check for Orchestrator."""
        return not self.is_halted and not self.is_degraded

class BybitLiveAdapter(IExchangeAdapter):
    """
    Live Bybit execution bridge (Task 3.3).
    Implements Idempotent Retry, Asymmetric Quantization and Institutional Auth Recovery (Audit 14).
    """
    def __init__(self, api_key: str, api_secret: str, rest_client, instruments_provider, db_manager=None):
        from .auth import BybitAuthManager
        from core.metrics.execution_profiler import TradeExecutionProfiler
        from core.alerts.pager import pager # [Gektor v6.25] OOB
        self.auth = BybitAuthManager(api_key, api_secret)
        self.rest = rest_client
        self.instruments = instruments_provider
        self.db = db_manager
        self.latency_guard = LatencyGuard()
        self.profiler = TradeExecutionProfiler()
        self.pager = pager
        self._sync_lock = asyncio.Lock()
        
        # [Gektor v6.25] Resilience: Hibernate vs Decapitation
        self.is_hibernating = False
        self.is_access_denied = False
        self._consecutive_5xx = 0
        self._hibernate_task = None
        self._dcp_active = False

    async def enable_dcp(self, window_sec: int = 15):
        """[Audit 25.9] Enable Bybit Disconnect Cancel All (DCP)."""
        logger.warning(f"🛡️ [Bybit] Enabling Disconnect-Cancel-All (DCP) | Window: {window_sec}s")
        payload = {"category": "linear", "timeWindow": window_sec}
        headers, _ = self.auth.sign_post_request(payload)
        try:
            resp = await self.rest._request("POST", "/v5/order/disconnected-cancel-all", params=payload, headers=headers)
            if int(resp.get("retCode", -1)) == 0:
                self._dcp_active = True
                logger.success("✅ [Bybit] DCP Protocol ACTIVE. Exchange will auto-cancel on loss of pulse.")
                return True
        except Exception as e:
            logger.error(f"❌ [Bybit] Failed to enable DCP: {e}")
        return False

    async def hot_swap_credentials(self, new_key: str, new_secret: str):
        from .auth import BybitAuthManager
        self.auth = BybitAuthManager(new_key, new_secret)
        self.is_access_denied = False # Reset on new keys
        logger.info("🔑 [LiveAdapter] Credentials updated via Hot-Swap. Authorization RESET.")

    def _start_hibernation(self, reason: str):
        """[Audit 24.5] Trigger Hibernate Protocol on 502/503/Maintenance."""
        if self.is_hibernating: return
        self.is_hibernating = True
        logger.warning(f"🐻 [HIBERNATE] Deep sleep engaged: {reason}. Execution suspended.")
        if not self._hibernate_task or self._hibernate_task.done():
            self._hibernate_task = asyncio.create_task(self._hibernate_recovery_loop())

    async def _hibernate_recovery_loop(self):
        """Пингует ПРИВАТНЫЙ эндпоинт и форсирует синхронизацию при пробуждении."""
        backoff = 30
        while self.is_hibernating:
            await asyncio.sleep(backoff)
            try:
                # 1. Пингуем ПРИВАТНЫЙ эндпоинт (гарантия готовности матчинг-движка)
                balance = await self.get_wallet_balance()
                
                if balance > 0:
                    logger.info("🌤️ [HIBERNATE] Private API responds. Initiating Wake-Up sequence...")
                    
                    # 2. КРИТИЧНО: Форсируем аудит реальности (Cold Boot Protocol)
                    from core.events.safe_publish import safe_publish
                    await safe_publish(ConnectionRestoredEvent(
                        source="OMS_HIBERNATE",
                        downtime_seconds=backoff, 
                        reconnect_count=1
                    ), session=None) # No active TX here, using best-effort publish
                    
                    # 3. Возвращаем управление
                    self.is_hibernating = False
                    self._consecutive_5xx = 0
                    logger.critical("🚀 [HIBERNATE OVER] System hydrated. Resuming AUTO operations.")
                    break
                    
            except Exception as e:
                logger.debug(f"💤 [HIBERNATE] Private API still unavailable: {e}")
                backoff = min(60, backoff * 1.2) 

    async def execute_order(self, symbol: str, side: str, qty: float, price: float, 
                             order_link_id: str, **kwargs) -> Optional[Dict]:
        """
        Executes real order on Bybit with HFT-TTL Control and Time-Sync Interception.
        """
        # 0. State Guard (HFT Protocol v6.25)
        if self.is_hibernating:
            logger.debug(f"🔇 [OMS] Order for {symbol} suppressed: System is HIBERNATING.")
            return {"status": "REJECTED", "error": "EXCHANGE_HIBERNATION_OUTAGE"}
            
        if self.is_access_denied:
            logger.critical(f"💀 [OMS] BLOCKED: Permanent Auth Loss. Triggering cellular pager if not done.")
            return {"status": "REJECTED", "error": "API_AUTH_REVOKED"}

        # HFT TTL: Mark signal creation time (Audit 14.1)
        creation_time_ms = int(time.time() * 1000)
        HFT_TTL_MS = 100 

        if not self.latency_guard.check_safe():
            logger.critical(f"🛑 [HALT] Latency Circuit Breaker triggered for {symbol}")
            return {"status": "REJECTED", "error": "EXCHANGE_LATENCY_DEGRADED"}

        # 1. Quantization (Asymmetric Logic)
        params = kwargs.get('params', {})
        is_long = side.upper() in ("BUY", "LONG")
        is_maker = params.get('order_type') == 'Maker'
        
        # [Audit 6.3] Support qty=0 for Atomic 'Close All' (Bybit V5 Perpetual Special)
        is_nuke = str(qty) == "0" or qty == 0.0
        
        if not is_nuke:
            try:
                q_price, q_qty = self.instruments.quantize_order(symbol, price, qty, is_long, is_maker)
            except Exception as e:
                logger.error(f"❌ [LiveAdapter] Quantization failed: {e}")
                return {"status": "REJECTED", "error": f"QUANT_FAIL:{e}"}
        else:
            # For nukes, we don't quantize price if it's a Market order, or use a dummy price for Limit
            q_qty = "0"
            q_price = price

        # Handle SL/TP quantization if provided
        sl_raw = params.get('stop_loss')
        tp_raw = params.get('take_profit')
        q_sl = self.instruments.quantize_order(symbol, sl_raw, 0, not is_long, False)[0] if sl_raw and not is_nuke else None
        q_tp = self.instruments.quantize_order(symbol, tp_raw, 0, not is_long, False)[0] if tp_raw and not is_nuke else None

        # 2. Payload Construction (Atomic Protection - Audit 15.2)
        payload = {
            "category": "linear", "symbol": symbol, "side": side.capitalize(),
            "orderType": params.get('order_type', "Limit"), 
            "timeInForce": params.get('tif', "IOC"), 
            "qty": str(q_qty), 
            "price": str(q_price) if q_price and float(q_price) > 0 else None,
            "orderLinkId": order_link_id,
            "positionIdx": params.get('positionIdx', 0), # One-way Mode
        }
        
        # Intent-driven Protection
        intent = kwargs.get('intent', OrderIntent.ENTRY)
        if intent == OrderIntent.EXIT:
            payload["reduceOnly"] = True
            payload["closeOnTrigger"] = True
        else:
            # Atomic SL/TP on server-side (Scaling support)
            if q_sl: 
                payload["stopLoss"] = str(q_sl)
                payload["slOrderType"] = "Market"
            if q_tp: 
                payload["takeProfit"] = str(q_tp)
                payload["tpOrderType"] = "Market"
            payload["tpslMode"] = "Full"

        # 3. Request loop with Terminal Error & Clock Drift protection
        TERMINAL_ERRORS = {10001, 10003, 10004, 10005, 10010} # API/IP mismatch/Invalid
        
        for attempt in range(2):
            try:
                # HFT TTL check
                current_time_ms = int(time.time() * 1000)
                if (current_time_ms - creation_time_ms) > HFT_TTL_MS:
                    logger.error(f"☠️ [HFT TTL EXPIRED] Drop {symbol}.")
                    return {"status": "DROPPED_TTL"}

                headers, _ = self.auth.sign_post_request(payload)
                t_send = time.perf_counter()
                resp = await self.rest._request("POST", "/v5/order/create", params=payload, headers=headers)
                rtt_ms = (time.perf_counter() - t_send) * 1000
                self.latency_guard.update(rtt_ms)

                ret_code = resp.get("retCode")
                ret_msg = resp.get("retMsg", "")

                # A. Exchange Outage / Maintenance (Transient)
                if str(ret_code).startswith('5') or ret_code in (10016, 10018):
                    self._consecutive_5xx += 1
                    if self._consecutive_5xx >= 3:
                        msg = f"Exchange outage (Status: {ret_code})" if ret_code != 10016 else "Bybit Scheduled Maintenance"
                        self._start_hibernation(msg)
                    return {"status": "ERROR", "error": f"EXCHANGE_TRANSIENT_{ret_code}"}

                # B. Terminal API Errors (Audit 15.3: Access Revoked)
                if ret_code in TERMINAL_ERRORS:
                    msg = f"🔥 [ULTRA-CRITICAL] API AUTH LOST (Code {ret_code}): {ret_msg}"
                    logger.critical(msg)
                    self.is_access_denied = True
                    
                    # 1. Out-of-Band Physical Call
                    asyncio.create_task(self.pager.trigger_nuclear_alarm(f"AUTH LOST {ret_code}"))
                    
                    # 2. Trigger System-Wide Event (Durable Outbox)
                    from core.events.safe_publish import safe_publish_critical
                    await safe_publish_critical(
                        EmergencyAlertEvent(message=msg, severity="P0"),
                        db_manager=self.db
                    )
                    
                    return {"status": "ERROR", "error": f"API_AUTH_FAILED_{ret_code}"}

                if ret_code == 0 or ret_code is None: 
                    # HFT TELEMETRY: Profile the execution (Audit 15.6)
                    try:
                        self.profiler.profile_execution(
                            symbol=symbol,
                            is_long=(side.lower() == "buy"),
                            signal_price=Decimal(str(q_price)),
                            exec_price=Decimal(str(q_price)), 
                            qty=Decimal(str(q_qty)),
                            rtt_ms=rtt_ms
                        )
                    except Exception: pass
                    
                    return {"status": "FILLED", "order_id": resp.get("orderId"), "filled_qty": q_qty, "avg_price": q_price, "rtt_ms": rtt_ms}
                
                # C. Idempotency Collision
                if ret_code == 110006:
                    return await self.reconcile_order_state(symbol, order_link_id)
                
                # D. Clock Drift (10002)
                if ret_code == 10002:
                    logger.warning(f"🕒 [SYNC] Drift detected for {symbol}. Aligning clock...")
                    try:
                        async with asyncio.timeout(0.5):
                            async with self._sync_lock:
                                if self.auth.needs_sync(): await self.auth.sync_time(self.rest)
                        continue 
                    except asyncio.TimeoutError:
                        return {"status": "DROPPED_SYNC_TIMEOUT"}

            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                self.latency_guard.record_failure()
                logger.warning(f"🌐 [UNCERTAIN STATE] Network fail on {order_link_id} for {symbol}: {e}. Spawning Recon Task.")
                asyncio.create_task(self._background_reconcile(symbol, order_link_id))
                return {"status": "PENDING_RECONCILIATION", "order_link_id": order_link_id}
            except Exception as e:
                logger.error(f"💥 [LiveAdapter] Final Local Error: {e}")
                return {"status": "ERROR", "error": str(e)}
        
        return {"status": "ERROR", "error": "RETRY_EXHAUSTED"}

    async def _background_reconcile(self, symbol: str, order_link_id: str):
        """
        Dual-Path Reconciliation (Audit 16.4).
        Path A: Private WebSocket (Instant Push - handled in WS callback).
        Path B: This poll (Fallback).
        """
        logger.info(f"🔍 [RECON-TASK] Started for {order_link_id} | {symbol}")
        
        for attempt in range(5): # More thorough background polling
            # Exponential Backoff for REST protection (Audit 16.5)
            await asyncio.sleep(0.5 * (2 ** attempt))
            
            # Check if WS already filled this order (Global Execution State check)
            # from core.realtime.execution_state import execution_state
            # if execution_state.is_order_finalized(order_link_id): 
            #    logger.info(f"✅ [RECON-TASK] Order {order_link_id} already finalized by Private WS. Aborting REST poll.")
            #    return

            headers = self.auth.sign_get_request({"category": "linear", "symbol": symbol, "orderLinkId": order_link_id})
            try:
                resp = await self.rest.get_order_realtime(symbol, order_link_id, headers)
                order_list = resp.get("list", []) if isinstance(resp, dict) else []
                
                if order_list:
                    info = order_list[0]
                    status = info.get("orderStatus")
                    logger.info(f"🔄 [RECON-TASK] SUCCESS: Order {order_link_id} is {status}")
                    
                    # Notify Orchestrator/OMS via NerveCenter
                    # Notify Orchestrator/OMS via NerveCenter (Buffered)
                    from core.events.safe_publish import safe_publish
                    await safe_publish(OrderUpdateEvent(
                        symbol=symbol, order_link_id=order_link_id, status=status,
                        filled_qty=float(info.get("cumExecQty", 0)), 
                        avg_price=float(info.get("avgPrice", 0))
                    ))
                    return
            except Exception as e:
                logger.error(f"🚨 [RECON-TASK] Try {attempt+1} failed: {e}")
                
        logger.critical(f"🛑 [FATAL RECON] Order {order_link_id} remains UNKNOWN after 5 REST attempts.")

    async def reconcile_order_state(self, symbol: str, order_link_id: str) -> Dict:
        """
        [Gektor v5.5] Ground-Truth Order State Check.
        
        Called on:
        - 5xx errors (exchange accepted but returned server error)
        - Idempotency Collision (retCode 110006)
        
        Returns normalized response dict for the OMS caller.
        Previously this method was missing, causing AttributeError on 5xx.
        """
        logger.info(f"🔍 [RECON] reconcile_order_state for {order_link_id} | {symbol}")
        
        for attempt in range(3):
            await asyncio.sleep(0.3 * (2 ** attempt))  # 0.3, 0.6, 1.2s
            
            try:
                headers = self.auth.sign_get_request({
                    "category": "linear", 
                    "symbol": symbol, 
                    "orderLinkId": order_link_id
                })
                resp = await self.rest.get_order_realtime(symbol, order_link_id, headers)
                order_list = resp.get("list", []) if isinstance(resp, dict) else []
                
                if order_list:
                    info = order_list[0]
                    status = info.get("orderStatus", "")
                    filled_qty = float(info.get("cumExecQty", 0))
                    avg_price = float(info.get("avgPrice", 0))
                    order_id = info.get("orderId", order_link_id)
                    
                    logger.info(
                        f"✅ [RECON] Order {order_link_id} confirmed: {status} "
                        f"(filled: {filled_qty}, avg: {avg_price})"
                    )
                    
                    # Map Bybit status to normalized OMS response
                    if status in ("Filled", "PartiallyFilled"):
                        return {
                            "status": "FILLED", 
                            "order_id": order_id, 
                            "filled_qty": filled_qty, 
                            "avg_price": avg_price
                        }
                    elif status in ("Cancelled", "Rejected", "Deactivated"):
                        return {
                            "status": "REJECTED", 
                            "order_id": order_id, 
                            "filled_qty": filled_qty, 
                            "error": f"ORDER_{status.upper()}"
                        }
                    elif status == "New":
                        # Order accepted but not yet filled — return as pending
                        return {
                            "status": "PENDING", 
                            "order_id": order_id, 
                            "filled_qty": 0
                        }
                    else:
                        return {
                            "status": "UNKNOWN",
                            "order_id": order_id,
                            "error": f"UNEXPECTED_STATUS:{status}"
                        }
                        
            except Exception as e:
                logger.error(f"🚨 [RECON] Attempt {attempt+1}/3 failed for {order_link_id}: {e}")
        
        # Exhausted retries — spawn background reconcile as fallback
        logger.warning(f"⚠️ [RECON] Inline reconciliation failed. Spawning background task for {order_link_id}.")
        asyncio.create_task(self._background_reconcile(symbol, order_link_id))
        return {"status": "PENDING_RECONCILIATION", "order_link_id": order_link_id}

    async def get_position(self, symbol: str) -> Optional[Dict]:
        """[Audit 16.31] Ground Truth Position Query (Priority: CRITICAL)."""
        headers = self.auth.sign_get_request({"category": "linear", "symbol": symbol})
        try:
            resp = await self.rest._request("GET", "/v5/position/list", 
                                          params={"category": "linear", "symbol": symbol},
                                          headers=headers)
            pos_list = resp.get("result", {}).get("list", [])
            if pos_list:
                pos = pos_list[0]
                return {
                    "symbol": pos.get("symbol"),
                    "size": float(pos.get("size", 0)),
                    "side": pos.get("side"),
                    "avg_price": float(pos.get("avgPrice", 0)),
                    "stop_loss": float(pos.get("stopLoss", 0))
                }
            return None
        except Exception as e:
            logger.error(f"❌ [Bybit] Position fetch error: {e}")
            return None

    async def cancel_all_orders(self, symbol: str) -> bool:
        """[Audit 25.8] Orphaned State Cleanup: Cancel all Bybit orders."""
        payload = {"category": "linear", "symbol": symbol}
        headers, _ = self.auth.sign_post_request(payload)
        try:
            resp = await self.rest._request("POST", "/v5/order/cancel-all", 
                                          params=payload, headers=headers)
            return resp.get("retCode") == 0
        except Exception as e:
            logger.error(f"❌ [Bybit] Cancel-all failure: {e}")
            return False

    async def amend_order(self, symbol: str, order_id: str, **kwargs) -> bool:
        """[Audit 5.12] Rate-Limited SL Synchronization."""
        payload = {"category": "linear", "symbol": symbol}
        if order_id.startswith("oms_") or order_id.startswith("evac_") or order_id.startswith("exit_"):
            payload["orderLinkId"] = order_id
        else:
            payload["orderId"] = order_id
            
        if "stop_loss" in kwargs: payload["stopLoss"] = str(kwargs["stop_loss"])
        if "take_profit" in kwargs: payload["takeProfit"] = str(kwargs["take_profit"])
        if "price" in kwargs: payload["price"] = str(kwargs["price"])
        
        headers, _ = self.auth.sign_post_request(payload)
        resp = await self.rest._request("POST", "/v5/order/amend", params=payload, headers=headers)
        return int(resp.get("retCode", -1)) == 0

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        payload = {"category": "linear", "symbol": symbol, "orderId": order_id}
        headers, _ = self.auth.sign_post_request(payload)
        resp = await self.rest._request("POST", "/v5/order/cancel", params=payload, headers=headers)
        return int(resp.get("retCode", -1)) == 0

    async def cancel_all_orders(self, symbol: str) -> bool:
        """[Audit 5.11] Cancel All active orders for symbol (Atomic Cleanup)."""
        payload = {"category": "linear", "symbol": symbol}
        headers, _ = self.auth.sign_post_request(payload)
        resp = await self.rest._request("POST", "/v5/order/cancel-all", params=payload, headers=headers)
        return int(resp.get("retCode", -1)) == 0

    async def get_position(self, symbol: str) -> Optional[Dict]:
        params = {"category": "linear", "symbol": symbol}
        headers = self.auth.sign_get_request(params)
        resp = await self.rest._request("GET", "/v5/position/list", params=params, headers=headers)
        return resp[0] if isinstance(resp, list) and resp else None

    async def get_all_positions(self) -> List[Dict]:
        params = {"category": "linear", "settleCoin": "USDT"}
        headers = self.auth.sign_get_request(params)
        resp = await self.rest._request("GET", "/v5/position/list", params=params, headers=headers)
        return resp if isinstance(resp, list) else []

    async def get_wallet_balance(self, coin: str = "USDT") -> float:
        """
        [Gektor v5.6] Dynamic Equity Provider.
        Fetches 'walletBalance' from Bybit Unified Account.
        Used by RiskGuard 2.0 for Risk-per-Trade (%) calculations.
        """
        try:
            params = {"accountType": "UNIFIED", "coin": coin}
            headers = self.auth.sign_get_request(params)
            # Use rest._request directly as get_wallet_balance helper might not exist in all rest_client versions
            resp = await self.rest._request("GET", "/v5/account/wallet-balance", params=params, headers=headers)
            
            # Protocol: resp -> list[0] -> coin[i] where coin == "USDT"
            if isinstance(resp, dict) and "list" in resp:
                account_data = resp["list"][0]
                for c_data in account_data.get("coin", []):
                    if c_data.get("coin") == coin:
                        balance = float(c_data.get("walletBalance", 0.0))
                        logger.info(f"💰 [LiveAdapter] Current {coin} Equity: ${balance:,.2f}")
                        return balance
            
            # Fallback for non-Unified or unexpected structure
            logger.warning(f"⚠️ [LiveAdapter] Unrecognized balance structure for {coin}. Returning 0.")
            return 0.0
        except Exception as e:
            logger.error(f"🚨 [LiveAdapter] Balance fetch FAILED: {e}")
            return 0.0

    async def get_bbo(self, symbol: str) -> Optional[Any]:
        """
        [P0] Fetch Best Bid/Ask for Limit-Chase (v6.9).
        Returns a simple object with .bid and .ask attributes.
        """
        params = {"category": "linear", "symbol": symbol}
        headers = self.auth.sign_get_request(params)
        try:
            resp = await self.rest._request("GET", "/v5/market/tickers", params=params, headers=headers)
            if isinstance(resp, dict) and "list" in resp and len(resp["list"]) > 0:
                ticker = resp["list"][0]
                bid = float(ticker.get("bid1Price", 0))
                ask = float(ticker.get("ask1Price", 0))
                
                # Anonymous class for .bid/.ask access
                class BBO:
                    def __init__(self, b, a):
                        self.bid = b
                        self.ask = a
                return BBO(bid, ask)
        except Exception as e:
            logger.error(f"🚨 [Bybit] Failed to fetch BBO for {symbol}: {e}")
        return None

    async def place_order(self, symbol: str, qty: float, price: float, side: str, 
                          time_in_force: str = "GTC", **kwargs) -> Dict:
        """
        [P0] Low-level order placement for Nuke / Reconciliation.
        Simple wrapper around execute_order with IOC/GTC support.
        """
        # side in snippet is "SELL"/"BUY", our adapter expects same or Bybit-native 
        # Bybit-native: "Buy"/"Sell"
        normalized_side = "Buy" if side.upper() == "BUY" else "Sell"
        
        # Use existing execute_order logic but allow TIF override
        resp = await self.execute_order(
            symbol=symbol,
            side=normalized_side,
            qty=qty,
            price=price,
            order_link_id=kwargs.get("order_link_id", f"nuke_{int(time.time()*1000)}"),
            params={
                "order_type": "Limit",
                "tif": time_in_force,
                "reduceOnly": True # Always reduce on nuke
            }
        )
        
        # Snippet expects "executedQty"
        if resp:
            resp["executedQty"] = resp.get("filled_qty", 0.0)
        return resp or {"status": "ERROR", "executedQty": 0.0}

class BinanceLiveAdapter(IExchangeAdapter):
    """
    Адаптер для Binance (USD-M Futures) с проактивным контролем весов (Audit 15.3).
    Используется как резервный контур хеджирования для SOR.
    """
    
    def __init__(self, api_key: str, secret: str, proxy_url: str = None):
        from data.binance_weight_manager import BinanceWeightManager, Priority
        self._current_key = api_key
        self._current_secret = secret
        self.proxy_url = proxy_url
        self.weight_manager = BinanceWeightManager()
        self.Priority = Priority
        self._auth_lock = asyncio.Lock()
        self.session = None

    async def _get_session(self):
        import httpx
        if self.session is None or self.session.is_closed:
            self.session = httpx.AsyncClient(
                base_url="https://fapi.binance.com",
                proxy=self.proxy_url,
                http2=True,
                timeout=10.0
            )
        return self.session

    def _generate_signature(self, query: str) -> str:
        import hmac, hashlib
        return hmac.new(
            self._current_secret.encode('utf-8'),
            query.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    async def execute_order(self, symbol: str, side: str, qty: float, price: float = 0.0, 
                            order_link_id: str = None, **kwargs) -> Optional[Dict]:
        """Исполнение ордера на Binance с учетом весов и приоритетов [Audit 25.5: Symmetric SL/TP]."""
        await self.weight_manager.acquire(self.Priority.CRITICAL)
        
        timestamp = int(time.time() * 1000)
        # Audit 16.3: Prohibit pure MARKET for hedging. Use aggressive LIMIT IOC.
        order_type = kwargs.get("type", "LIMIT" if price > 0 else "MARKET")
        tif = kwargs.get("tif", "GTC")
        params = kwargs.get("params", {})
        
        params_dict = {
            "symbol": symbol,
            "side": side.upper(),
            "type": order_type,
            "quantity": str(qty),
            "recvWindow": 5000,
            "timestamp": timestamp
        }
        if order_type == "LIMIT":
            params_dict["price"] = str(price)
            params_dict["timeInForce"] = tif
        
        if order_link_id:
            params_dict["newClientOrderId"] = order_link_id

        # [Audit 25.5] Support Symmetric Stops (Bybit Mirror)
        # Binance Futures STOP_MARKET orders are separate requests.
        # However, we can try to find them in params.
        sl_price = params.get("stop_loss")
        tp_price = params.get("take_profit")

        query = "&".join([f"{k}={v}" for k, v in params_dict.items()])
        signature = self._generate_signature(query)
        url = f"/fapi/v1/order?{query}&signature={signature}"
        
        headers = {"X-MBX-APIKEY": self._current_key}
        session = await self._get_session()
        
        try:
            resp = await session.post(url, headers=headers)
            await self.weight_manager.update_from_headers(resp.status_code, resp.headers)
            
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"✅ [Binance] Order {side} {qty} {symbol} processed ({data.get('status')}).")
                
                # --- [Audit 25.5] Execute Symmetric STOP_MARKET / TAKE_PROFIT_MARKET ---
                if sl_price or tp_price:
                    asyncio.create_task(self._place_secondary_stops(symbol, side, sl_price, tp_price))
                
                exec_status = data.get("status")
                return {
                    "status": "FILLED" if exec_status in ("FILLED", "PARTIALLY_FILLED") else "REJECTED", 
                    "order_id": data.get("orderId"), 
                    "filled_qty": float(data.get("executedQty", 0)), 
                    "avg_price": float(data.get("avgPrice", 0))
                }
            
            if resp.status_code >= 500:
                # Audit 16.3: NETWORK_UNCERTAINTY
                logger.error(f"⚠️ [Binance] HTTP {resp.status_code} — State is UNCERTAIN.")
                return {"status": "UNCERTAIN", "error": f"Internal Server Error: {resp.status_code}"}

            logger.error(f"❌ [Binance] Order REJECTED ({resp.status_code}): {resp.text}")
            return {"status": "REJECTED", "error": f"Binance:{resp.status_code}:{resp.text}"}
            
        except (asyncio.TimeoutError, httpx.TimeoutException, httpx.NetworkError) as e:
            # Audit 16.3: Network timeout = Uncertainty
            logger.error(f"⚠️ [Binance] Network/Timeout error — State is UNCERTAIN: {e}")
            return {"status": "UNCERTAIN", "error": f"Network Uncertainty: {repr(e)}"}
        except Exception as e:
            logger.error(f"❌ [Binance] Order Execution Critical Error: {e}")
            return {"status": "ERROR", "error": str(e)}

    async def _place_secondary_stops(self, symbol: str, side: str, sl_price: float = None, tp_price: float = None):
        """Asynchronously places SL/TP orders on Binance to maintain symmetry."""
        # For a Buy order, SL is a Sell STOP_MARKET below price.
        # For a Sell order, SL is a Buy STOP_MARKET above price.
        stop_side = "SELL" if side.upper() == "BUY" else "BUY"
        headers = {"X-MBX-APIKEY": self._current_key}
        session = await self._get_session()

        if sl_price and float(sl_price) > 0:
            ts = int(time.time() * 1000)
            q = f"symbol={symbol}&side={stop_side}&type=STOP_MARKET&stopPrice={sl_price}&closePosition=true&timestamp={ts}"
            s = self._generate_signature(q)
            try:
                await session.post(f"/fapi/v1/order?{q}&signature={s}", headers=headers)
                logger.info(f"🛡️ [Binance] Symmetric SL deployed for {symbol} @ {sl_price}")
            except Exception as e:
                logger.error(f"⚠️ [Binance] SL Deployment FAILED: {e}")

        if tp_price and float(tp_price) > 0:
            ts = int(time.time() * 1000)
            q = f"symbol={symbol}&side={stop_side}&type=TAKE_PROFIT_MARKET&stopPrice={tp_price}&closePosition=true&timestamp={ts}"
            s = self._generate_signature(q)
            try:
                await session.post(f"/fapi/v1/order?{q}&signature={s}", headers=headers)
                logger.info(f"🛡️ [Binance] Symmetric TP deployed for {symbol} @ {tp_price}")
            except Exception as e:
                logger.error(f"⚠️ [Binance] TP Deployment FAILED: {e}")

    async def get_order_status(self, symbol: str, order_id: str = None, order_link_id: str = None) -> Optional[Dict]:
        """Сверка статуса ордера с резервным опросом истории (Audit 16.9)."""
        await self.weight_manager.acquire(self.Priority.CRITICAL)
        
        params_dict = {"symbol": symbol, "timestamp": int(time.time() * 1000)}
        if order_id: params_dict["orderId"] = order_id
        if order_link_id: params_dict["origClientOrderId"] = order_link_id
        
        query = "&".join([f"{k}={v}" for k, v in params_dict.items()])
        signature = self._generate_signature(query)
        headers = {"X-MBX-APIKEY": self._current_key}
        session = await self._get_session()
        
        # 1. Попытка получить статус из активных ордеров
        try:
            url_active = f"/fapi/v1/order?{query}&signature={signature}"
            resp = await session.get(url_active, headers=headers)
            await self.weight_manager.update_from_headers(resp.status_code, resp.headers)
            
            if resp.status_code == 200:
                data = resp.json()
                return self._parse_binance_resp(data)
            
            # 2. Если 'No such order' (400), опрашиваем историю (AllOrders / UserTrades)
            if resp.status_code == 400:
                logger.debug(f"🔍 [Binance] No active order for {order_link_id}. Probing history...")
                # allOrders (LIMIT 10) для поиска по clOrdId в терминальных
                url_history = f"/fapi/v1/allOrders?{query}&signature={signature}"
                resp_h = await session.get(url_history, headers=headers)
                if resp_h.status_code == 200:
                    history = resp_h.json()
                    # Т.к. мы передали origClientOrderId, Binance вернет список с этим ордером
                    if history and isinstance(history, list):
                        return self._parse_binance_resp(history[0])
            
            return None
        except Exception as e:
            logger.error(f"❌ [Binance] GetOrderStatus/History Error: {e}")
            return None

    def _parse_binance_resp(self, data: Dict) -> Dict:
        return {
            "status": data.get("status"),
            "filled_qty": float(data.get("executedQty", 0)),
            "avg_price": float(data.get("avgPrice", 0))
        }


    async def get_position(self, symbol: str) -> Optional[Dict]:
        """Получение позиции по символу (Priority: BACKGROUND)."""
        await self.weight_manager.acquire(self.Priority.BACKGROUND)
        
        timestamp = int(time.time() * 1000)
        query = f"symbol={symbol}&timestamp={timestamp}"
        signature = self._generate_signature(query)
        url = f"/fapi/v2/positionRisk?{query}&signature={signature}"
        
        headers = {"X-MBX-APIKEY": self._current_key}
        session = await self._get_session()
        
        try:
            resp = await session.get(url, headers=headers)
            await self.weight_manager.update_from_headers(resp.status_code, resp.headers)
            
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    pos = data[0]
                    return {
                        "symbol": pos.get("symbol"),
                        "size": float(pos.get("positionAmt", 0)),
                        "entry_price": float(pos.get("entryPrice", 0)),
                        "unrealized_pnl": float(pos.get("unRealizedProfit", 0))
                    }
            return None
        except Exception as e:
            logger.error(f"❌ [Binance] Position fetch error: {e}")
            return None

    async def get_all_positions(self) -> List[Dict]:
        """Получение всех активных позиций (Priority: BACKGROUND)."""
        await self.weight_manager.acquire(self.Priority.BACKGROUND)
        
        timestamp = int(time.time() * 1000)
        query = f"timestamp={timestamp}"
        signature = self._generate_signature(query)
        url = f"/fapi/v2/positionRisk?{query}&signature={signature}"
        
        headers = {"X-MBX-APIKEY": self._current_key}
        session = await self._get_session()
        
        try:
            resp = await session.get(url, headers=headers)
            await self.weight_manager.update_from_headers(resp.status_code, resp.headers)
            
            if resp.status_code == 200:
                data = resp.json()
                return [
                    {
                        "symbol": pos.get("symbol"),
                        "size": float(pos.get("positionAmt", 0)),
                        "entry_price": float(pos.get("entryPrice", 0))
                    }
                    for pos in data if abs(float(pos.get("positionAmt", 0))) > 1e-8
                ]
            return []
        except Exception as e:
            logger.error(f"❌ [Binance] All positions fetch error: {e}")
            return []

        except Exception:
            return False

    async def cancel_all_orders(self, symbol: str) -> bool:
        """Отмена всех ордеров на Binance по символу (Priority: CRITICAL)."""
        await self.weight_manager.acquire(self.Priority.CRITICAL)
        timestamp = int(time.time() * 1000)
        query = f"symbol={symbol}&timestamp={timestamp}"
        signature = self._generate_signature(query)
        url = f"/fapi/v1/allOpenOrders?{query}&signature={signature}"
        
        headers = {"X-MBX-APIKEY": self._current_key}
        session = await self._get_session()
        try:
            resp = await session.delete(url, headers=headers)
            await self.weight_manager.update_from_headers(resp.status_code, resp.headers)
            return resp.status_code == 200
        except Exception:
            return False

    async def enable_countdown_cancel(self, symbol: str, window_ms: int = 15000):
        """[Audit 25.9] Enable Binance Countdown Cancel (Dead Man's Switch)."""
        timestamp = int(time.time() * 1000)
        query = f"symbol={symbol}&countdownTime={window_ms}&timestamp={timestamp}"
        signature = self._generate_signature(query)
        url = f"/fapi/v1/countdownCancelAll?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self._current_key}
        session = await self._get_session()
        try:
            resp = await session.post(url, headers=headers)
            return resp.status_code == 200
        except Exception:
            return False

    async def hot_swap_credentials(self, new_key: str, new_secret: str):
        """Горячая замена ключей под защитой Lock."""
        async with self._auth_lock:
            self._current_key = new_key
            self._current_secret = new_secret
            logger.info("🔑 [Binance] Credentials Swapped Successfully.")

