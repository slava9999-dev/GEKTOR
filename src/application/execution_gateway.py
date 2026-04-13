import time
import uuid
import asyncio
import logging
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger("GEKTOR.Execution")

@dataclass(slots=True, frozen=True)
class ExecutionReceipt:
    client_oid: str
    symbol: str
    filled_qty: float
    avg_price: float
    status: str  # FILLED, PARTIAL, REJECTED, UNKNOWN

class ExecutionGateway:
    """[GEKTOR v2.0] Идемпотентный шлюз исполнения и сверки стейта (State Reconciliation)."""
    __slots__ = ['api_client', 'max_slippage_bps']

    def __init__(self, api_client, max_slippage_bps: int = 50):
        self.api_client = api_client
        self.max_slippage_bps = max_slippage_bps

    async def execute_liquidation(self, symbol: str, qty: float, reference_price: float) -> ExecutionReceipt:
        # 1. Генерация идемпотентного ключа ДО сетевого I/O (Kleppmann Rule)
        client_oid = f"gktr_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
        
        # 2. Защита от проскальзывания (Quant Rule)
        limit_price = reference_price * (1 - (self.max_slippage_bps / 10000.0))
        
        logger.critical(f"🔥 [EXECUTION] Fire order {client_oid} | {symbol} | QTY: {qty} | MIN_PRICE: {limit_price}")

        try:
            # 3. Отправка IOC-ордера (Immediate-Or-Cancel)
            response = await self.api_client.place_order(
                symbol=symbol,
                side="SELL",
                order_type="LIMIT",
                time_in_force="IOC", 
                qty=qty,
                price=limit_price,
                client_order_id=client_oid
            )
            return self._parse_receipt(response, client_oid)
            
        except TimeoutError:
            # Стейт неизвестен. Биржа могла исполнить ордер, но не вернуть ответ.
            logger.error(f"☠️ [BYZANTINE FAULT] Timeout on {client_oid}. State is UNKNOWN.")
            return ExecutionReceipt(client_oid, symbol, 0.0, 0.0, "UNKNOWN")
            
        except Exception as e:
            logger.error(f"❌ [EXECUTION FAILED] {e}")
            return ExecutionReceipt(client_oid, symbol, 0.0, 0.0, "REJECTED")

    async def reconcile_dark_state(self, client_oid: str, symbol: str, qty: float, reference_price: float) -> ExecutionReceipt:
        """
        Процедура Сверки Стейта (State Reconciliation) при падении сети.
        Асимптотическая гарантия отсутствия Double Execution.
        """
        logger.warning(f"🔍 [RECONCILE] Entering Dark State resolution for {client_oid}...")
        
        retries = 0
        while retries < 10:
            try:
                # 1. Poll the REST API for EXACT client_oid state
                response = await self.api_client.get_order_history(symbol=symbol, client_order_id=client_oid)
                
                # 2. Kleppmann's Check: Did the exchange ever see it?
                if not response.get('result', {}).get('list'):
                    # Order Not Found. Это значит, что TimeoutError (502) случился ДО 
                    # попадания ордера в Matching Engine. Позиция открыта!
                    logger.critical(f"🛡️ [RECONCILE] Order {client_oid} completely rejected by Matching Engine.")
                    
                    # Безопасно генерируем НОВЫЙ выстрел
                    return await self.execute_liquidation(symbol, qty, reference_price)
                
                # 3. Match was processed internally by Exchange
                return self._parse_receipt(response, client_oid)
                
            except Exception as e:
                retries += 1
                backoff = min(10, 2 ** retries)
                logger.error(f"📡 [RECONCILE] Network still down. Retrying in {backoff}s. {e}")
                await asyncio.sleep(backoff)
                
        # Если мы здесь, значит сеть лежит фатально. VoIP Guardian уже разбудил Оператора.
        return ExecutionReceipt(client_oid, symbol, 0.0, 0.0, "UNKNOWN")

    def _parse_receipt(self, response: dict, expected_oid: str) -> ExecutionReceipt:
        # Упрощенный маппер для ответа Bybit/Binance V5
        list_orders = response.get('result', {}).get('list', [])
        if not list_orders:
            return ExecutionReceipt(expected_oid, "UNKNOWN", 0.0, 0.0, "UNKNOWN")
            
        data = list_orders[0]
        return ExecutionReceipt(
            client_oid=expected_oid,
            symbol=data.get('symbol', 'UNKNOWN'),
            filled_qty=float(data.get('cumExecQty', 0.0)),
            avg_price=float(data.get('avgPrice', 0.0)),
            status=data.get('orderStatus', 'UNKNOWN')
        )
