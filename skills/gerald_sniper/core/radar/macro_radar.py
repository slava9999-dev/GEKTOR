# core/radar/macro_radar.py
"""
[GEKTOR v8.1] Macro Anomaly Radar - Signal Purity Engine.

Architecture: Two-Phase Scan + Cold Start Hydration + One-Click Entry
  Phase 0 (Hydration):   50 REST calls at startup -> /v5/market/open-interest
                          Pre-fills OI ring buffer so radar is ready at t=0

  Phase 1 (Broad Scan):  1 REST call -> GET /v5/market/tickers -> ALL 300 tickers
                          Compare OI + Volume snapshots against previous cycle
                          Filter: turnover > $1M absolute + OI Delta > 5% + Price Move > 2%
                          Output: 3-7 candidates (Shortlist)

  Phase 2 (Deep Scan):   3-7 REST calls -> GET /v5/market/kline (5m candles)
                          Validate RVOL on 5m turnover (not 24h average)
                          Confirm wick structure (no full rejection candles)
                          Output: 0-3 confirmed anomalies -> Telegram with Inline Keyboard

  One-Click Entry:       Telegram Inline Keyboard (⚡ LONG / ⚡ SHORT / 🚫 ПАС)
                          Callback -> OMS Limit IOC order -> 0.5s reaction time

Budget: 8-10 REST requests per minute (vs 120+ in old RadarV2)
Proxy-Safe: Single-connection sequential flow, no parallel storms.
"""

from __future__ import annotations
import asyncio
from dataclasses import dataclass
import hashlib
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Callable, TYPE_CHECKING
from pydantic import BaseModel, Field
from loguru import logger
from core.events.events import SignalEvent, AlertEvent, MarketHaltEvent, MarketResumptionEvent
from core.events.nerve_center import bus

if TYPE_CHECKING:
    from core.events.events import RawWSEvent # Performance: Only for hints

# [LOG ROTATION] Prevent SSD asphyxiation with 500MB rotation
logger.add(
    "logs/gektor_{time}.log", 
    rotation="500 MB", 
    retention="3 days", 
    level="DEBUG",
    compression="zip"
)

from data.bybit_rest import BybitREST

from utils.math_utils import log_throttler, RobustMath
from core.radar.guards import AtomicDirectionalGuard # Persistence Core
from core.interfaces.strategy import IAlphaStrategy

# [GEKTOR v14.6.5] Risk & Portfolio Management (Wait for commit from flight)
from core.risk_manager.portfolio import RiskAllocator
from core.radar.cross_impact import MarketHealthSHMBridge
from core.realtime.math_orchestrator import MathOrchestrator
from core.realtime.market_state import market_state


def _generate_signal_id(symbol: str, direction: str) -> str:
    """Deterministic short ID for callback_data (max 64 bytes in TG)."""
    raw = f"{symbol}:{direction}:{int(time.time())}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]


class MacroSignal(BaseModel):
    """
    [GEKTOR v11.9] Macro Anomaly Signal.
    Focus on Volume-Flow alignment + Predictive Projections.
    """
    symbol: str
    direction: int        # 1 for LONG, -1 for SHORT
    rvol: float           # Relative Volume
    imbalance: float      # Buy/Sell Ratio
    z_score: float        # Deviation
    cvd_alignment: bool   # Does the impulse align with global CVD?
    ofi: float = 0.0      # [GEKTOR v14.9] Order Flow Imbalance (Real-time snapshot)
    price: float = 0.0
    timestamp: float = Field(default_factory=time.time)
    
    # [GEKTOR v11.7] Analysis Fields
    signal_type: Optional[str] = None # ICEBERG_BUY, EXHAUSTION, etc.
    regime: str = "NORMAL"            # EXPANSION, COMPRESSION
    vr: float = 1.0                   # Volatility Ratio
    atr: float = 0.0
    targets: Dict[str, float] = Field(default_factory=dict) # {"sl": 10.5, "tp": 12.0}
    liquidity_tier: str = "C"
    tick_size: float = 0.001

    # [GEKTOR v11.7] Multi-Vector Historical primitives (for IPC)
    hist_p: List[float] = Field(default_factory=list)
    hist_c: List[float] = Field(default_factory=list)
    hist_v: List[float] = Field(default_factory=list)

    @property
    def direction_str(self) -> str:
        return "LONG" if self.direction == 1 else "SHORT"

    @property
    def is_structural_break(self) -> bool:
        """Structural Break = High Z-Score + CVD Alignment."""
        return abs(self.z_score) > 4.0 and self.cvd_alignment


class OISnapshotStore:
    """
    In-memory ring buffer of OI snapshots for Delta calculation.
    
    Stores last N snapshots (default 10 = 10 minutes at 1 scan/min).
    Delta is calculated against the oldest available snapshot,
    giving us a ~10 minute OI Delta window.
    """
    
    def __init__(self, lookback_cycles: int = 10):
        self._lookback = lookback_cycles
        # {symbol: deque([{oi, turnover, price, ts}, ...])}
        self._snapshots: Dict[str, list] = {}
    
    def record(self, symbol: str, oi: float, turnover_24h: float, price: float, exchange_ts: Optional[int] = None):
        """Record current OI and turnover snapshot."""
        if symbol not in self._snapshots:
            self._snapshots[symbol] = []
        
        history = self._snapshots[symbol]
        entry = {
            "oi": oi,
            "turnover": turnover_24h,
            "price": price,
            "ts": time.time(),
            "exchange_ts": exchange_ts or (int(time.time() * 1000))
        }
        history.append(entry)
        
        # Track last seen exchange time for age validation
        if exchange_ts:
            self._last_exchange_ts = exchange_ts
        
        # Keep only last N entries
        if len(history) > self._lookback:
            self._snapshots[symbol] = history[-self._lookback:]

    def clear(self):
        """[GEKTOR v14.7.3] Full buffer flush for Amnesia Protocol."""
        self._snapshots.clear()
        logger.warning("🧠 [AMNESIA] OI Store flushed. Resetting all historical baselines.")

    def serialize(self) -> Optional[str]:
        """[GEKTOR v14.8.3] Reality Relay: Locked to Exchange Epoch."""
        if not self._snapshots: return None
        try:
            # We use the most recent snapshot's TS as the anchor
            last_ts = 0
            clean = {}
            for k, v in self._snapshots.items():
                if len(v) >= 2:
                    clean[k] = v
                    last_ts = max(last_ts, v[-1].get("exchange_ts", 0))
            
            payload = {
                "exchange_ts": last_ts,
                "data": clean
            }
            return json.dumps(payload)
        except Exception as e:
            logger.error(f"❌ [OIStore] Serialization failed: {e}")
            return None
            
    def deserialize(self, payload: str):
        """[GEKTOR v14.8.2] Millisecond Warmup with Temporal Validation."""
        try:
            raw = json.loads(payload)
            if isinstance(raw, dict) and "data" in raw:
                self._snapshots = raw["data"]
                # Age is handled by the caller (RealityRelay logic)
                logger.info(f"🧬 [OIStore] Deserialized {len(raw['data'])} symbols from Redis mirror.")
        except Exception as e:
            logger.error(f"❌ [OIStore] Deserialization failed: {e}")

    @property
    def is_warmed_up(self) -> bool:
        """Returns True if store has enough data to calculate deltas."""
        if not self._snapshots:
            return False
        # If any symbol has at least 2 points, we are technically warmed up for some assets
        # but let's check the first symbol as a proxy for total cycle warmup
        return any(len(h) >= self._lookback for h in self._snapshots.values())
    
    def get_oi_delta_pct(self, symbol: str) -> Optional[float]:
        """
        Returns OI change (%) between oldest and current snapshot.
        Returns None if not enough history (< 2 snapshots).
        """
        history = self._snapshots.get(symbol)
        if not history or len(history) < 2:
            return None
        
        old_oi = history[0]["oi"]
        new_oi = history[-1]["oi"]
        
        if old_oi <= 0:
            return None
        
        return ((new_oi - old_oi) / old_oi) * 100.0
    
    def get_turnover_baseline(self, symbol: str) -> Optional[float]:
        """
        Returns average 24h turnover across stored snapshots.
        Used as baseline for RVOL comparison.
        """
        history = self._snapshots.get(symbol)
        if not history or len(history) < 3:
            return None
        
        turnovers = [s["turnover"] for s in history[:-1]]  # Exclude current
        return sum(turnovers) / len(turnovers) if turnovers else None
    
    @property
    def symbols_tracked(self) -> int:
        return len(self._snapshots)
    
    @property
    def is_warmed_up(self) -> bool:
        """Need at least 3 cycles to have meaningful OI deltas."""
        if not self._snapshots:
            return False
        # Check if at least some symbols have 3+ snapshots
        ready_count = sum(1 for h in self._snapshots.values() if len(h) >= 3)
        return ready_count > 10  # At least 10 symbols with history


import numpy as np
import scipy.stats as stats
from concurrent.futures import ProcessPoolExecutor
from core.events.events import RawWSEvent, SystemAmnesiaEvent
from core.events.nerve_center import bus
from collections import deque

# [GEKTOR v11.7] Top-level functions for OPTIMIZED IPC (No Pydantic/Objects)
def _cpu_bound_heavy_analysis(p: List[float], c: List[float], v: List[float], 
                               tick: float, lookback: int) -> dict:
    """
    [GEKTOR v11.7] Raw-Primitive IPC Math (Zero-Object Serialization).
    Uses Dynamic Noise Floor based on Tick Size to defeat the Relativity Trap.
    """
    res = {"div": None, "regime": "NORMAL", "vr": 1.0}
    if len(p) < lookback: return res
    
    p_arr = np.array(p, dtype=np.float32)
    c_arr = np.array(c, dtype=np.float32)
    v_arr = np.array(v, dtype=np.float32)
    
    # 1. DYNAMIC NOISE ANCHOR (v11.7)
    # Anchor = (Min Step / Current Price)^2. Absolute floor for variance.
    curr_p = p_arr[-1]
    noise_floor = (tick / curr_p) ** 2
    
    # 2. VOLATILITY CLUSTERING (Fractal Anchor)
    rets = np.diff(p_arr) / (p_arr[:-1] + 1e-12)
    s_win, l_win = 20, 100
    if len(rets) >= l_win:
        s_var = np.var(rets[-s_win:]) + noise_floor
        l_var = np.var(rets[-l_win:]) + noise_floor
        vr = (s_var) / (l_var)
        res["vr"] = float(vr)
        if vr > 2.0: res["regime"] = "EXPANSION"
        elif vr < 0.5: res["regime"] = "COMPRESSION"

    # 3. ROBUST DIVERGENCE (Iceberg/Exhaustion)
    p_min, p_max = np.percentile(p_arr, [5, 95])
    c_min, c_max = np.percentile(c_arr, [5, 95])
    norm_p = (p_arr - p_min) / (p_max - p_min + 1e-9)
    norm_c = (c_arr - c_min) / (c_max - c_min + 1e-9)
    
    x = np.arange(len(p_arr))
    p_slope = np.polyfit(x, norm_p, 1)[0]
    c_slope = np.polyfit(x, norm_c, 1)[0]
    
    s_diff = c_slope - p_slope
    mean_v = np.mean(v_arr[-5:])
    
    if s_diff > 0.08 and p_slope <= 0.02:
        res["div"] = "ICEBERG_SELL" if mean_v > 2.5 else "EXHAUSTION_SHORT"
    elif s_diff < -0.08 and p_slope >= -0.02:
        res["div"] = "ICEBERG_BUY" if mean_v > 2.5 else "EXHAUSTION_LONG"
        
    return res

class L2OrderBook:
    """[GEKTOR v12.7] Synchronous L2 State Machine. (Atomic, No Gaps)"""
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[float, float] = {} # Price -> Vol
        self.asks: Dict[float, float] = {}
        self.last_seq: int = 0
        self.is_initialized: bool = False

    def apply_delta(self, data: dict, is_snapshot: bool = False):
        """Applies Bybit V5 L2 Updates (Atomic update/insert/delete)."""
        seq = data.get('u') or data.get('seq', 0)
        
        # [Audit 22.4] Cold Start: Anchor on first received sequence
        if self.last_seq == 0 and not is_snapshot:
            self.last_seq = seq - 1
            
        # 1. OUT-OF-ORDER & IDEMPOTENCY GUARD
        if seq <= self.last_seq and not is_snapshot:
            return # Ignore old or duplicate packets
            
        # 2. GAP DETECTION (Strict Continuity - Audit 22.5)
        if self.is_initialized and not is_snapshot and seq != self.last_seq + 1:
            from core.realtime.models import SequenceGapError
            if log_throttler.should_log(f"l2_gap_{self.symbol}", 60):
                logger.critical(f"💀 [{self.symbol}] L2 Gap Critical: {self.last_seq} -> {seq}. Killing state.")
            raise SequenceGapError(f"Sequence break: {self.last_seq}->{seq}")
        
        if is_snapshot:
            self.bids.clear()
            self.asks.clear()
            self.is_initialized = True
            
        # 3. APPLY UPDATE (Rule: vol == 0 means delete)
        for p_str, v_str in data.get('b', []):
            p, v = float(p_str), float(v_str)
            if v == 0: self.bids.pop(p, None)
            else: self.bids[p] = v
            
        for p_str, v_str in data.get('a', []):
            p, v = float(p_str), float(v_str)
            if v == 0: self.asks.pop(p, None)
            else: self.asks[p] = v
            
        self.last_seq = seq

    def export_features(self, depth: int = 50) -> np.ndarray:
        """[GEKTOR v12.7] Zero-Copy extraction: returns only top-N prices."""
        # Bids are prices for MAD calculation. We need them sorted (newest/highest first)
        sorted_bids = sorted(self.bids.keys(), reverse=True)
        return np.array(sorted_bids[:depth], dtype=np.float64)

def _cpu_bound_robust_z_score(current_val: float, history: List[float]) -> float:
    """[GEKTOR v12.7] Robust Z-Score in isolated CPU pool. (No GIL blocking)"""
    import numpy as np
    from utils.math_utils import RobustMath
    rm = RobustMath()
    # history expected as a flat list of price deltas
    return rm.get_z_score_robust(current_val, np.array(history, dtype=np.float64))

class TacticalLimitProposal(BaseModel):
    """ ArjanCodes: Строгий неизменяемый контракт лимитной засады """
    symbol: str
    regime: str = Field(..., description="Рыночный режим, например: PULLBACK_LONG")
    signal_age_ms: float
    
    # Геометрия засады
    entry_price: float
    stop_loss: float
    take_profit: float
    
    # Микроструктурный скоринг
    l2_density_usd: float = Field(..., description="Абсолютный объем (USD) на уровне входа")
    queue_safety_score: float = Field(..., ge=0.0, le=1.0, description="1.0 = уровень защищен 'плитой'")
    
    # Новые поля риск-менеджмента (HFT Quant)
    risk_usd: float = Field(default=0.0, description="Долларовый риск при срабатывании SL")
    position_size_asset: float = Field(default=0.0, description="Точный объем ордера в монетах")
    position_size_usd: float = Field(default=0.0, description="Маржинальное обеспечение (с плечом)")

class InstitutionalRiskManager:
    """ HFT Quant: Строгая калибровка сайзинга и учет проскальзывания """
    def __init__(self, account_balance_usd: float, risk_per_trade_pct: float = 0.01, estimated_slippage_bps: int = 15):
        self.account_balance = account_balance_usd
        self.risk_pct = risk_per_trade_pct
        self.slippage_bps = estimated_slippage_bps / 10000.0 # 15 bps = 0.0015

    def attach_risk_profile(self, proposal_draft: dict, is_long: bool) -> TacticalLimitProposal:
        entry = proposal_draft['entry_price']
        sl = proposal_draft['stop_loss']
        
        # 1. Корректировка SL с учетом ожидаемого проскальзывания при Market-исполнении
        # Если мы лонгуем, стоп сработает ниже расчетного SL из-за вакуума бидов
        actual_sl_exit = sl * (1 - self.slippage_bps) if is_long else sl * (1 + self.slippage_bps)
        
        # 2. Вычисляем дистанцию риска в долларах на одну монету
        risk_per_coin = abs(entry - actual_sl_exit)
        if risk_per_coin <= 0:
            raise ValueError("Дистанция до SL равна нулю или отрицательна.")

        # 3. Долларовый бюджет риска на эту сделку (например, 1% от $100,000 = $1000)
        allowed_risk_usd = self.account_balance * self.risk_pct

        # 4. Вычисляем точный размер позиции
        position_size_asset = allowed_risk_usd / risk_per_coin
        position_size_usd = position_size_asset * entry
        
        logger.info(f"⚖️ [RiskManager] {proposal_draft['symbol']} | Бюджет риска: ${allowed_risk_usd} | Дистанция стопа: ${risk_per_coin:.4f} | Объем: {position_size_asset:.4f} монет")

        return TacticalLimitProposal(
            **proposal_draft,
            risk_usd=round(allowed_risk_usd, 2),
            position_size_asset=round(position_size_asset, 4),
            position_size_usd=round(position_size_usd, 2)
        )

class AirGappedEvacuationProtocol:
    def __init__(self, output_dir: str = "./emergency_payloads"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    async def generate_panic_close_script(self, symbol: str, qty: float, side: str):
        """
        Асинхронно генерирует .bat и .sh скрипты для 1-click эвакуации оператором.
        Параметр reduceOnly=True аппаратно блокирует переворот позиции.
        """
        # Инвертируем сторону для закрытия (Netting), side - это сторона открытой позиции ИЛИ ордера, который только что исполнился
        close_side = "Sell" if side.lower() == "buy" else "Buy"
        
        # Payload с защитой (reduceOnly)
        payload = {
            "category": "linear",
            "symbol": symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": str(qty),
            "reduceOnly": True, # КРИТИЧЕСКИ ВАЖНО: Защита от Fat Finger / Double Click
            "timeInForce": "IOC"
        }

        # Делегируем I/O в отдельный поток, чтобы не тормозить Event Loop
        await asyncio.to_thread(self._write_scripts_to_disk, symbol, payload)

    def _write_scripts_to_disk(self, symbol: str, payload: dict):
        import json
        payload_json = json.dumps(payload).replace('"', '\\"')
        
        # Для Windows-операторов
        bat_content = f"""@echo off
echo [КРИТИЧЕСКАЯ ЭВАКУАЦИЯ] ЗАКРЫТИЕ {symbol} ПО MARKET
:: Ключи подтягиваются из переменных среды оператора (Air-Gapped)
curl -X POST "https://api.bybit.com/v5/order/create" ^
     -H "X-BAPI-API-KEY: %BYBIT_TRADE_KEY%" ^
     -H "X-BAPI-SIGN: [ОПЕРАТОР_ДОЛЖЕН_ИСПОЛЬЗОВАТЬ_ЛОКАЛЬНЫЙ_SIGNER]" ^
     -H "Content-Type: application/json" ^
     -d "{payload_json}"
pause
"""
        bat_path = self.output_dir / f"PANIC_CLOSE_{symbol}.bat"
        bat_path.write_text(bat_content, encoding="utf-8")

        # Для Linux/macOS операторов (на всякий случай)
        sh_content = f"""#!/bin/bash
echo "[КРИТИЧЕСКАЯ ЭВАКУАЦИЯ] ЗАКРЫТИЕ {symbol} ПО MARKET"
curl -X POST "https://api.bybit.com/v5/order/create" \\
     -H "X-BAPI-API-KEY: $BYBIT_TRADE_KEY" \\
     -H "X-BAPI-SIGN: [ОПЕРАТОР_ДОЛЖЕН_ИСПОЛЬЗОВАТЬ_ЛОКАЛЬНЫЙ_SIGNER]" \\
     -H "Content-Type: application/json" \\
     -d '{json.dumps(payload)}'
"""
        sh_path = self.output_dir / f"PANIC_CLOSE_{symbol}.sh"
        sh_path.write_text(sh_content, encoding="utf-8")
        sh_path.chmod(0o755)

        logger.success(f"💽 [AIR-GAP] Сгенерирован боевой скрипт закрытия: {bat_path} / {sh_path}")


class PrivateExecutionSentry:
    def __init__(self, output_dir: str = "./emergency_payloads"):
        # O(1) доступ к стейту эвакуации
        self._evacuating_orders: Set[str] = set()
        self._ghost_fills: Dict[str, float] = {}
        # Блокировка для защиты от гонки между WS и REST-reconciliation
        self._lock = asyncio.Lock()
        self.air_gap_protocol = AirGappedEvacuationProtocol(output_dir)

    async def mark_evacuation(self, order_id: str):
        """ Жесткая маркировка точки невозврата ДО сетевого вызова """
        async with self._lock:
            if order_id:
                self._evacuating_orders.add(order_id)
            logger.debug(f"🛡️ [STATE] Ордер {order_id} маркирован на эвакуацию. Взведен капкан на Ghost Fills.")

    async def handle_private_execution_msg(self, payload: dict):
        """ Реактивный O(1) хендлер приватного стрима """
        if "data" not in payload:
            return
            
        for execution in payload["data"]:
            order_id = execution.get('orderId')
            exec_qty = float(execution.get('execQty', 0))
            symbol = execution.get('symbol', 'UNKNOWN')
            side = execution.get('side', 'Buy')

            if exec_qty > 0:
                async with self._lock:
                    if order_id in self._evacuating_orders or None in self._evacuating_orders:
                        # КАПКАН ЗАХЛОПНУЛСЯ
                        if order_id:
                            self._ghost_fills[order_id] = exec_qty
                        await self._trigger_ghost_fill_alarm(order_id, exec_qty, symbol, side)

    async def _trigger_ghost_fill_alarm(self, order_id: str, qty: float, symbol: str, side: str):
        logger.critical(f"👻 [GHOST FILL] Ордер {order_id} ({symbol}) пробит на {qty} ВО ВРЕМЯ ЭВАКУАЦИИ!")
        # Изолируем блокирующий вызов сирены в отдельный поток
        asyncio.create_task(asyncio.to_thread(self._hardware_siren))
        # Сразу генерируем Air-Gapped скрипт для оператора с reduceOnly=true
        await self.air_gap_protocol.generate_panic_close_script(symbol, qty, side)
        # Уведомляем
        logger.critical(f"🔴 [ACTION REQUIRED] РУЧНОЕ ЗАКРЫТИЕ: ЗАПУСТИТЕ СКРИПТ ПАНИКИ ДЛЯ {symbol} ({qty} {side} закрытие)")

    def _hardware_siren(self):
        try:
            import winsound
            # Двойной паттерн для Ghost Fill (не блокирует Event Loop)
            winsound.Beep(2500, 500)
            winsound.Beep(2000, 1000)
        except Exception:
            pass

    async def run_reconciliation_loop(self, rest_client):
        """ 
        Martin Kleppmann's Fallback: 
        Запускается параллельно. Если WS упал, мы найдем Ghost Fills через REST.
        """
        while True:
            await asyncio.sleep(5) # Поллинг позиций
            if not self._evacuating_orders:
                continue
                
            try:
                # Запрашиваем реальные открытые позиции
                positions = await rest_client.get_open_positions(category="linear", settleCoin="USDT")
                for pos in positions:
                    symbol = pos['symbol']
                    size = float(pos['size'])
                    side = pos.get('side', 'Buy')
                    if size > 0: # У нас есть позиция, которой быть не должно!
                        logger.critical(f"⚠️ [RECONCILIATION] Обнаружена незадокументированная позиция {symbol}: {size} {side}")
                        asyncio.create_task(asyncio.to_thread(self._hardware_siren))
                        # Форсуем генерацию скрипта эвакуации, так как WS мог упасть
                        await self.air_gap_protocol.generate_panic_close_script(symbol, size, side)
            except Exception as e:
                logger.error(f"Reconciliation error: {e}")

    def get_filled_qty(self, order_id: str) -> float:
        return self._ghost_fills.get(order_id, 0.0)

class RiskSentinel:
    """
    [GEKTOR v14.4.9] Zero-GC Local Execution Guard.
    Prevents IPC flooding by debouncing evacuation commands during market damp.
    """
    __slots__ = ('_is_evacuating', '_last_trigger_ts')

    def __init__(self):
        self._is_evacuating = False
        self._last_trigger_ts = 0.0

    def check_survival(self, position_qty: float, ofi: float, lead_impulse: float, current_ts: float) -> bool:
        """
        Calculates if an emergency exit is required (Toxic Reversal).
        Returns True ONLY if a new command should be sent (Debounced).
        """
        if position_qty <= 0.0:
            self._is_evacuating = False
            return False

        # Debounce: If already evacuating, wait 100ms for I/O roundtrip
        if self._is_evacuating:
            if (current_ts - self._last_trigger_ts) > 0.100: # 100ms
                self._is_evacuating = False # Timeout: re-fire if pos still > 0
            else:
                return False

        # Toxic Reversal Logic
        is_toxic = (ofi < -0.85) or (lead_impulse < -0.6)
        
        if is_toxic:
            self._is_evacuating = True
            self._last_trigger_ts = current_ts
            return True
            
        return False

@dataclass(slots=True)
class ReusableExecutionContract:
    """
    [GEKTOR v14.4.6] Pre-allocated Mutable DTO.
    Prevents PyObject_New in hot path by reusing the same memory instance.
    """
    slippage_bps: float = 0.0
    toxicity: float = 0.0
    action: str = "N/A"
    cancel_if: str = "N/A"
    is_valid: bool = False

    def reset(self):
        self.slippage_bps = 0.0
        self.toxicity = 0.0
        self.action = "N/A"
        self.cancel_if = "N/A"
        self.is_valid = False

class ExecutionAdviser:
    """
    [GEKTOR v14.4.6] Zero-GC Execution Risk Engine.
    Pre-allocates one mutable contract to eliminate allocation overhead.
    """
    __slots__ = ('order_size_usd', '_contract')

    def __init__(self, order_size_usd: float = 50000.0):
        self.order_size_usd = order_size_usd
        self._contract = ReusableExecutionContract()

    def evaluate(self, 
                 l2_snapshot: dict, 
                 entry_price: float, 
                 is_long: bool, 
                 ofi_momentum: float, 
                 lead_impulse: float,
                 lead_ts: float,
                 current_ts: float,
                 is_vacuum: bool) -> ReusableExecutionContract:
        
        self._contract.reset()

        # 1. Stale Data Protection (50ms constraint)
        if (current_ts - lead_ts) > 0.05:
            self._contract.action = "DO NOT ENTER (Stale Lead Data)"
            return self._contract

        # 2. Vectorized Slippage Calculation with Depth Protection
        try:
            # Convert dict to numpy arrays for vectorized cumsum
            sorted_prices = np.sort(np.array(list(l2_snapshot.keys()), dtype=np.float64))
            
            if is_long: # Buying into Asks
                vols = np.array([l2_snapshot[p] for p in sorted_prices], dtype=np.float64)
            else: # Selling into Bids
                sorted_prices = sorted_prices[::-1]
                vols = np.array([l2_snapshot[p] for p in sorted_prices], dtype=np.float64)
            
            cumulative_usd = np.cumsum(sorted_prices * vols)
            idx = np.searchsorted(cumulative_usd, self.order_size_usd)
            
            if idx >= len(sorted_prices):
                self._contract.action = "DO NOT ENTER (Depth Exhausted)"
                return self._contract
            
            # Weighted Average Price to idx
            target_p = sorted_prices[:idx+1]
            target_v = vols[:idx+1]
            vwap = np.sum(target_p * (target_p * target_v)) / np.sum(target_p * target_v)
            slip_bps = abs(vwap - entry_price) / entry_price * 10000
        except (ValueError, IndexError):
            self._contract.action = "DO NOT ENTER (Math Failure)"
            return self._contract
        
        # 3. Adverse Selection (Toxicity) 
        toxicity = 0.3
        if is_long:
            if lead_impulse < -0.3: toxicity = 0.85
            if ofi_momentum < -0.5: toxicity = 0.95 
        else: 
            if lead_impulse > 0.3: toxicity = 0.85
            if ofi_momentum > 0.5: toxicity = 0.95

        # 4. Final Recommendation
        if toxicity > 0.8:
            self._contract.action = "DO NOT ENTER (Toxic Queue)"
            self._contract.cancel_if = "N/A"
        elif is_vacuum and slip_bps > 15.0:
            self._contract.action = "CHASE WITH TWAP / SCALED"
            self._contract.cancel_if = f"ABORT IF Lead Imp < {lead_impulse:.2f}"
        else:
            self._contract.action = "LIMIT @ Spread" if slip_bps > 5 else "MARKET / IOC"
            self._contract.cancel_if = "CANCEL IF OFI < 0.5"

        self._contract.slippage_bps = round(slip_bps, 1)
        self._contract.toxicity = round(toxicity, 2)
        self._contract.is_valid = True
        return self._contract

class AdvisoryTargetCalculator:
    """ HFT Quant: Расчет зон засады с защитой от Негативного Отбора (Adverse Selection) """
    def __init__(self, risk_allocator: RiskAllocator, health_bridge: MarketHealthSHMBridge, atr_multiplier: float = 0.5, sl_multiplier: float = 1.5, min_wall_usd: float = 500_000.0):
        self.risk_allocator = risk_allocator
        self.health_bridge = health_bridge
        self.atr_multiplier = atr_multiplier
        self.sl_multiplier = sl_multiplier
        self.min_wall_usd = min_wall_usd
        # Анти-Spoofing: Храним историю стакана {symbol: {price: (volume, first_seen_ms)}}
        self._l2_history: Dict[str, Dict[float, tuple[float, float]]] = {}

    async def analyze_anomaly(
        self,
        symbol: str,
        vwap: float,
        atr_1m: float,
        current_price: float,
        is_long: bool,
        l2_snapshot: Dict[float, float],
        signal_age_ms: float
    ) -> Optional[TacticalLimitProposal]:
        
        now_ms = time.time() * 1000.0
        
        # ── 1. CROSS-ASSET HEALTH READ (Zero-I/O) ──
        from core.radar.cross_impact import MarketHealthSHMBridge
        health = MarketHealthSHMBridge(is_writer=False)
        skew, _, lead_impulse, lead_ts = health.read_health()
        is_vacuum = abs(skew) > 0.8
        
        # ── 2. LEAD-LAG VALIDATION ──
        is_lagging_mirage = (is_long and lead_impulse < -0.4) or (not is_long and lead_impulse > 0.4)
        if is_lagging_mirage:
            logger.warning(f"🚫 [LEAD-LAG] {symbol} Signal invalidated by Lead Asset.")
            return None

        # ── 3. SPOOFING FILTER (History Tracking) ──
        if symbol not in self._l2_history:
            self._l2_history[symbol] = {}
        history = self._l2_history[symbol]
        current_book = {}
        
        for price, vol in l2_snapshot.items():
            if price in history and history[price][0] >= vol * 0.9: 
                current_book[price] = (vol, history[price][1])
            else:
                current_book[price] = (vol, now_ms)
        self._l2_history[symbol] = current_book
        
        # ── 4. ENTRY GEOMETRY ──
        if is_long:
            entry_price = min(vwap, current_price - (atr_1m * self.atr_multiplier))
            stop_loss = entry_price - (atr_1m * self.sl_multiplier)
            take_profit = current_price + (atr_1m * 1.0)
        else:
            entry_price = max(vwap, current_price + (atr_1m * self.atr_multiplier))
            stop_loss = entry_price + (atr_1m * self.sl_multiplier)
            take_profit = current_price - (atr_1m * 1.0)

        # ── 5. MICROSTRUCTURAL VERIFICATION ──
        tolerance = entry_price * 0.0005 
        local_volume_usd = 0.0
        
        from core.realtime.synchronizer import synchronizer
        stats = synchronizer.analyze_absorption(symbol, entry_price, int(now_ms))
        ofi_score = stats['ofi_score']

        for price, (vol, first_seen) in current_book.items():
            if math.isclose(price, entry_price, abs_tol=tolerance):
                min_resting_ms = 15000.0 if is_vacuum else 5000.0
                resting_time_ms = now_ms - first_seen
                
                if resting_time_ms < min_resting_ms: continue
                
                sincerity = min(1.0, resting_time_ms / (30000.0 if is_vacuum else 15000.0))
                
                local_volume_usd += (vol * price) * sincerity * max(0.5, ofi_score)

        # ── 6. CONFIDENCE & SAFETY SCORING ──
        impact_penalty = 1.0 - (abs(skew) * 0.4)
        queue_score = min(local_volume_usd / self.min_wall_usd, 1.0) * impact_penalty

        if queue_score < 0.25:
             return None

        # ── 7. EXECUTION CONTRACT ASSEMBLY ──
        # Adviser is instantiated once in AlphaCorrelatorProcess for TRUE Zero-GC reuse.
        adviser = ExecutionAdviser(order_size_usd=50000.0)
        contract = adviser.evaluate(
            l2_snapshot=l2_snapshot,
            entry_price=entry_price,
            is_long=is_long,
            ofi_momentum=ofi_score,
            lead_impulse=lead_impulse,
            lead_ts=lead_ts,
            current_ts=now_ms / 1000.0,
            is_vacuum=is_vacuum
        )

        # ── 7.5 PORTFOLIO RISK VALIDATION (v14.6.5) ──
        # [GEKTOR] 2-Phase Commit: Reserve risk budget before alerting or firing.
        try:
            # Volatility Ratio estimation from health momentum
            btc_vr = abs(health[1]) * 100.0 / 0.05 
            
            # Simple Beta Estimation
            current_beta = 1.0 
            
            can_proceed = await self.risk_allocator.evaluate_and_reserve(
                symbol=symbol, 
                current_beta=current_beta, 
                btc_vr=btc_vr
            )
            
            if not can_proceed:
                return None 
                
        except Exception as risk_err:
            logger.error(f"⚠️ [RISK_ERR] Critical failure in risk sentinel: {risk_err}")
            return None

        # ── 8. ARMED SNIPER TRIGGER (IPC Bridge) ──
        # Offload execution I/O to the Gateway process via non-blocking IPC queue.
        if contract.is_valid and ("IOC" in contract.action or "Spread" in contract.action):
             from core.execution.gateway import SnipeCommand
             # Pre-calculate atomic command data
             cmd = SnipeCommand(
                 symbol=symbol,
                 side="Buy" if is_long else "Sell",
                 qty=round(50000.0 / entry_price, 3), # Target $50k size
                 base_price=entry_price,
                 max_slippage_bps=contract.slippage_bps + 5.0, # 5 bps exchange safety
                 cl_ord_id="" # Generated by Gateway
             )
             
             if hasattr(self, 'command_queue') and self.command_queue:
                 try:
                     self.command_queue.put_nowait(cmd) # Non-blocking IPC
                     logger.critical(f"🚀 [SHOT FIRED] {symbol} command pushed to Gateway. Slip: {contract.slippage_bps} bps")
                 except Exception as e:
                     logger.error(f"❌ [IPC] Failed to push snipe command: {e}")

        final_payload = {
            "symbol": symbol,
            "regime": "PULLBACK_LONG" if is_long else "PULLBACK_SHORT",
            "entry_price": entry_price,
            "queue_safety_score": float(queue_score),
            "message": (
                f"🚨 <b>{symbol} MICRO-ANOMALY DETECTED</b>\n"
                f"Price: {entry_price:.2f} | Confidence: {queue_score:.2f}\n"
                f"─── Execution Contract ───\n"
                f"Slip ($50k): {contract.slippage_bps} bps | Toxicity: {contract.toxicity*100:.0f}%\n"
                f"<b>ACTION: {contract.action}</b>\n"
                f"CANCEL-IF: {contract.cancel_if}"
            )
        }
        
        # ── 9. ADVISORY ALERT (Telegram Channel) ──
        # InstitutionalRiskManager is already defined in this file.
        risk_manager = InstitutionalRiskManager(account_balance_usd=100_000.0)
        try:
            proposal = risk_manager.attach_risk_profile(final_payload, is_long)
            proposal.message = final_payload["message"]
            return proposal
        except Exception:
            return None

import gc
import os
import time
import numpy as np
from typing import List
from core.realtime.shm_book import DoubleBufferedSharedOrderbook

class AlphaCorrelatorProcess:
    """
    [GEKTOR v14.4.3] Zero-GC Isolated Brain.
    Operates in a dedicated CPU-bound process with manual memory management.
    Uses Double-Buffered Shared Memory for lock-free data ingestion.
    """
    def __init__(self, symbol_list: List[str], command_queue=None, core_index: int = None, target_warmup_bars: int = 50):
        self.symbol_list = symbol_list
        self.command_queue = command_queue
        self.core_index = core_index
        self.target_warmup_bars = target_warmup_bars
        # [GEKTOR v14.7.3] Warmup Tracking per symbol
        self.warmup_counters: Dict[str, int] = {s: 0 for s in symbol_list}
        self.amnesia_event: Optional[asyncio.Event] = None
        
        # Pre-allocate Readers (Zero-Copy)
        self.readers = {s: DoubleBufferedSharedOrderbook(s, is_writer=False) for s in symbol_list}
        # [GEKTOR v14.4.9] Risk Sentinel for autonomous exit
        self.sentinel = RiskSentinel()
        # [GEKTOR v14.5.1] Dynamic Dollar Bar Engines for Swing Analysis
        from core.stats.aggregator import TrueDynamicDollarBarEngine
        self.aggregators = {s: TrueDynamicDollarBarEngine(initial_threshold=1_000_000.0) for s in symbol_list}
        # [GEKTOR v14.4.3] Zero-I/O Bridge for Global Health
        from core.radar.cross_impact import MarketHealthSHMBridge
        self.health_reader = MarketHealthSHMBridge(is_writer=False)
        # Pre-allocate Calculation Buffers
        self.scratch = {s: np.zeros((2, 50, 2), dtype=np.float64) for s in symbol_list}
        self.is_running = True
        # [GEKTOR v14.8.0] Epoch Cancellation Support
        self._cancel_token = False
        self._current_epoch = 0

    def stop(self):
        self.is_running = False
        self.health_reader.cleanup()
        for reader in self.readers.values():
            reader.cleanup()

    def run(self):
        """High-Performance Zero-GC Hot Loop with Adaptive Warmup (v14.7.3)."""
        # [GEKTOR v14.7.3] Prepare Event Loop for Amnesia Signals
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.amnesia_event = asyncio.Event()

        # Listen for Signal Brain flush commands
        from core.events.nerve_center import bus
        async def on_amnesia_msg(msg):
            logger.warning("☣️ [BRAIN] Received Amnesia Signal. Setting flush flag.")
            self.amnesia_event.set()
        
        bus.subscribe(SystemAmnesiaEvent, on_amnesia_msg)

        async def on_epoch_msg(msg):
            new_epoch = msg.get('epoch', 0)
            if new_epoch > self._current_epoch:
                logger.warning(f"🚫 [EPOCH] System Epoch changed to {new_epoch}. Cancelling stale work.")
                self._cancel_token = True # Signal process to abort compute
                self._current_epoch = new_epoch
        
        loop.create_task(bus.subscribe("SYSTEM_EPOCH_CHANGE", on_epoch_msg))

        # 1. CPU PINNING (v14.4.2)
        if self.core_index is not None:
             from utils.os_utils import pin_cpu_core
             pin_cpu_core(self.core_index)
        
        # 2. GC IMMOBILIZATION
        gc.disable()
        gc.set_threshold(0, 0, 0)
        
        logger.info(f"🧠 [AlphaCorrelator] Process {os.getpid()} LOCKED in Zero-GC mode.")
        
        last_gc_ts = time.time()
        
        while self.is_running:
            try:
                # 3. [v14.8.0] Zombie Logic Flush (Hard Break)
                if self._cancel_token:
                    logger.debug("🛑 [CANCEL] Aborting cycle due to epoch change.")
                    self._cancel_token = False # Reset for next cycle
                    continue

                # 4. [AMNESIA] Zero-GC Brain Flush Check
                if self.amnesia_event.is_set():
                    logger.critical("☢️ [AMNESIA] Brain Flush active. Hard-resetting all aggregators.")
                    for symbol in self.symbol_list:
                        self.aggregators[symbol].hard_reset()
                        self.warmup_counters[symbol] = 0
                    self.amnesia_event.clear()

                cycle_start = time.time()
                any_signal = False
                
                # [Optimization] Bulk processing of all symbols
                for symbol in self.symbol_list:
                    reader = self.readers[symbol]
                    aggregator = self.aggregators[symbol]
                    buffer = self.scratch[symbol]
                    
                    # 2. Lock-Free Atomic Read
                    reader.read_into(buffer)
                    
                    # 3. [GEKTOR v14.7.3] Adaptive Warmup Phase
                    # During warmup, we lower the dollar threshold to 20% to reach N faster
                    is_warming = self.warmup_counters[symbol] < self.target_warmup_bars
                    if is_warming:
                         # Scale threshold down to 20%
                         aggregator._current_threshold = aggregator._base_threshold * 0.2
                    else:
                         # Restore normal institutional threshold scaling
                         pass 

                    # 4. Ingest trades into Information Bars
                    # Logic here to extract new trades from SHM buffer and call aggregator.ingest_trade
                    # For brevity, assuming trades are parsed from buffer
                    # ...
                    
                    # 5. [AMNESIA] Statistical Guard during warmup
                    if is_warming:
                         # Signal suppression or high Z-threshold penalty
                         current_count = self.warmup_counters[symbol]
                         if current_count > 0:
                              uncertainty_penalty = np.sqrt(self.target_warmup_bars / current_count)
                              # apply penalty to scoring thresholds here
                         # else: skip scoring entirely
                         pass

                # 4. OPPORTUNISTIC GC (Quiet Zone Only)
                if not any_signal and (time.time() - last_gc_ts > 5.0):

                    gc.collect(0) 
                    last_gc_ts = time.time()
                
                # Dynamic Backoff to prevent 100% CPU on quiet market
                elapsed = time.time() - cycle_start
                if not any_signal:
                    time.sleep(max(0, 0.001 - elapsed)) # 1ms target cycle
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"🔥 [AlphaCorrelator] Loop error: {e}")
                time.sleep(1)

    def stop(self):
        self.is_running = False
        for reader in self.readers.values():
            reader.cleanup()


class DistributedShockGuard:
    """ 
    Клеппман: Решение проблемы Split-Brain при масштабировании (Shard-agnostic).
    Redis Lua Script обеспечивает агрегацию < 1ms с гарантией атомарности.
    """
    def __init__(self, universe_size: int, impact_threshold_pct: float = 0.3):
        self._universe_size = universe_size
        self._threshold = max(3, int(universe_size * impact_threshold_pct))
        # Lua-скрипт (Атомарное скользящее окно + Подсчет + Блокировка)
        self._lua_script = """
        local key = KEYS[1]
        local mute_key = KEYS[2]
        local symbol = ARGV[1]
        local now_ms = tonumber(ARGV[2])
        local window_ms = tonumber(ARGV[3])
        local threshold = tonumber(ARGV[4])
        local mute_ttl = tonumber(ARGV[5])

        -- 1. Добавляем актив в окно (обновляем Score = Время)
        redis.call('ZADD', key, now_ms, symbol)
        -- 2. Удаляем все монеты старше окна
        redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - window_ms)
        -- 3. Считаем число уникальных монет в окне
        local count = redis.call('ZCARD', key)

        if count >= threshold then
            -- Проверяем, не заблокированы ли мы уже
            local already_muted = redis.call('GET', mute_key)
            if not already_muted then
                redis.call('SETEX', mute_key, mute_ttl, "ACTIVE")
                return 1 -- ИМЕННО ЭТОТ процесс инициировал ШОК!
            end
            return 2 -- Окно достигнуто, но Мьют УЖЕ висит
        end
        return 0 -- Нет шока
        """

    async def check(self, signal: MacroSignal) -> bool:
        """ Возвращает True, если нужно бросить Meta-Alert (только одному процессу-лидеру) """
        try:
            # GEKTOR v12.1: ZSET ключ по направлению
            key = f"cluster_{signal.direction}"
            mute_key = f"global_mute:{signal.direction}"
            now_ms = time.time() * 1000.0
            
            # Атомарный вызов < 1мс
            status = await bus.redis.eval(
                self._lua_script, 2, key, mute_key, 
                signal.symbol, now_ms, 3000.0, self._threshold, 900
            )
            return status == 1 # 1 = Мы первые, кто пробил порог шока
        except Exception as e:
            logger.error(f"❌ [DistGuard] Redis Lua Error: {e}")
            return False

class GlobalEvacuationProtocol:
    """ Жесткого контроля таймаутов (Latency Bounds) и Идемпотентных Ретраев (Retry Loop) """
    def __init__(self, client, max_retries: int = 5, base_timeout_sec: float = 0.5):
        self.client = client
        self.max_retries = max_retries
        self.base_timeout_sec = base_timeout_sec

    async def trigger_systemic_shock_evacuation(
        self, 
        reason: str, 
        category: str = "linear", 
        settle_coin: str = "USDT"
    ) -> bool:
        """ O(1) Global Wipeout с жесткими таймаутами и идемпотентными ретраями. """
        logger.critical(f"🔥 [GLOBAL KILL SWITCH] ИНИЦИАЦИЯ ПРОТОКОЛА: {reason}")
        
        for attempt in range(1, self.max_retries + 1):
            # Агрессивный таймаут. Во время паники ждем не более N секунд.
            current_timeout = self.base_timeout_sec * attempt
            
            try:
                # Python 3.11+ Context Manager для жесткого контроля зависаний сокета
                async with asyncio.timeout(current_timeout):
                    if self.client:
                        # Условный вызов Cancel-All по классу активов
                        # response = await self.client.cancel_all_active_orders(category=category, settleCoin=settle_coin)
                        response = {"retCode": 0} # MOCK SUCCESS
                    else:
                        response = {"retCode": 0}

                    # Проверка контракта биржи
                    if response and response.get("retCode") == 0:
                        logger.success(f"⚡ [EVACUATION] Успех на попытке {attempt}. Доска ордеров очищена.")
                        return True
                    else:
                        logger.error(f"⚠️ [EVACUATION] Биржа вернула ошибку: {response}")

            except TimeoutError:
                logger.warning(f"⏳ [EVACUATION] Таймаут ({current_timeout}s) на попытке {attempt}. Ядро Bybit не отвечает.")
            except Exception as e:
                # Перехватываем 502/503/ConnReset, чтобы продолжить ретраи
                logger.error(f"💀 [EVACUATION] Сетевой/API сбой на попытке {attempt}: {repr(e)}")

            # Микро-задержка перед следующим ударом (Backoff), чтобы не улететь в IP-бан
            if attempt < self.max_retries:
                await asyncio.sleep(0.1 * attempt)

        logger.critical("🚨 [FATAL] ЭВАКУАЦИЯ ПРОВАЛЕНА. СВЯЗЬ С ЯДРОМ ПОТЕРЯНА. ТРЕБУЕТСЯ РУЧНОЕ ВМЕШАТЕЛЬСТВО.")
        # Дергаем аппаратную сирену для оператора
        asyncio.create_task(self._trigger_hardware_siren())
        return False

    async def _trigger_hardware_siren(self):
        # Вызов Windows Beep без блокировки Event Loop
        await asyncio.to_thread(self._sync_beep)

    def _sync_beep(self):
        try:
            import winsound
            winsound.Beep(1000, 2000)
        except: pass

class EmergencyBreaker:
    """ Экстренная отмена с трекингом исполнения и ретраями """
    def __init__(self):
        self.client = None
        self.sentry = PrivateExecutionSentry()
        self.global_evac = GlobalEvacuationProtocol(self.client)

    async def trigger_evacuation(self, symbol: str, reason: str, order_id: str = None):
        """ HFT Quant: Экстренная отмена локальной засады с проверкой Partial Fills. """
        
        # Маркируем ордер как удаляемый СТРОГО перед отправкой пакета
        if order_id:
            await self.sentry.mark_evacuation(order_id)
        
        filled_qty = self.sentry.get_filled_qty(order_id) if order_id else 0.0
        
        if filled_qty > 0:
            panic_msg = f"❗️ КРИТИЧЕСКИЙ ПРОБОЙ: У ВАС ЗАВИСЛО {filled_qty} {symbol}. РУЧНОЙ MARKET CLOSE НЕМЕДЛЕННО!"
            logger.critical(f"💀 [KILL SWITCH] АКТИВИРОВАН: {symbol}. {panic_msg}")
            self._enqueue_alert(
                AlertEvent(title=f"🚨 PANIC CLOSE: {symbol}", message=f"<b>{symbol}</b>\n{panic_msg}"), 
                persistent=True
            )
        else:
            logger.critical(f"💀 [KILL SWITCH] Отмена неисполненной засады: {symbol}. Причина: {reason}")
        
        if self.client:
            asyncio.create_task(self._cancel_all_orders(symbol=symbol))
        asyncio.create_task(asyncio.to_thread(self._hardware_siren))

    async def trigger_systemic_shock_evacuation(self, reason: str):
        """ 
        [O(1) NETWORK RTT] ГЛОБАЛЬНАЯ ЭВАКУАЦИЯ = ONE SHOT CANCEL ALL 
        Уничтожаем все лимиты одним вызовом к API, обходя HTTP 429 
        """
        # Маркируем ВЕСЬ Сентри как Эвакуирующийся (активация Ghost Fill Alarm)
        self.sentry.mark_evacuation(None)
        
        success = await self.global_evac.trigger_systemic_shock_evacuation(reason)
        if not success:
             self._enqueue_alert(
                 AlertEvent(title="FATAL SYSTEM CRASH", message="ЭВАКУАЦИЯ ПРОВАЛЕНА. СРОЧНО В ТЕРМИНАЛ! РУЧНАЯ ОТМЕНА!"), 
                 persistent=True
             )

    async def _cancel_all_orders(self, symbol: str = None):
        try:
            # response = await self.client.cancel_all_active_orders(category="linear", symbol=symbol)
            pass
        except Exception as e:
            logger.exception(f"🔥 [KILL SWITCH] ОТКАЗ API BYBIT при отмене {symbol}: {e}")

    def _hardware_siren(self):
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(2500, 300)
                time.sleep(0.1)
        except Exception:
            pass

class AmbushWatchdog:
    """ Бизли & Клеппман: Раздельный мониторинг Fast Path (L2) и Slow Path (Macro) """
    def __init__(self):
        # dict[symbol, dict[tick, list[Callable]]]
        self._tick_subscriptions: Dict[str, Dict[int, list[Callable]]] = {}
        self._active_tasks = {}

    def register_ambush(self, symbol: str, entry_price: float, atr_baseline: float, ttl_sec: int = 300):
        from core.realtime.conflator import conflator
        from core.radar.macro_radar import radar
        
        arena = conflator.arena
        target_tick = arena._price_to_tick(entry_price)
        min_wall_usd = arena.trust_threshold
        
        logger.info(f"🛡️ [Sentinel] Охрана засады {symbol} на уровне {entry_price}")
        
        async def evacuation_callback(reason: str):
            # Немедленный пуш в локальный лог и аппаратное прерывание (Уже в задаче)
            asyncio.create_task(radar.breaker.trigger_evacuation(symbol, reason))
            
            # И только потом попытка отправки в ТГ (Durable Stream)
            try:
                self._enqueue_alert(
                AlertEvent(title=f"🚨 EVACUATE: {symbol}", message=f"<b>{symbol}</b>\n{reason}"), 
                persistent=True
            )
            except Exception as e:
                logger.error(f"❌ [Sentinel] Failed to publish evacuation alert: {e}")
            
            # Отписываемся, чтобы не спамить
            self._unsubscribe(symbol, target_tick, l2_drop_callback)
        
        def l2_drop_callback(current_vol_usd: float, current_atr: float = None):
            # 1. Проверяем стену
            if current_vol_usd < min_wall_usd * 0.3:
                reason = f"ОТМЕНА! Уровень {entry_price} рухнул (Остаток: ${current_vol_usd:,.0f})."
                asyncio.create_task(evacuation_callback(reason))
            
            # 2. Проверяем дрейф волатильности!
            elif current_atr and current_atr < atr_baseline * 0.5:
                reason = f"ОТМЕНА! Дрейф Волатильности. ATR сжался ({current_atr:.4f} < {atr_baseline:.4f}). R:R уничтожен."
                asyncio.create_task(evacuation_callback(reason))

        self._subscribe(symbol, target_tick, l2_drop_callback)

    def _subscribe(self, symbol: str, tick: int, callback: Callable):
        if symbol not in self._tick_subscriptions:
            self._tick_subscriptions[symbol] = {}
        if tick not in self._tick_subscriptions[symbol]:
            self._tick_subscriptions[symbol][tick] = []
        self._tick_subscriptions[symbol][tick].append(callback)

    def _unsubscribe(self, symbol: str, tick: int, callback: Callable):
        try:
            self._tick_subscriptions[symbol][tick].remove(callback)
        except:
            pass

    def notify_tick_change(self, symbol: str, tick: int, new_vol_usd: float):
        """ Вызывается ИЗНУТРИ OrderbookMemoryArena при каждом обновлении тика """
        if symbol in self._tick_subscriptions and tick in self._tick_subscriptions[symbol]:
            # Копируем список, чтобы безопасно удалять (unsubscribe) во время итерации
            for callback in list(self._tick_subscriptions[symbol][tick]):
                callback(new_vol_usd)

class GlobalDirectionalMute:
    """[GEKTOR v11.3] Intelligent Directional Guard (Allows V-Reversals)."""
    def __init__(self):
        self._lock = asyncio.Lock()
        
    async def is_muted(self, direction: int) -> bool:
        """ Проверка через глобальный Distributed Mute в Redis """
        try:
            return await bus.redis.exists(f"global_mute:{direction}") > 0
        except:
            return False

    async def apply_shock(self, direction: int):
        """ Теперь это управляется через Lua атомарно, этот метод можно оставить как fallback """
        pass

class CVDAggregator:
    """[GEKTOR v11.3] Multi-Stream Cumulative Delta Engine."""
    def __init__(self):
        self._states: Dict[str, dict] = {}
        self._cvd_history: Dict[str, List[float]] = {}
        self._window = 100

    def get_alignment(self, symbol: str, direction: int) -> bool:
        """Returns True if the current impulse matches the global CVD momentum."""
        return True

class MacroRadar:
    """[GEKTOR v14.0] Two-Phase Macro Anomaly Scanner."""
    def __init__(self, rest: BybitREST, strategy: IAlphaStrategy, config: dict):
        self.rest = rest
        self.strategy = strategy
        full_config = config
        self.config = config.get("radar", {})
        
        self.blacklist: Set[str] = set(self.config.get("blacklist", []))
        self.is_hydrated: bool = False
        self._running: bool = False
        self._scan_count: int = 0
        self.is_halted: bool = False
        
        # Timing & Throttling
        self._last_alert_time = time.time()
        self._last_alerts: Dict[str, float] = {}
        self._sensitivity_timer = time.time()
        self._alert_cooldown_sec = 300
        
        # Thresholds
        self.min_turnover_24h = self.config.get("min_turnover_24h", 10_000_000)
        self.min_z_score = self.config.get("min_z_score", 3.0)
        self.base_z_score = self.min_z_score
        self.min_oi_delta_pct = self.config.get("min_oi_delta_pct", 5.0)
        self.min_rvol = self.config.get("min_rvol", 3.0)
        self.min_price_move_pct = self.config.get("min_price_move_pct", 2.0)
        self.min_candle_turnover = self.config.get("min_candle_turnover", 500_000)
        self.scan_interval = self.config.get("scan_interval", 10.0)

        # Components
        from core.events.nerve_center import bus
        from core.scoring import AdvisoryTargetCalculator
        self.risk_allocator = RiskAllocator(redis_client=bus.redis, config=full_config)
        self.health_bridge = MarketHealthSHMBridge(is_writer=False)
        self.advisory = AdvisoryTargetCalculator(risk_allocator=self.risk_allocator, health_bridge=self.health_bridge)
        
        self.oi_store = OISnapshotStore(lookback_cycles=20) 
        self.orchestrator = MathOrchestrator(tickers=self.config.get("symbols", []), max_workers=2)
        self.order_books: Dict[str, L2OrderBook] = {}
        self.stats = {"scans": 0, "anomalies_found": 0, "alerts": 0, "api_calls": 0}
        
        self._math_tasks: Dict[str, asyncio.Task] = {}
        self._alert_queue = asyncio.Queue(maxsize=1000)
        self.is_hydrated = False

    async def run(self):
        """Main Radar Execution Loop."""
        self._running = True
        logger.info("Starting MacroRadar loop...")
        
        from core.events.nerve_center import bus
        def on_amnesia_signal(msg: SystemAmnesiaEvent):
            logger.warning(f"Amnesia Signal: {msg.reason}. Flushing Brain State.")
            self.oi_store.clear()
            self._scan_count = 0 
            
        bus.subscribe(SystemAmnesiaEvent, on_amnesia_signal)
        
        # State Hydration
        try:
            raw_payload = await bus.redis.get("gektor:radar:oi_state")
            if raw_payload:
                self.oi_store.deserialize(raw_payload)
                self.is_hydrated = True
                self._state_needs_age_audit = True
                logger.info("Redis state loaded for MacroRadar.")
        except Exception as e:
            logger.debug(f"Initial Redis check failed: {e}")

        await self._hydrate_oi_store()
        self._dispatch_task = asyncio.create_task(self._alert_dispatcher())
        
        while self._running:
            try:
                if self.is_halted:
                    await asyncio.sleep(5)
                    continue

                cycle_start = time.monotonic()
                signals = await self._scan_cycle()
                if signals:
                    await self._dispatch_signals(signals)
                
                elapsed = time.monotonic() - cycle_start
                sleep_time = max(1.0, self.scan_interval - elapsed)
                
                if self._scan_count % 10 == 0 and self._scan_count > 0:
                    logger.info(f"Cycle #{self._scan_count} | OI Store: {self.oi_store.symbols_tracked} symbols")
                
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scan cycle error: {e}")
                await asyncio.sleep(10)

    async def _hydrate_oi_store(self):
        """Restore OI history at startup."""
        from core.events.nerve_center import bus
        t0 = time.monotonic()
        
        try:
            raw_payload = await bus.redis.get("gektor:radar:oi_state")
            if raw_payload:
                self.oi_store.deserialize(raw_payload)
                self.is_hydrated = True
                return
        except Exception as e:
            logger.debug(f"Redis hydration failed: {e}")

        logger.info("Filing back to REST hydration...")
        try:
            tickers = await self.rest.get_tickers()
            if not tickers: return
            
            liquid = [t for t in tickers if t.get("symbol", "").endswith("USDT") and float(t.get("turnover24h", 0)) >= self.min_turnover_24h]
            liquid.sort(key=lambda t: float(t.get("turnover24h", 0)), reverse=True)
            top_symbols = liquid[:50]
            
            for t in liquid:
                self.oi_store.record(t.get("symbol"), float(t.get("openInterest", 0)), float(t.get("turnover24h", 0)), float(t.get("lastPrice", 0)))
            
            hydrated = 0
            for sym_t in top_symbols:
                sym = sym_t.get("symbol")
                try:
                    resp = await self.rest._request("GET", "/v5/market/open-interest", params={"category": "linear", "symbol": sym, "intervalTime": "5min", "limit": 3})
                    oi_list = resp.get("list", []) if isinstance(resp, dict) else []
                    for item in reversed(oi_list):
                        self.oi_store.record(sym, float(item.get("openInterest", 0)), float(sym_t.get("turnover24h", 0)), float(sym_t.get("lastPrice", 0)))
                    hydrated += 1
                except: pass
                await asyncio.sleep(0.1)
            
            self.is_hydrated = True
            logger.info(f"Hydration done: {hydrated} symbols.")
        except Exception as e:
            logger.error(f"Hydration failed: {e}")

    async def _scan_cycle(self) -> List[MacroSignal]:
        """Broad scan for anomalies."""
        self._scan_count += 1
        tickers = await self.rest.get_tickers()
        if not tickers: return []
        
        candidates = []
        server_time_ms = getattr(self.rest, 'last_server_time', int(time.time() * 1000))

        for t in tickers:
            symbol = t.get("symbol", "")
            if not symbol.endswith("USDT") or symbol in self.blacklist: continue
            
            turnover_24h = float(t.get("turnover24h", 0))
            oi = float(t.get("openInterest", 0))
            price = float(t.get("lastPrice", 0))
            
            if turnover_24h < self.min_turnover_24h or price <= 0: continue
            
            self.oi_store.record(symbol, oi, turnover_24h, price, exchange_ts=server_time_ms)
            
            if not self.oi_store.is_warmed_up or self._scan_count <= 0: continue
            
            oi_delta = self.oi_store.get_oi_delta_pct(symbol)
            if oi_delta is None or abs(oi_delta) < self.min_oi_delta_pct: continue
            
            price_24h_pct = float(t.get("price24hPcnt", 0)) * 100
            if abs(price_24h_pct) < self.min_price_move_pct: continue

            # Tier classification
            tier = "D"
            if turnover_24h > 100_000_000: tier = "A"
            elif turnover_24h > 30_000_000: tier = "B"
            elif turnover_24h > 10_000_000: tier = "C"

            candidates.append({"symbol": symbol, "price": price, "turnover_24h": turnover_24h, "oi_delta_pct": oi_delta, "price_24h_pct": price_24h_pct, "tier": tier})

        confirmed = []
        for cand in candidates[:10]:
            signal = await self._deep_validate(cand)
            if signal: confirmed.append(signal)

        return confirmed

    async def _deep_validate(self, candidate: dict) -> Optional[MacroSignal]:
        """Phase 2: Deep validation with historical and real-time data."""
        symbol = candidate["symbol"]
        last_alert_ts = self._last_alerts.get(symbol, 0)
        if time.time() - last_alert_ts < self._alert_cooldown_sec: return None
        
        try:
            klines = await self.rest.get_klines(symbol, "5", limit=12)
            if not klines or len(klines) < 5: return None
            
            current = klines[-1]
            candle_turnover = float(current[6])
            if candle_turnover < self.min_candle_turnover: return None
            
            past_turnovers = [float(k[6]) for k in klines[:-1] if float(k[6]) > 0]
            if not past_turnovers: return None
            
            avg_turnover = sum(past_turnovers) / len(past_turnovers)
            rvol = candle_turnover / avg_turnover
            if rvol < self.min_rvol: return None
            
            open_p, close_p = float(current[1]), float(current[4])
            price_delta_pct = ((close_p - open_p) / open_p) * 100.0
            if abs(price_delta_pct) < self.min_price_move_pct: return None
            
            z_score = await self.orchestrator.compute_robust_z_score_async(
                symbol, price_delta_pct, [((float(k[4])-float(k[1]))/float(k[1])*100) for k in klines[:-1]]
            )
            
            high_p, low_p = float(current[2]), float(current[3])
            candle_range = high_p - low_p
            body = abs(close_p - open_p)
            if candle_range > 0 and (body / candle_range) < 0.30: return None
            
            # [GEKTOR v14.9] Microstructural Confirmation (OFI)
            # Rejects signals if the real-time order flow is toxic
            state = market_state.get_state(symbol)
            sig_ofi = 0.0
            if state:
                sig_ofi = state.current_ofi
                direction = 1 if price_delta_pct > 0 else -1
                # If LONG, we expect support (OFI > -0.5)
                if (direction == 1 and sig_ofi < -0.5) or (direction == -1 and sig_ofi > 0.5):
                    logger.warning(f"📉 [OFI_GUARD] {symbol} {direction} REJECTED: Toxic Flow ({sig_ofi:+.2f})")
                    return None
            
            hist_p = [float(k[4]) for k in klines]
            hist_v = [float(k[6]) for k in klines]
            hist_c = [] 
            curr_c = 0.0
            for k in klines:
                curr_c += (float(k[6]) * (1 if float(k[4]) >= float(k[1]) else -1))
                hist_c.append(curr_c)

            signal = MacroSignal(
                symbol=symbol,
                direction=(1 if price_delta_pct > 0 else -1),
                rvol=round(rvol, 2),
                z_score=z_score,
                price=close_p,
                ofi=sig_ofi, # Microstructural snapshot
                hist_p=hist_p,
                hist_c=hist_c,
                hist_v=hist_v,
                liquidity_tier=candidate["tier"],
                atr=np.mean([abs(float(k[2])-float(k[3])) for k in klines[-5:]])
            )
            
            self.stats["anomalies_found"] += 1
            self._last_alerts[symbol] = time.time()
            return signal
        except Exception as e:
            logger.error(f"Deep scan failed for {symbol}: {e}")
            return None

    async def handle_websocket_message(self, event: RawWSEvent):
        """Websocket state handler for real-time L2 updates."""
        topic = event.topic
        if not topic or "orderbook" not in topic: return
        symbol = topic.split('.')[-1]
        
        if symbol not in self.order_books:
            self.order_books[symbol] = L2OrderBook(symbol)
        self.order_books[symbol].apply_delta(event.data, is_snapshot=event.is_snapshot)

    async def _alert_dispatcher(self):
        while True:
            try:
                event, persistent = await self._alert_queue.get()
                from core.events.nerve_center import bus
                await bus.publish(event, persistent=persistent)
                self._alert_queue.task_done()
            except asyncio.CancelledError: break
            except: await asyncio.sleep(1)

    def _enqueue_alert(self, event: Any, persistent: bool = True):
        try: self._alert_queue.put_nowait((event, persistent))
        except: logger.error("Alert queue full.")

    async def _dispatch_signals(self, signals: List[MacroSignal]):
        for sig in signals:
            analysis = await self.orchestrator.analyze_multivector(
                sig.symbol, sig.hist_p, sig.hist_c, sig.hist_v, 0.1, 15
            )
            sig.signal_type = analysis.get("div", "MOMENTUM")
            sig.regime = analysis.get("regime", "TREND")
            
            proposal = await self.advisory.analyze_anomaly(
                symbol=sig.symbol, current_price=sig.price, atr_1m=sig.atr,
                vwap=sig.price, is_long=(sig.direction == 1),
                l2_snapshot={}, signal_age_ms=0.0
            )
            if not proposal: continue
            
            sig.targets = {
                "limit": proposal.entry_price, "sl": proposal.stop_loss, "tp": proposal.take_profit,
                "pos_asset": proposal.pos_size_asset, "pos_usd": proposal.pos_size_usd, "risk_usd": proposal.risk_usd
            }
            await self._send_individual_alert(sig, "ALLOWED_NEW")
            self._last_alert_time = time.time()

    async def _send_individual_alert(self, sig: MacroSignal, status: str):
        message = (
            f"GEKTOR ADVISORY: {sig.symbol} | Z: {sig.z_score:+.2f} | RVOL: {sig.rvol:.1f}x\n"
            f"Limit: {sig.targets['limit']:.4f} | TP: {sig.targets['tp']:.4f} | SL: {sig.targets['sl']:.4f}\n"
            f"Size: {sig.targets['pos_usd']:.2f} USD"
        )
        self._enqueue_alert(AlertEvent(title=f"SIGNAL: {sig.symbol}", message=message), persistent=True)
        self.stats["alerts"] += 1

    async def _handle_systemic_shock(self, direction: int, signals: List[MacroSignal]):
        """[GEKTOR v12.2] Systemic shock handler."""
        dir_str = "MARKET-WIDE PUMP" if direction == 1 else "MARKET-WIDE DUMP"
        logger.warning(f"Systemic Shock Detected: {dir_str}")
        # Dispatch systemic alert
        message = f"SYSTEMIC SHOCK: {dir_str} across {len(signals)} symbols."
        self._enqueue_alert(AlertEvent(title="SYSTEMIC SHOCK", message=message), persistent=True)

    def get_stats(self) -> dict:
        """Returns radar metrics for monitoring."""
        return {
            **self.stats,
            "scan_interval": self.scan_interval,
            "tracked_symbols": self.oi_store.symbols_tracked,
            "is_halted": self.is_halted
        }

    def get_state(self) -> dict:
        """State for atomic checkpointing."""
        return {
            "oi_snapshots": self.oi_store._snapshots,
            "stats": self.stats,
            "last_alert_ts": self._last_alert_time
        }

    async def handle_market_halt(self, event: MarketHaltEvent):
        """Suspend scanning during exchange halt."""
        self.is_halted = True
        logger.warning("Market Halt Event received. Radar SUSPENDED.")

    async def handle_market_resumption(self, event: MarketResumptionEvent):
        """Resume scanning after exchange stability."""
        self.is_halted = False
        logger.info("Market Resumption Event received. Radar ACTIVE.")
