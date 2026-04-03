# core/shield/amputation.py
"""
[GEKTOR v8.3] Amputation Protocol — /panic_sell

Emergency parallel liquidation of ALL positions and cancellation of ALL orders.

Architecture: Fan-Out Parallel Execution
  1. Cancel ALL open orders (single atomic API call)
  2. Fetch ALL positions
  3. Fan-Out: asyncio.gather() — fire Market Close for EVERY position simultaneously
  4. Each individual close retries independently with Exponential Backoff
  5. Timeout ceiling: 30 seconds total, then force-halt

The key insight: if you close 5 positions sequentially and each takes 5s (502 errors),
the last position liquidates after 25 seconds. In a -30% Flash Crash, that's death.
Fan-Out means ALL 5 close commands fire in the same millisecond.

Budget: N+2 REST calls (1 cancel-all + 1 positions fetch + N market closes)
"""

import asyncio
import time
import random
from typing import Dict, List, Optional, Any
from loguru import logger
from dataclasses import dataclass, field


@dataclass
class AmputationResult:
    """Result of a single position amputation."""
    symbol: str
    side: str
    size: str
    success: bool = False
    error: Optional[str] = None
    attempts: int = 0
    elapsed_ms: float = 0.0


class AmputationProtocol:
    """
    [GEKTOR v8.3] Emergency All-Position Liquidation Engine.
    
    Design Principles:
    - Fan-Out: ALL closes fire simultaneously (asyncio.gather)
    - Independent Retry: Each position retries on its own, not blocking others
    - Fail-Loud: Every step is logged and reported to Telegram
    - Hard Ceiling: 30-second total timeout — after that, system HALTs
    - Cancel-First: All pending limit orders cancelled BEFORE position closes
    """
    
    TOTAL_TIMEOUT = 30.0      # Max time for entire protocol
    PER_POSITION_RETRIES = 5  # Retries per individual position close
    
    # [GEKTOR v8.4] Global Circuit Breaker — Post-Panic Adrenaline Firewall
    # After amputation, ALL MacroRadar signals are suppressed for this duration.
    # Prevents FOMO re-entry into a falling knife immediately after panic exit.
    GLOBAL_COOLDOWN_SEC = 7200  # 2 hours
    COOLDOWN_KEY = "tekton:global_cooldown"
    
    def __init__(self, rest_client, kill_switch=None, bus=None):
        self.rest = rest_client
        self.kill_switch = kill_switch
        self.bus = bus
        self._executing = False
        self._last_execution = 0.0
    
    async def execute(self, operator_id: str = "UNKNOWN") -> Dict[str, Any]:
        """
        Execute the Amputation Protocol.
        
        Returns a report dict with results of each step.
        Idempotent: subsequent calls within 60s are rejected.
        """
        # ── Idempotency: prevent double-panic ──────────────────────────
        now = time.time()
        if self._executing:
            logger.warning("🚫 [AMPUTATION] Already executing. Ignoring duplicate.")
            return {"status": "DUPLICATE", "message": "Already executing"}
        
        if now - self._last_execution < 60:
            logger.warning("🚫 [AMPUTATION] Cooldown active (60s). Ignoring.")
            return {"status": "COOLDOWN", "message": "Executed <60s ago"}
        
        self._executing = True
        self._last_execution = now
        t0 = time.monotonic()
        
        report = {
            "status": "STARTED",
            "operator": operator_id,
            "timestamp": now,
            "cancel_orders": None,
            "positions_found": 0,
            "results": [],
            "elapsed_ms": 0,
        }
        
        logger.critical(
            f"☢️☢️☢️ [AMPUTATION PROTOCOL] INITIATED by {operator_id} ☢️☢️☢️"
        )
        
        try:
            # Wrap entire protocol in hard timeout
            result = await asyncio.wait_for(
                self._execute_protocol(report),
                timeout=self.TOTAL_TIMEOUT
            )
            return result
            
        except asyncio.TimeoutError:
            logger.critical(
                f"💀 [AMPUTATION] TIMEOUT after {self.TOTAL_TIMEOUT}s. "
                f"Some positions may still be open! MANUAL INTERVENTION REQUIRED."
            )
            report["status"] = "TIMEOUT"
            report["elapsed_ms"] = (time.monotonic() - t0) * 1000
            return report
            
        except Exception as e:
            logger.critical(f"💀 [AMPUTATION] FATAL ERROR: {e}")
            report["status"] = "FATAL_ERROR"
            report["error"] = str(e)
            return report
            
        finally:
            self._executing = False
            report["elapsed_ms"] = (time.monotonic() - t0) * 1000
    
    async def _execute_protocol(self, report: dict) -> dict:
        """Core protocol logic inside the timeout fence."""
        
        # ═══════════════════════════════════════════════════════════════
        # STEP 1: CANCEL ALL OPEN ORDERS (Atomic, single API call)
        # ═══════════════════════════════════════════════════════════════
        logger.critical("☢️ [STEP 1] Cancel ALL open orders...")
        try:
            cancel_result = await self.rest._request(
                "POST", "/v5/order/cancel-all",
                params={"category": "linear"},
                retries=3,
            )
            error = cancel_result.get("error") if isinstance(cancel_result, dict) else None
            if error:
                logger.error(f"⚠️ [STEP 1] Cancel-all returned: {error}")
                report["cancel_orders"] = f"ERROR: {error}"
            else:
                cancelled = cancel_result.get("list", []) if isinstance(cancel_result, dict) else []
                count = len(cancelled) if isinstance(cancelled, list) else 0
                logger.critical(f"✅ [STEP 1] Cancel-all: {count} orders cancelled.")
                report["cancel_orders"] = f"OK: {count} cancelled"
        except Exception as e:
            logger.error(f"⚠️ [STEP 1] Cancel-all failed: {e}. Continuing...")
            report["cancel_orders"] = f"FAILED: {e}"
        
        # ═══════════════════════════════════════════════════════════════
        # STEP 2: FETCH ALL OPEN POSITIONS
        # ═══════════════════════════════════════════════════════════════
        logger.critical("☢️ [STEP 2] Fetching all open positions...")
        try:
            positions = await self.rest.get_positions(category="linear")
        except Exception as e:
            logger.critical(f"💀 [STEP 2] Cannot fetch positions: {e}")
            report["status"] = "POSITIONS_FETCH_FAILED"
            return report
        
        open_positions = [
            p for p in positions
            if float(p.get("size", 0)) > 0
        ]
        
        report["positions_found"] = len(open_positions)
        
        if not open_positions:
            logger.info("✅ [STEP 2] No open positions. Nothing to amputate.")
            report["status"] = "NO_POSITIONS"
            return report
        
        logger.critical(
            f"☢️ [STEP 2] Found {len(open_positions)} positions to AMPUTATE: "
            f"{[p['symbol'] for p in open_positions]}"
        )
        
        # ═══════════════════════════════════════════════════════════════
        # STEP 3: FAN-OUT — PARALLEL MARKET CLOSE ALL POSITIONS
        # ═══════════════════════════════════════════════════════════════
        # Each close fires SIMULTANEOUSLY. No waiting for neighbors.
        # Each has its own retry loop with exponential backoff.
        logger.critical("☢️ [STEP 3] FAN-OUT: Firing parallel market closes!")
        
        close_tasks = [
            self._amputate_single_position(pos)
            for pos in open_positions
        ]
        
        # asyncio.gather with return_exceptions=True — never blocks on neighbor failure
        results = await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                r = AmputationResult(
                    symbol=open_positions[i].get("symbol", "?"),
                    side=open_positions[i].get("side", "?"),
                    size=open_positions[i].get("size", "?"),
                    success=False,
                    error=str(result),
                )
            else:
                r = result
            report["results"].append(r)
        
        # Summary
        success_count = sum(1 for r in report["results"] if isinstance(r, AmputationResult) and r.success)
        fail_count = len(report["results"]) - success_count
        
        if fail_count == 0:
            report["status"] = "ALL_CLOSED"
            logger.critical(
                f"✅✅✅ [AMPUTATION] ALL {success_count} positions closed! "
                f"Elapsed: {report['elapsed_ms']:.0f}ms"
            )
        else:
            report["status"] = "PARTIAL"
            logger.critical(
                f"⚠️ [AMPUTATION] {success_count} closed, {fail_count} FAILED. "
                f"MANUAL INTERVENTION REQUIRED!"
            )
        
        # ═══════════════════════════════════════════════════════════════
        # STEP 4: ENGAGE KILL SWITCH (prevent new trades)
        # ═══════════════════════════════════════════════════════════════
        if self.kill_switch:
            try:
                await self.kill_switch.engage(
                    reason="AMPUTATION_PROTOCOL",
                    trigger_id="operator_panic_sell"
                )
                logger.critical("🛑 [STEP 4] Kill switch ENGAGED. No new trades allowed.")
            except Exception as e:
                logger.error(f"⚠️ Kill switch engagement failed: {e}")
        
        # ═══════════════════════════════════════════════════════════════
        # STEP 5: GLOBAL CIRCUIT BREAKER — Adrenaline Firewall
        # ═══════════════════════════════════════════════════════════════
        # Suppress ALL MacroRadar signals for 2 hours.
        # Prevents FOMO re-entry into falling knife.
        await self._engage_global_cooldown(report.get("operator", "UNKNOWN"))
        
        return report
    
    async def _amputate_single_position(self, pos: dict) -> AmputationResult:
        """
        Close a SINGLE position with aggressive retry.
        
        Each position runs independently — failure here does NOT block others.
        Uses Market Order + reduceOnly for guaranteed close direction.
        """
        symbol = pos.get("symbol", "?")
        side = pos.get("side", "?")
        size = pos.get("size", "0")
        close_side = "Sell" if side == "Buy" else "Buy"
        
        result = AmputationResult(symbol=symbol, side=side, size=size)
        t0 = time.monotonic()
        
        for attempt in range(self.PER_POSITION_RETRIES):
            result.attempts = attempt + 1
            try:
                resp = await self.rest._request(
                    "POST", "/v5/order/create",
                    params={
                        "category": "linear",
                        "symbol": symbol,
                        "side": close_side,
                        "orderType": "Market",
                        "qty": str(size),
                        "reduceOnly": True,
                        "timeInForce": "IOC",
                        "positionIdx": 0,  # One-way mode
                    },
                    retries=1,  # Single attempt per REST call (we handle retries)
                )
                
                error = resp.get("error") if isinstance(resp, dict) else None
                ret_code = resp.get("retCode") if isinstance(resp, dict) else None
                
                # Success
                if not error and ret_code in (None, 0):
                    result.success = True
                    result.elapsed_ms = (time.monotonic() - t0) * 1000
                    logger.critical(
                        f"✅ [AMPUTATE] {symbol} {close_side} {size} — "
                        f"CLOSED (attempt {attempt+1}, {result.elapsed_ms:.0f}ms)"
                    )
                    return result
                
                # Rate limit — back off
                if ret_code in (10006, 10018) or (isinstance(error, str) and "429" in error):
                    wait = (2 ** attempt) * 0.5 + random.uniform(0.1, 0.3)
                    logger.warning(f"⏳ [AMPUTATE] {symbol} rate limited. Wait {wait:.1f}s...")
                    await asyncio.sleep(wait)
                    continue
                
                # Other API error — retry with backoff
                logger.error(f"⚠️ [AMPUTATE] {symbol} attempt {attempt+1}: {error or resp}")
                
            except (asyncio.TimeoutError, Exception) as e:
                logger.error(f"🌐 [AMPUTATE] {symbol} attempt {attempt+1} error: {e}")
            
            # Exponential backoff with jitter (0.5s, 1s, 2s, 4s)
            wait = min(8.0, (0.5 * (2 ** attempt))) + random.uniform(0.05, 0.2)
            await asyncio.sleep(wait)
        
        # All retries exhausted
        result.elapsed_ms = (time.monotonic() - t0) * 1000
        result.error = f"FAILED after {self.PER_POSITION_RETRIES} attempts"
        logger.critical(
            f"💀 [AMPUTATE] {symbol} — FAILED TO CLOSE! {result.error}"
        )
        return result
    
    async def _engage_global_cooldown(self, operator_id: str):
        """
        [GEKTOR v8.4] Global Circuit Breaker — Adrenaline Firewall.
        
        Sets a Redis key that MacroRadar and bot_listener check
        BEFORE dispatching/executing any signal. Key has TTL of 2 hours,
        self-cleaning and survives system restarts.
        
        Psychology: After panic exit, adrenaline peaks. FOMO screams "BUY THE DIP!"
        This firewall gives the operator AND the market 2 hours to stabilize.
        """
        try:
            from core.events.nerve_center import bus as nerve_bus
            import json
            
            cooldown_data = json.dumps({
                "activated_at": time.time(),
                "operator": operator_id,
                "reason": "AMPUTATION_PROTOCOL",
                "expires_in_sec": self.GLOBAL_COOLDOWN_SEC,
            })
            
            await nerve_bus.redis.set(
                self.COOLDOWN_KEY,
                cooldown_data,
                ex=self.GLOBAL_COOLDOWN_SEC,
            )
            
            hours = self.GLOBAL_COOLDOWN_SEC / 3600
            logger.critical(
                f"🧊 [GLOBAL COOLDOWN] ENGAGED for {hours:.0f}h. "
                f"ALL MacroRadar signals SUPPRESSED. "
                f"Override: /cooldown_off"
            )
        except Exception as e:
            logger.error(
                f"⚠️ [GLOBAL COOLDOWN] Failed to set Redis key: {e}. "
                f"MacroRadar may still fire signals!"
            )
    
    @staticmethod
    async def check_global_cooldown() -> dict | None:
        """
        Check if Global Circuit Breaker is active.
        Returns cooldown metadata dict if active, None if clear.
        """
        try:
            from core.events.nerve_center import bus as nerve_bus
            import json
            
            raw = await nerve_bus.redis.get(AmputationProtocol.COOLDOWN_KEY)
            if raw:
                data = json.loads(raw)
                # Calculate remaining time
                activated = data.get("activated_at", 0)
                expires_in = data.get("expires_in_sec", 7200)
                remaining = max(0, (activated + expires_in) - time.time())
                data["remaining_sec"] = remaining
                data["remaining_min"] = round(remaining / 60, 1)
                return data
            return None
        except Exception:
            return None
    
    @staticmethod
    async def disengage_global_cooldown() -> bool:
        """Manually remove the Global Circuit Breaker. Returns True if removed."""
        try:
            from core.events.nerve_center import bus as nerve_bus
            result = await nerve_bus.redis.delete(AmputationProtocol.COOLDOWN_KEY)
            if result:
                logger.warning("🔓 [GLOBAL COOLDOWN] Manually disengaged by operator.")
            return bool(result)
        except Exception as e:
            logger.error(f"❌ [GLOBAL COOLDOWN] Disengage failed: {e}")
            return False
    
    def format_report_telegram(self, report: dict) -> str:
        """Format amputation report for Telegram."""
        status_emoji = {
            "ALL_CLOSED": "✅",
            "PARTIAL": "⚠️",
            "NO_POSITIONS": "✅",
            "TIMEOUT": "💀",
            "FATAL_ERROR": "💀",
        }
        emoji = status_emoji.get(report.get("status", ""), "❓")
        
        hours = self.GLOBAL_COOLDOWN_SEC / 3600
        
        lines = [
            f"{emoji} <b>AMPUTATION PROTOCOL</b> {emoji}",
            f"━━━━━━━━━━━━━━━━━━━",
            f"Статус: <b>{report.get('status', '?')}</b>",
            f"Оператор: {report.get('operator', '?')}",
            f"Ордера отменены: {report.get('cancel_orders', '?')}",
            f"Позиций найдено: {report.get('positions_found', 0)}",
            f"Время: {report.get('elapsed_ms', 0):.0f}ms",
            f"━━━━━━━━━━━━━━━━━━━",
            f"🧊 <b>GLOBAL COOLDOWN: {hours:.0f}ч</b>",
            f"MacroRadar заглушён. Снять: /cooldown_off",
        ]
        
        for r in report.get("results", []):
            if isinstance(r, AmputationResult):
                status = "✅" if r.success else "❌"
                lines.append(
                    f"{status} {r.symbol} {r.side} {r.size} — "
                    f"{r.attempts} попыток, {r.elapsed_ms:.0f}ms"
                )
                if r.error:
                    lines.append(f"   └─ {r.error[:80]}")
        
        return "\n".join(lines)


# Singleton (lazy init)
_amputation_protocol: Optional[AmputationProtocol] = None

def get_amputation_protocol(rest_client=None, kill_switch=None, bus=None) -> AmputationProtocol:
    """Get or create the singleton AmputationProtocol."""
    global _amputation_protocol
    if _amputation_protocol is None and rest_client:
        _amputation_protocol = AmputationProtocol(rest_client, kill_switch, bus)
    return _amputation_protocol
