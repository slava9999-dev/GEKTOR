# core/radar/macro_radar.py
"""
[GEKTOR v8.1] Macro Anomaly Radar — Signal Purity Engine.

Architecture: Two-Phase Scan + Cold Start Hydration + One-Click Entry
  Phase 0 (Hydration):   50 REST calls at startup → /v5/market/open-interest
                          Pre-fills OI ring buffer so radar is ready at t=0

  Phase 1 (Broad Scan):  1 REST call → GET /v5/market/tickers → ALL 300 tickers
                          Compare OI + Volume snapshots against previous cycle
                          Filter: turnover > $1M absolute + OI Delta > 5% + Price Move > 2%
                          Output: 3-7 candidates (Shortlist)

  Phase 2 (Deep Scan):   3-7 REST calls → GET /v5/market/kline (5m candles)
                          Validate RVOL on 5m turnover (not 24h average)
                          Confirm wick structure (no full rejection candles)
                          Output: 0-3 confirmed anomalies → Telegram with Inline Keyboard

  One-Click Entry:       Telegram Inline Keyboard (⚡ LONG / ⚡ SHORT / 🚫 ПАС)
                          Callback → OMS Limit IOC order → 0.5s reaction time

Budget: 8-10 REST requests per minute (vs 120+ in old RadarV2)
Proxy-Safe: Single-connection sequential flow, no parallel storms.
"""

import asyncio
import hashlib
import json
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from loguru import logger

from data.bybit_rest import BybitREST
from utils.math_utils import log_throttler


def _generate_signal_id(symbol: str, direction: str) -> str:
    """Deterministic short ID for callback_data (max 64 bytes in TG)."""
    raw = f"{symbol}:{direction}:{int(time.time())}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]


@dataclass
class MacroSignal:
    """Confirmed macro anomaly signal for Telegram delivery."""
    symbol: str
    direction: str        # "LONG_IMPULSE" | "SHORT_IMPULSE"
    rvol: float           # Relative Volume (turnover-based)
    oi_delta_pct: float   # OI change since last snapshot (%)
    price_delta_pct: float  # Price move in current 5m candle (%)
    turnover_usdt: float  # Absolute turnover in current candle ($)
    funding_rate: float   # Current funding rate
    liquidity_tier: str   # A/B/C/D
    price: float = 0.0    # Current price for order pre-fill
    signal_id: str = ""   # Unique ID for callback tracking
    timestamp: float = field(default_factory=time.time)

    @property
    def is_overheated(self) -> bool:
        """Фандинг > 0.05% = рынок перегрет в одну сторону."""
        if self.direction == "LONG_IMPULSE" and self.funding_rate > 0.0005:
            return True
        if self.direction == "SHORT_IMPULSE" and self.funding_rate < -0.0005:
            return True
        return False


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
    
    def record(self, symbol: str, oi: float, turnover_24h: float, price: float):
        """Record current OI and turnover snapshot."""
        if symbol not in self._snapshots:
            self._snapshots[symbol] = []
        
        history = self._snapshots[symbol]
        history.append({
            "oi": oi,
            "turnover": turnover_24h,
            "price": price,
            "ts": time.time()
        })
        
        # Keep only last N entries
        if len(history) > self._lookback:
            self._snapshots[symbol] = history[-self._lookback:]
    
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


class MacroRadar:
    """
    [GEKTOR v8.0] Two-Phase Macro Anomaly Scanner.
    
    Designed for Hostile Environment (laggy proxy, REST-only).
    Uses MINIMAL API calls to detect institutional-grade anomalies.
    
    Budget: ~8 REST requests per scan cycle (vs 120+ in RadarV2).
    """
    
    def __init__(
        self,
        rest: BybitREST,
        config: dict,
        scan_interval_sec: float = 60.0,
    ):
        self.rest = rest
        self.config = config
        self.scan_interval = scan_interval_sec
        
        # OI Snapshot Store (10 cycles = 10 min lookback at 1 scan/min)
        self.oi_store = OISnapshotStore(lookback_cycles=10)
        
        # Configuration (from config_sniper.yaml macro_radar section)
        # NOTE: main.py passes the already-extracted macro_radar dict
        self.min_turnover_24h = config.get("min_turnover_24h_usd", 5_000_000)
        self.min_candle_turnover = config.get("min_candle_turnover_usd", 500_000)
        self.min_rvol = config.get("min_rvol", 3.0)
        self.min_oi_delta_pct = config.get("min_oi_delta_pct", 5.0)
        self.min_price_move_pct = config.get("min_price_move_pct", 1.5)
        self.blacklist = set(config.get("blacklist", [
            "LUNAUSDT", "USTCUSDT", "FTTUSDT"
        ]))
        
        # State
        self._running = False
        self._scan_count = 0
        self._last_alerts: Dict[str, float] = {}  # symbol → last alert timestamp
        self._alert_cooldown_sec = config.get("alert_cooldown_sec", 300)  # 5 min between same symbol
        
        # Stats
        self.stats = {
            "scans": 0,
            "anomalies_found": 0,
            "alerts_sent": 0,
            "api_calls": 0,
        }
    
    async def start(self):
        """Main scan loop. Runs until stopped."""
        self._running = True
        logger.info(
            f"📡 [MacroRadar] ONLINE — Scan every {self.scan_interval}s | "
            f"Min Turnover: ${self.min_turnover_24h:,.0f} | "
            f"RVOL: >{self.min_rvol} | OI Δ: >{self.min_oi_delta_pct}%"
        )
        
        # ── [GEKTOR v8.1] Cold Start Hydration ────────────────────────
        # Pre-fill OI ring buffer from historical data so radar is
        # ready at t=0 instead of waiting 10 minutes for warmup.
        await self._hydrate_oi_store()
        
        while self._running:
            try:
                cycle_start = time.monotonic()
                
                signals = await self._scan_cycle()
                
                if signals:
                    await self._dispatch_signals(signals)
                
                elapsed = time.monotonic() - cycle_start
                sleep_time = max(1.0, self.scan_interval - elapsed)
                
                # Periodic status log
                if self._scan_count % 10 == 0 and self._scan_count > 0:
                    logger.info(
                        f"📡 [MacroRadar] Cycle #{self._scan_count} | "
                        f"Elapsed: {elapsed:.1f}s | "
                        f"OI Store: {self.oi_store.symbols_tracked} symbols | "
                        f"Warmed: {'✅' if self.oi_store.is_warmed_up else '⏳'} | "
                        f"Total Anomalies: {self.stats['anomalies_found']}"
                    )
                
                await asyncio.sleep(sleep_time)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [MacroRadar] Scan cycle error: {e}")
                await asyncio.sleep(10)  # Back off on error
    
    def stop(self):
        self._running = False
        logger.info("🛑 [MacroRadar] Stopped.")
    
    # =========================================================================
    # PHASE 0: COLD START HYDRATION (eliminates 10-min blindness on restart)
    # =========================================================================
    
    async def _hydrate_oi_store(self):
        """
        [GEKTOR v8.1] Cold Start Hydration.
        
        On startup, fetches historical OI data for top-50 liquid symbols
        using /v5/market/open-interest endpoint (5min intervals, 3 points = 15 min).
        This pre-fills the ring buffer so OI Delta is available immediately.
        
        Budget: ~50 REST calls (one-time at startup, sequential with 100ms gaps).
        """
        logger.info("🔄 [MacroRadar] Cold Start Hydration — pre-loading OI history...")
        t0 = time.monotonic()
        
        try:
            # Step 1: Get current tickers to identify top-50 by turnover
            tickers = await self.rest.get_tickers()
            self.stats["api_calls"] += 1
            
            if not tickers:
                logger.warning("⚠️ [Hydration] Tickers empty. Radar starts cold.")
                return
            
            # Filter and sort by turnover
            liquid = [
                t for t in tickers
                if t.get("symbol", "").endswith("USDT")
                and float(t.get("turnover24h", 0)) >= self.min_turnover_24h
                and t.get("symbol") not in self.blacklist
            ]
            liquid.sort(key=lambda t: float(t.get("turnover24h", 0)), reverse=True)
            top_symbols = liquid[:50]  # Top-50 by liquidity
            
            # Step 2: Also record current snapshot from tickers (free data)
            for t in liquid:
                sym = t.get("symbol", "")
                oi = float(t.get("openInterest", 0))
                turnover = float(t.get("turnover24h", 0))
                price = float(t.get("lastPrice", 0))
                if oi > 0:
                    self.oi_store.record(sym, oi, turnover, price)
            
            # Step 3: Fetch historical OI for top-50 (sequential, not parallel)
            hydrated = 0
            for t in top_symbols:
                sym = t["symbol"]
                try:
                    resp = await self.rest._request(
                        "GET", "/v5/market/open-interest",
                        params={
                            "category": "linear",
                            "symbol": sym,
                            "intervalTime": "5min",
                            "limit": 3  # 3 points = 15 minutes of history
                        }
                    )
                    self.stats["api_calls"] += 1
                    
                    oi_list = resp.get("list", []) if isinstance(resp, dict) else []
                    
                    # Bybit returns newest first, we need chronological order
                    for item in reversed(oi_list):
                        historical_oi = float(item.get("openInterest", 0))
                        if historical_oi > 0:
                            # Record with current turnover/price as approximation
                            turnover = float(t.get("turnover24h", 0))
                            price = float(t.get("lastPrice", 0))
                            self.oi_store.record(sym, historical_oi, turnover, price)
                    
                    hydrated += 1
                    
                except Exception as e:
                    logger.debug(f"⚠️ [Hydration] OI fetch failed for {sym}: {e}")
                
                # Gentle pacing: 100ms between requests to avoid rate limits
                await asyncio.sleep(0.1)
            
            elapsed = time.monotonic() - t0
            logger.success(
                f"✅ [MacroRadar] Hydration DONE — {hydrated}/{len(top_symbols)} symbols | "
                f"{elapsed:.1f}s | OI store: {self.oi_store.symbols_tracked} symbols | "
                f"Warmed: {'✅' if self.oi_store.is_warmed_up else '⏳'}"
            )
            
        except Exception as e:
            logger.error(f"❌ [Hydration] Failed: {e}. Radar starts cold (10 min warmup).")
    
    # =========================================================================
    # PHASE 1: BROAD SCAN (1 API call)
    # =========================================================================
    
    async def _scan_cycle(self) -> List[MacroSignal]:
        """
        Complete two-phase scan cycle.
        
        Phase 1: 1 REST call → filter candidates
        Phase 2: N REST calls → deep validate candidates
        
        Returns list of confirmed MacroSignal objects.
        """
        self._scan_count += 1
        self.stats["scans"] += 1
        
        # ── PHASE 1: Broad Scan ──────────────────────────────────────────────
        t0 = time.monotonic()
        
        tickers = await self.rest.get_tickers()
        self.stats["api_calls"] += 1
        
        if not tickers:
            logger.warning("⚠️ [MacroRadar] Empty tickers response. Skipping cycle.")
            return []
        
        t_fetch = time.monotonic() - t0
        
        # Record ALL tickers into OI store (building history for Delta calc)
        candidates = []
        universe_size = 0
        
        for t in tickers:
            symbol = t.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            if symbol in self.blacklist:
                continue
            
            turnover_24h = float(t.get("turnover24h", 0))
            oi = float(t.get("openInterest", 0))
            price = float(t.get("lastPrice", 0))
            
            # Filter: minimum liquidity floor
            if turnover_24h < self.min_turnover_24h or price <= 0:
                continue
            
            universe_size += 1
            
            # Record snapshot for OI Delta tracking
            self.oi_store.record(symbol, oi, turnover_24h, price)
            
            # Skip scoring until OI store is warmed up (need history)
            if not self.oi_store.is_warmed_up:
                continue
            
            # ── Phase 1 Filters ──────────────────────────────────────────
            
            # 1. OI Delta check (vs oldest stored snapshot)
            oi_delta = self.oi_store.get_oi_delta_pct(symbol)
            if oi_delta is None or abs(oi_delta) < self.min_oi_delta_pct:
                continue
            
            # 2. Price movement check (24h % change from tickers endpoint)
            price_24h_pct = float(t.get("price24hPcnt", 0)) * 100  # Convert to %
            if abs(price_24h_pct) < self.min_price_move_pct:
                continue
            
            # 3. Turnover anomaly (current 24h vs stored baseline)
            baseline_turnover = self.oi_store.get_turnover_baseline(symbol)
            if baseline_turnover and baseline_turnover > 0:
                turnover_spike = turnover_24h / baseline_turnover
                # 24h turnover is relatively stable, a 1.5x jump is significant
                if turnover_spike < 1.3:  # Minimum 30% above baseline
                    continue
            
            # ── CANDIDATE FOUND ──────────────────────────────────────────
            funding = float(t.get("fundingRate", 0))
            
            # Liquidity Tier
            tier = "D"
            if turnover_24h > 100_000_000: tier = "A"
            elif turnover_24h > 30_000_000: tier = "B"
            elif turnover_24h > 10_000_000: tier = "C"
            
            candidates.append({
                "symbol": symbol,
                "price": price,
                "turnover_24h": turnover_24h,
                "oi_delta_pct": oi_delta,
                "price_24h_pct": price_24h_pct,
                "funding_rate": funding,
                "tier": tier,
            })
        
        if not self.oi_store.is_warmed_up:
            if self._scan_count % 3 == 0:
                logger.info(
                    f"⏳ [MacroRadar] Warming up OI store... "
                    f"Cycle #{self._scan_count} | Universe: {universe_size} | "
                    f"Fetch: {t_fetch:.1f}s"
                )
            return []
        
        if not candidates:
            return []
        
        logger.info(
            f"🔍 [MacroRadar P1] {len(candidates)} candidates from {universe_size} universe | "
            f"Fetch: {t_fetch:.1f}s"
        )
        
        # ── PHASE 2: Deep Scan (N API calls for N candidates) ────────────
        confirmed = []
        for cand in candidates[:10]:  # Hard cap: max 10 deep scans per cycle
            signal = await self._deep_validate(cand)
            if signal:
                confirmed.append(signal)
        
        return confirmed
    
    # =========================================================================
    # PHASE 2: DEEP SCAN (1 API call per candidate)
    # =========================================================================
    
    async def _deep_validate(self, candidate: dict) -> Optional[MacroSignal]:
        """
        Phase 2: Fetch 5-minute klines for a candidate to confirm anomaly.
        
        Validates:
        1. RVOL on 5m candle turnover (not 24h average)
        2. Absolute turnover floor ($500K+ per candle)
        3. Candle body structure (reject full-wick rejections)
        
        Costs: 1 API call.
        """
        symbol = candidate["symbol"]
        
        # Cooldown check: don't alert same symbol within 5 minutes
        last_alert_ts = self._last_alerts.get(symbol, 0)
        if time.time() - last_alert_ts < self._alert_cooldown_sec:
            return None
        
        try:
            # Fetch last 12 five-minute candles (1 hour of data)
            # Format: [startTime, open, high, low, close, volume, turnover]
            klines = await self.rest.get_klines(symbol, "5", limit=12)
            self.stats["api_calls"] += 1
            
            if not klines or len(klines) < 5:
                return None
            
            # Current candle = last element (klines are oldest→newest after reverse in get_klines)
            current = klines[-1]
            candle_turnover = float(current[6])  # turnover in USDT
            
            # ── Filter 1: Absolute Turnover Floor ────────────────────────
            if candle_turnover < self.min_candle_turnover:
                return None  # Not enough real money in this candle
            
            # ── Filter 2: RVOL on 5m Candle Turnover ────────────────────
            # Compare current candle turnover vs average of previous candles
            past_turnovers = [float(k[6]) for k in klines[:-1] if float(k[6]) > 0]
            if len(past_turnovers) < 3:
                return None
            
            avg_turnover = sum(past_turnovers) / len(past_turnovers)
            if avg_turnover <= 0:
                return None
            
            rvol = candle_turnover / avg_turnover
            
            if rvol < self.min_rvol:
                return None  # Not anomalous enough
            
            # ── Filter 3: Candle Body Structure ──────────────────────────
            open_p = float(current[1])
            high_p = float(current[2])
            low_p = float(current[3])
            close_p = float(current[4])
            
            if open_p <= 0:
                return None
            
            price_delta_pct = ((close_p - open_p) / open_p) * 100.0
            candle_range = high_p - low_p
            body = abs(close_p - open_p)
            
            # Reject full-wick rejection candles (body < 30% of range)
            # These are market makers pulling liquidity, not institutions entering.
            if candle_range > 0 and (body / candle_range) < 0.30:
                logger.debug(
                    f"🕯️ [MacroRadar] {symbol} rejected: "
                    f"Wick candle (body {body/candle_range:.0%} of range)"
                )
                return None
            
            # ── Filter 4: Price Move Confirmation ────────────────────────
            if abs(price_delta_pct) < self.min_price_move_pct:
                return None  # Move too small to be actionable
            
            # ══ SIGNAL CONFIRMED ═════════════════════════════════════════
            direction = "LONG_IMPULSE" if price_delta_pct > 0 else "SHORT_IMPULSE"
            sig_id = _generate_signal_id(symbol, direction)
            
            signal = MacroSignal(
                symbol=symbol,
                direction=direction,
                rvol=round(rvol, 2),
                oi_delta_pct=round(candidate["oi_delta_pct"], 2),
                price_delta_pct=round(price_delta_pct, 2),
                turnover_usdt=candle_turnover,
                funding_rate=candidate["funding_rate"],
                liquidity_tier=candidate["tier"],
                price=close_p,
                signal_id=sig_id,
            )
            
            self.stats["anomalies_found"] += 1
            self._last_alerts[symbol] = time.time()
            
            logger.info(
                f"🎯 [ANOMALY] {symbol} | {direction} | "
                f"RVOL: {rvol:.1f}x | OI Δ: {candidate['oi_delta_pct']:+.1f}% | "
                f"Price: {price_delta_pct:+.1f}% | "
                f"Turnover: ${candle_turnover:,.0f} | Tier: {candidate['tier']}"
            )
            
            return signal
            
        except Exception as e:
            logger.error(f"❌ [MacroRadar] Deep scan failed for {symbol}: {e}")
            return None
    
    # =========================================================================
    # DISPATCH: Send confirmed signals to Telegram
    # =========================================================================
    
    async def _dispatch_signals(self, signals: List[MacroSignal]):
        """
        Formats and sends confirmed anomaly alerts to Telegram via Outbox.
        
        [GEKTOR v8.1] Includes One-Click Entry Inline Keyboard.
        [GEKTOR v8.4] Checks Global Circuit Breaker before dispatch.
        """
        from core.alerts.outbox import telegram_outbox, MessagePriority
        
        # ══════════════════════════════════════════════════════════════════
        # [GEKTOR v8.4] GLOBAL CIRCUIT BREAKER — Anti-FOMO Adrenaline Wall
        # ══════════════════════════════════════════════════════════════════
        # After /panic_sell, this key exists in Redis for 2 hours.
        # ALL signals are silently suppressed. No TG noise. No temptation.
        try:
            from core.shield.amputation import AmputationProtocol
            cooldown = await AmputationProtocol.check_global_cooldown()
            if cooldown:
                remaining_min = cooldown.get("remaining_min", "?")
                if self._scan_count % 10 == 0:  # Log only every 10th cycle
                    logger.warning(
                        f"🧊 [COOLDOWN] {len(signals)} signals SUPPRESSED. "
                        f"Post-panic cooldown: {remaining_min} min remaining. "
                        f"Override: /cooldown_off"
                    )
                self.stats["signals_suppressed"] = self.stats.get("signals_suppressed", 0) + len(signals)
                return  # Silent exit. No alerts. No FOMO.
        except Exception as e:
            logger.debug(f"⚠️ [COOLDOWN] Check failed: {e}. Proceeding with dispatch.")
        
        for sig in signals:
            # Direction emoji and color
            if sig.direction == "LONG_IMPULSE":
                emoji = "🟢"
                dir_text = "ЛОНГ-ИМПУЛЬС"
            else:
                emoji = "🔴"
                dir_text = "ШОРТ-ИМПУЛЬС"
            
            # Overheat warning
            overheat = ""
            if sig.is_overheated:
                overheat = "\n⚠️ <b>Фандинг перегрет!</b> Осторожно с направлением."
            
            # Tier badge
            tier_badges = {"A": "💎", "B": "🏆", "C": "⚡", "D": "🔸"}
            tier_badge = tier_badges.get(sig.liquidity_tier, "")
            
            from core.alerts.formatters import _format_price
            price_str = _format_price(sig.price) if sig.price > 0 else "—"
            
            message = (
                f"{emoji} <b>МАКРО-АНОМАЛИЯ</b> {emoji}\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>{sig.symbol}</b> | {dir_text}\n"
                f"💵 Цена: <code>{price_str}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"🔥 RVOL: <b>{sig.rvol}x</b> (>{self.min_rvol}x)\n"
                f"📈 OI Δ: <b>{sig.oi_delta_pct:+.1f}%</b>\n"
                f"💰 Свеча: <b>${sig.turnover_usdt:,.0f}</b>\n"
                f"📐 Движение: <b>{sig.price_delta_pct:+.1f}%</b>\n"
                f"🏷️ Ликвидность: {tier_badge} Tier {sig.liquidity_tier}\n"
                f"💸 Фандинг: {sig.funding_rate*100:.4f}%"
                f"{overheat}\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 <i>Ищи FVG / Order Block / Discount Zone</i>"
            )
            
            # ──────────────────────────────────────────────────────────
            # [GEKTOR v8.1] ONE-CLICK TACTICAL KEYBOARD
            # Callback format: macro_exec|SYMBOL|PRICE|DIRECTION|SIGNAL_ID
            # Total < 64 bytes (Telegram limit)
            # ──────────────────────────────────────────────────────────
            direction_short = "L" if sig.direction == "LONG_IMPULSE" else "S"
            cb_exec = f"mexec|{sig.symbol}|{sig.price:.4f}|{direction_short}|{sig.signal_id}"
            cb_pass = f"mpass|{sig.signal_id}"
            
            # Buttons: Primary action (directional entry) + Pass
            if sig.direction == "LONG_IMPULSE":
                exec_text = "⚡ LONG (Limit IOC)"
            else:
                exec_text = "⚡ SHORT (Limit IOC)"
            
            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": exec_text, "callback_data": cb_exec},
                        {"text": "🚫 ПАС", "callback_data": cb_pass},
                    ],
                    [
                        {
                            "text": "📊 Bybit",
                            "url": f"https://www.bybit.com/trade/usdt/{sig.symbol}"
                        },
                        {
                            "text": "📈 TradingView",
                            "url": f"https://www.tradingview.com/chart/?symbol=BYBIT:{sig.symbol}.P"
                        },
                    ]
                ]
            }
            reply_markup_json = json.dumps(keyboard)
            
            # Use Outbox (non-blocking, batched delivery)
            alert_hash = f"macro_{sig.symbol}_{int(sig.timestamp)}"
            telegram_outbox.enqueue(
                message,
                priority=MessagePriority.CRITICAL,
                disable_notification=False,
                alert_hash=alert_hash,
                reply_markup=reply_markup_json,
            )
            
            # Cache signal for callback handler (in-memory, lightweight)
            self._cache_signal_for_callback(sig)
            
            self.stats["alerts_sent"] += 1
            logger.info(f"📨 [MacroRadar] Alert + Keyboard dispatched: {sig.symbol} → Telegram")
    
    def _cache_signal_for_callback(self, sig: MacroSignal):
        """
        Store signal in memory for callback handler to retrieve.
        Keeps last 50 signals max (ring buffer eviction).
        """
        if not hasattr(self, '_signal_cache'):
            self._signal_cache: Dict[str, MacroSignal] = {}
        
        self._signal_cache[sig.signal_id] = sig
        
        # Evict old entries if cache grows too large
        if len(self._signal_cache) > 50:
            oldest_key = next(iter(self._signal_cache))
            del self._signal_cache[oldest_key]
    
    def get_cached_signal(self, signal_id: str) -> Optional[MacroSignal]:
        """Retrieve cached signal for callback handler."""
        if not hasattr(self, '_signal_cache'):
            return None
        return self._signal_cache.get(signal_id)
    
    def get_stats(self) -> dict:
        """Returns radar metrics for /stats command."""
        return {
            **self.stats,
            "scan_interval_sec": self.scan_interval,
            "oi_store_symbols": self.oi_store.symbols_tracked,
            "oi_store_warmed": self.oi_store.is_warmed_up,
            "scan_count": self._scan_count,
            "active_cooldowns": len(self._last_alerts),
            "cached_signals": len(getattr(self, '_signal_cache', {})),
        }
