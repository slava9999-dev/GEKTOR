import asyncio
import time
import numpy as np
from typing import List, Dict, Optional, Any
from loguru import logger
from concurrent.futures import ProcessPoolExecutor

from data.bybit_rest import BybitREST
from data.database import DatabaseManager
from .models import RadarV2Metrics
from .utils import safe_float, calculate_atr
from .liquidity_filter import is_liquid
from .volume_spike import calculate_volume_spike, score_volume_spike
from .trade_velocity import calculate_trade_velocity, score_trade_velocity
from .momentum import calculate_momentum_pct, score_momentum
from .volatility import calculate_atr_ratio, score_volatility_expansion
from .scoring import compute_final_radar_score
from .funding_tracker import funding_tracker
from collections import deque
from utils.math_utils import log_throttler

def _compute_metrics_cpu(data: dict) -> Dict[str, Any]:
    """
    Pure CPU-bound math isolated for ProcessPoolExecutor (Audit 25.3).
    Avoids pickling heavy objects by using raw dicts.
    """
    try:
        symbol = data['symbol']
        ticker = data['ticker']
        k5 = data['k5']
        atr_1h = data['atr_1h']
        t1m = data['t1m']
        t10m = data['t10m']
        orderflow = data['orderflow']
        vol_24h = data['vol_24h']
        price = data['price']

        # 1. Volume Acceleration & Momentum
        t5_history = [float(k[6]) for k in k5]
        volume_spike = calculate_volume_spike(t5_history)
        momentum = calculate_momentum_pct(float(k5[-1][1]), float(k5[-1][4]))

        # 2. Velocity
        velocity = calculate_trade_velocity(t1m, t10m / 10.0) if t10m > 0 else 1.0

        # 3. Volatility
        atr_5m = calculate_atr(
            np.array([float(k[2]) for k in k5]),
            np.array([float(k[3]) for k in k5]),
            np.array([float(k[4]) for k in k5]), 1
        )
        atr_ratio = calculate_atr_ratio(atr_5m, atr_1h)

        # 4. Scoring
        s_spike = score_volume_spike(volume_spike)
        s_vel = score_trade_velocity(velocity)
        s_mom = score_momentum(momentum)
        s_vola = score_volatility_expansion(atr_ratio)
        s_order = min(6.0, (orderflow - 1.0) * 4.0) if orderflow > 1.0 else 0.0
        
        final_score = compute_final_radar_score(s_spike, s_vel, s_mom, s_vola, s_order)

        # Liquidity Tiering
        tier = "D"
        if vol_24h > 100_000_000: tier = "A"
        elif vol_24h > 30_000_000: tier = "B"
        elif vol_24h > 10_000_000: tier = "C"

        return {
            "symbol": symbol, "price": price, "volume_24h": vol_24h,
            "funding_rate": float(ticker.get('fundingRate', 0)),
            "volume_spike": float(volume_spike),
            "velocity": float(velocity),
            "momentum_pct": float(momentum),
            "atr_ratio": float(atr_ratio),
            "orderflow_imbalance": float(orderflow),
            "volume_spike_score": s_spike,
            "velocity_score": s_vel,
            "momentum_score": s_mom,
            "volatility_score": s_vola,
            "orderflow_score": s_order,
            "final_score": final_score,
            "liquidity_tier": tier
        }
    except Exception as e:
        return {"error": str(e), "symbol": data.get('symbol')}

class RadarScorer:
    """Stabilizes radar scores using historical percentiles (Rule 2.1)"""
    def __init__(self, history_window: int = 5):
        self._history: deque[dict] = deque(maxlen=history_window)
        self._warmup_cycles = 0
        self._min_warmup = 2

    def normalize_scores(self, raw_results: List[Dict]) -> List[Dict]:
        """
        Gerald v5.1 Rank-based Normalization (T-06):
        Assigns scores based on rank to ensure even distribution.
        """
        if not raw_results: return []

        # 1. Sort by raw score (from Scoring engine)
        raw_results.sort(key=lambda x: x["score"], reverse=True)
        
        # 2. Assign scores based on rank: Top=100, then 98, 96, etc. (Less aggressive T-RADAR fix)
        for i, r in enumerate(raw_results):
            # Cap at 20 to avoid total irrelevance
            norm = max(20, 100 - (i * 2)) 
            r["score"] = norm
            if "metrics" in r:
                r["metrics"].final_score = norm
            
        return raw_results

# Global Process Pool to avoid zombie leakage on scanner reset
_RADAR_EXECUTOR = None

def shutdown_radar_executor():
    global _RADAR_EXECUTOR
    if _RADAR_EXECUTOR:
        logger.warning("🛑 [Radar] Shutting down ProcessPoolExecutor...")
        _RADAR_EXECUTOR.shutdown(wait=True)
        _RADAR_EXECUTOR = None

class RadarV2Scanner:
    def __init__(self, rest: BybitREST, db: DatabaseManager, config):
        self.rest = rest
        self.db = db
        self.config = config
        self.sem = asyncio.Semaphore(15)  # Enforced Semaphore(15) from T-25
        self._h1_atr_cache = {}
        self.scorer = RadarScorer()
        
        global _RADAR_EXECUTOR
        if _RADAR_EXECUTOR is None:
             _RADAR_EXECUTOR = ProcessPoolExecutor(max_workers=4)
        self.executor = _RADAR_EXECUTOR

    async def scan(self) -> List[Dict]:
        logger.info("📡 [Radar v2] Сканирование рынка (180+ тикеров)...")
        start_time = time.monotonic()
        
        t0 = time.monotonic()
        tickers = await self.rest.get_tickers()
        t_tickers = time.monotonic() - t0
        
        if not tickers: return []

        # P0 Filter (Universe)
        candidates = []
        for t in tickers:
            symbol = t['symbol']
            if symbol in getattr(self.config.radar, 'blacklist', []): continue
            
            vol_24h = safe_float(t.get('turnover24h', 0))
            price = safe_float(t.get('lastPrice', 0))
            
            if vol_24h >= 5_000_000 and price >= 0.01:  # v4.2: Tier D support ($5M+)
                candidates.append(t)
                
        # v4.2: Hard cap to Top 80 to strictly enforce Request Budget
        candidates.sort(key=lambda t: safe_float(t.get('turnover24h', 0)), reverse=True)
        candidates = candidates[:80]
        t_candidates = time.monotonic() - (t0 + t_tickers)

        tasks = [self._process_symbol(t) for t in candidates]
        results = await asyncio.gather(*tasks)
        t_processing = time.monotonic() - (t0 + t_tickers + t_candidates)
        
        # P1 Liquidity Filter (>10M)
        valid_results = [r for r in results if r and is_liquid(r.volume_24h)]
        
        output = []
        for m in valid_results:
            output.append({
                "metrics": m,
                "score": m.final_score,
                "symbol": m.symbol
            })

        output.sort(key=lambda x: x["score"], reverse=True)
        
        # 3. Stabilize Scores (Rule 2.1)
        output = self.scorer.normalize_scores(output)
        output.sort(key=lambda x: x["score"], reverse=True)
        
        # Логирование "Гемов" (ARIA, NAORIS, FLOW и т.д.)
        for x in output[:15]: # Expanded to 15
            score = x['score']
            if score >= 50:
                m = x['metrics']
                # Task: "Режим Тишины" - level is INFO only for score >= 90
                log_level = "INFO" if score >= 90 else "DEBUG"
                getattr(logger, log_level.lower())(
                    f"💎 RADAR | {x['symbol']} | "
                    f"score={score} | "
                    f"spike={m.volume_spike:.1f}(s={m.volume_spike_score:.1f}) | "
                    f"vel={m.velocity:.1f}(s={m.velocity_score:.1f}) | "
                    f"mom={m.momentum_pct:.1f}% | "
                    f"vola={m.atr_ratio:.2f} | "
                    f"imbalance={m.orderflow_imbalance:.2f}"
                )

        elapsed = time.monotonic() - start_time
        logger.info(
            f"📡 [Radar v2] Найдено {len(output)} активных монет за {elapsed:.2f}с. "
            f"(Tickers: {t_tickers:.2f}s, Filter: {t_candidates:.2f}s, Process: {t_processing:.2f}s)"
        )
        
        # v10/10: Publish Metrics
        try:
            from core.events.nerve_center import bus
            from core.metrics.metrics import RadarScanEvent
            asyncio.create_task(bus.publish(RadarScanEvent(
                elapsed=elapsed,
                found_count=len(output),
                symbols=[x['symbol'] for x in output[:10]]
            )))
        except Exception:
            pass

        if output:
            await self.db.insert_watchlist_history(output[:20])

        return output

    async def get_priority_universe(self, limit=100) -> List[str]:
        """Returns top N symbols by 24h turnover for real-time trade monitoring."""
        tickers = await self.rest.get_tickers()
        if not tickers: return []
        
        # Sort by turnover24h
        valid = [t for t in tickers if 'turnover24h' in t]
        valid.sort(key=lambda t: safe_float(t.get('turnover24h', 0)), reverse=True)
        
        return [t['symbol'] for t in valid[:limit]]

    async def _process_symbol(self, ticker: dict) -> Optional[RadarV2Metrics]:
        symbol = ticker['symbol']
        vol_24h = safe_float(ticker.get('turnover24h', 0))
        price = safe_float(ticker.get('lastPrice', 0))
        
        async with self.sem:
            try:
                now_ts = time.time()
                cached_h1 = self._h1_atr_cache.get(symbol)
                
                from core.realtime.market_state import market_state
                state = market_state.get_state(symbol)
                
                k5_task = self.rest.get_klines(symbol, "5", limit=13)
                
                if cached_h1 and (now_ts - cached_h1['ts'] < 1800):
                    k5 = await k5_task
                    atr_1h = cached_h1['atr']
                else:
                    k1h_task = self.rest.get_klines(symbol, "60", limit=25)
                    k5, k1h = await asyncio.gather(k5_task, k1h_task)
                    
                    if len(k1h) >= 2:
                        atr_1h = calculate_atr(
                            np.array([safe_float(k[2]) for k in k1h]),
                            np.array([safe_float(k[3]) for k in k1h]),
                            np.array([safe_float(k[4]) for k in k1h]), 14
                        )
                        self._h1_atr_cache[symbol] = {'atr': atr_1h, 'ts': now_ts}
                    else:
                        atr_1h = 0.0001
                
                if len(k5) < 2: return None
                
                # Context gathering for ProcessPool
                t1m = state.get_trade_count(60) if state else 0
                t10m = state.get_trade_count(600) if state else 0
                orderflow = state.get_orderflow_imbalance(60) if state else 1.0

                # OFFLOAD CPU-bound math to ProcessPool (Audit 25.3)
                loop = asyncio.get_event_loop()
                math_input = {
                    "symbol": symbol, "ticker": ticker, "k5": k5, "atr_1h": atr_1h,
                    "t1m": t1m, "t10m": t10m, "orderflow": orderflow,
                    "vol_24h": vol_24h, "price": price
                }
                
                res = await loop.run_in_executor(self.executor, _compute_metrics_cpu, math_input)
                
                if "error" in res:
                    return None

                # [GEKTOR v21.15] Feed FundingRateTracker ring buffer BEFORE return
                if 'funding_rate' in res:
                    funding_tracker.record(symbol, res['funding_rate'])

                return RadarV2Metrics(**res, timestamp=time.time())

            except Exception as e:
                if log_throttler.should_log(f"radar_sym_{symbol}", 120):
                    logger.exception(f"Radar scan critical fail for {symbol}: {e}")
                return None
