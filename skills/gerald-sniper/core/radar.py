import asyncio
import time
from dataclasses import dataclass
from typing import List, Dict
from loguru import logger
import pandas as pd
import numpy as np

from data.bybit_rest import BybitREST
from data.database import DatabaseManager
from utils.safe_math import safe_float

@dataclass
class CoinRadarMetrics:
    symbol: str
    rvol: float
    delta_oi_4h_pct: float
    atr_pct: float
    funding_rate: float
    direction: str  # 'BULLISH' / 'BEARISH' / 'NEUTRAL'
    volume_24h_usd: float = 0.0

def calculate_radar_score(m: CoinRadarMetrics, config) -> float:
    # Normalize 0-1
    # We use these max values for normalization: RVOL=5.0, OI=25.0, ATR=8.0
    rvol_norm = min(m.rvol / 5.0, 1.0)
    oi_norm = min(m.delta_oi_4h_pct / 25.0, 1.0)
    atr_norm = min(m.atr_pct / 8.0, 1.0)
    
    # Weighted calculation just for Radar sorting
    raw_score = (rvol_norm * 40) + (oi_norm * 35) + (atr_norm * 25)
    
    # Funding penalties
    if m.direction == 'BULLISH' and m.funding_rate > config.radar.funding_penalty_threshold:
        raw_score -= 10
    elif m.direction == 'BULLISH' and m.funding_rate < config.radar.funding_bonus_threshold:
        raw_score += 5
    elif m.direction == 'BEARISH' and m.funding_rate < config.radar.funding_bonus_threshold:
        raw_score -= 10
    elif m.direction == 'BEARISH' and m.funding_rate > config.radar.funding_penalty_threshold:
        raw_score += 5
        
    return max(0.0, min(100.0, raw_score))

class RadarScanner:
    def __init__(self, rest_client: BybitREST, db_manager: DatabaseManager, config):
        self.rest = rest_client
        self.db = db_manager
        self.config = config

    async def _calculate_metrics(self, ticker: dict, macro_block_longs: bool, macro_block_shorts: bool) -> CoinRadarMetrics | None:
        symbol = ticker['symbol']
        
        if symbol in self.config.radar.blacklist:
            return None
            
        try:
            # Basic info from ticker
            volume_24h = safe_float(ticker.get('turnover24h', 0))
            funding_rate = safe_float(ticker.get('fundingRate', 0))
            price_change_24h_pct = safe_float(ticker.get('price24hPcnt', 0)) * 100
            current_price = safe_float(ticker.get('lastPrice', 0))
            
            direction = 'NEUTRAL'
            if price_change_24h_pct > 2.0:
                direction = 'BULLISH'
            elif price_change_24h_pct < -2.0:
                direction = 'BEARISH'

            # Macro Block Check 
            is_exempt = (symbol == self.config.macro_filter.btc_symbol and self.config.macro_filter.btc_exempt_from_macro)
            if not is_exempt:
                if direction == 'BULLISH' and macro_block_longs:
                    return None
                if direction == 'BEARISH' and macro_block_shorts:
                    return None

            klines_d1_task = self.rest.get_klines(symbol, "D", limit=self.config.radar.rvol_lookback_days + 1)
            klines_h1_task = self.rest.get_klines(symbol, "60", limit=self.config.radar.atr_period + 1)
            oi_task = self.rest.get_open_interest(symbol, "1h", limit=self.config.radar.delta_oi_lookback_hours + 1)
            
            klines_d1, klines_h1, oi_data = await asyncio.gather(
                klines_d1_task, klines_h1_task, oi_task
            )

            filters_passed = 0

            # --- RVOL ---
            rvol = 0.0
            if len(klines_d1) >= self.config.radar.rvol_lookback_days:
                past_vols = [safe_float(k[6]) for k in klines_d1[:-1]] 
                sma_vol = np.mean(past_vols[-self.config.radar.rvol_lookback_days:])
                rvol = volume_24h / sma_vol if sma_vol > 0 else 0
                
                is_large_cap = symbol in self.config.radar.large_caps
                min_rvol = self.config.radar.rvol_min_large_caps if is_large_cap else self.config.radar.rvol_min_altcoins
                if rvol >= min_rvol:
                    filters_passed += 1

            # --- ATR ---
            atr_pct = 0.0
            if len(klines_h1) >= self.config.radar.atr_period:
                highs = np.array([safe_float(k[2]) for k in klines_h1[-self.config.radar.atr_period:]])
                lows = np.array([safe_float(k[3]) for k in klines_h1[-self.config.radar.atr_period:]])
                closes = np.array([safe_float(k[4]) for k in klines_h1[-self.config.radar.atr_period:]])
                trs = np.maximum(highs - lows, np.maximum(abs(highs - np.concatenate(([closes[0]], closes[:-1]))), abs(lows - np.concatenate(([closes[0]], closes[:-1])))))
                atr = np.mean(trs)
                atr_pct = (atr / current_price) * 100 if current_price > 0 else 0
                
                if atr_pct >= self.config.radar.atr_min_pct:
                    filters_passed += 1

            # --- Delta OI ---
            delta_oi_4h_pct = 0.0
            if len(oi_data) > self.config.radar.delta_oi_lookback_hours:
                oi_now = safe_float(oi_data[0]['openInterest'])
                idx_ago = min(len(oi_data) - 1, self.config.radar.delta_oi_lookback_hours)
                oi_ago = safe_float(oi_data[idx_ago]['openInterest'])
                delta_oi_4h_pct = ((oi_now - oi_ago) / oi_ago) * 100 if oi_ago > 0 else 0
                
                if delta_oi_4h_pct >= self.config.radar.delta_oi_min_pct:
                    filters_passed += 1
            
            # --- EVALUATE MIN FILTERS ---
            if filters_passed < self.config.radar.min_filters_passed:
                return None

            return CoinRadarMetrics(
                symbol=symbol,
                rvol=rvol,
                delta_oi_4h_pct=delta_oi_4h_pct,
                atr_pct=atr_pct,
                funding_rate=funding_rate,
                direction=direction,
                volume_24h_usd=volume_24h
            )
            
        except Exception as e:
            logger.debug(f"Error calculating metrics for {symbol}: {e}")
            return None

    async def scan(self) -> List[Dict]:
        tickers = await self.rest.get_tickers()
        valid_tickers = [t for t in tickers if t.get('turnover24h') and safe_float(t['turnover24h']) > self.config.radar.min_volume_24h_usd]
        
        # MACRO FILTER LOGIC
        macro_block_longs = False
        macro_block_shorts = False
        
        if self.config.macro_filter.enabled:
            try:
                btc_sym = self.config.macro_filter.btc_symbol
                # Check 4H explicitly to match block conditions
                btc_klines = await self.rest.get_klines(btc_sym, "240", limit=5)
                if len(btc_klines) >= 5:
                    btc_now = safe_float(btc_klines[-1][4]) # current close
                    btc_4h_ago = safe_float(btc_klines[-2][1]) # open of PREVIOUS 4h candle (to catch recent 4h momentum)
                    btc_change_pct = ((btc_now - btc_4h_ago) / btc_4h_ago) * 100
                    
                    if btc_change_pct < -self.config.macro_filter.block_alt_longs_if_btc_drop_4h_pct:
                        macro_block_longs = True
                        logger.warning(f"🚨 MACRO BLOCK: {btc_sym} dropped {btc_change_pct:.2f}% (4h). Blocking ALT LONGS.")
                    elif btc_change_pct > self.config.macro_filter.block_alt_shorts_if_btc_pump_4h_pct:
                        macro_block_shorts = True
                        logger.warning(f"🚨 MACRO BLOCK: {btc_sym} pumped {btc_change_pct:.2f}% (4h). Blocking ALT SHORTS.")
                        
                # Market funding
                valid_tickers_for_funding = sorted(valid_tickers, key=lambda x: safe_float(x.get('turnover24h', 0)), reverse=True)[:20]
                fundings = [safe_float(t.get('fundingRate', 0)) for t in valid_tickers_for_funding if t.get('fundingRate')]
                if fundings:
                    avg_funding = sum(fundings) / len(fundings)
                    if avg_funding > self.config.macro_filter.market_funding_warning_threshold:
                        logger.warning(f"⚠️ MACRO WARNING: Market overheated! Avg top-20 funding: {avg_funding:.4f}")
            except Exception as e:
                logger.error(f"Macro filter error: {e}")

        logger.info(f"Scanning {len(valid_tickers)} liquid tickers...")
        results = []
        batch_size = 10
        for i in range(0, len(valid_tickers), batch_size):
            batch = valid_tickers[i:i+batch_size]
            tasks = [self._calculate_metrics(t, macro_block_longs, macro_block_shorts) for t in batch]
            batch_results = await asyncio.gather(*tasks)
            await asyncio.sleep(0.3)  # Anti-timeout pausing between batches
            
            for res in batch_results:
                if res is not None:
                    score = calculate_radar_score(res, self.config)
                    ticker_data = next(t for t in valid_tickers if t['symbol'] == res.symbol)
                    results.append({
                        "metrics": res,
                        "score": score,
                        "price_change_24h_pct": safe_float(ticker_data.get('price24hPcnt', 0)) * 100
                    })
                    
        results.sort(key=lambda x: x["score"], reverse=True)
        top_results = results[:self.config.radar.watchlist_size]
        
        if top_results:
            await self.db.insert_watchlist_history(top_results)
                
        return top_results
