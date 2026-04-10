import sqlite3
import pandas as pd
import numpy as np
import time
from datetime import datetime, timezone
from loguru import logger
import os
import sys

# Add core to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.radar.volume_spike import calculate_volume_spike, score_volume_spike
from core.radar.momentum import calculate_momentum_pct, score_momentum
from core.radar.volatility import calculate_atr_ratio, score_volatility_expansion
from core.radar.scoring import compute_final_radar_score
from core.radar.utils import calculate_atr

class RadarV2Backtester:
    def __init__(self, db_path="data_run/history.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        logger.info(f"Radar Backtester initialized via {db_path}")

    def load_all_data(self):
        """Loads all klines from DB into a dict of DataFrames."""
        df_all = pd.read_sql_query("SELECT * FROM klines ORDER BY timestamp ASC", self.conn)
        df_all['datetime'] = pd.to_datetime(df_all['timestamp'], unit='ms')
        
        symbols = df_all['symbol'].unique()
        data_map = {}
        for s in symbols:
            data_map[s] = df_all[df_all['symbol'] == s].reset_index(drop=True)
        return data_map

    def run(self, min_score=60):
        data_map = self.load_all_data()
        if not data_map:
            logger.error("No data found in DB.")
            return

        # Get common timestamps across symbols (roughly)
        # We'll just use the timestamps from the first symbol as our timeline
        first_sym = list(data_map.keys())[0]
        timestamps = data_map[first_sym]['timestamp'].unique()
        
        logger.info(f"Starting simulation across {len(data_map)} symbols and {len(timestamps)} time points.")
        
        # Window sizes for calculations (based on 15m candles)
        # 1h = 4 candles
        # 4h = 16 candles
        
        results = []

        # Iterate through timeline
        # Skip the first 20 candles to have some history for averages/ATR
        for i in range(20, len(timestamps)):
            ts = timestamps[i]
            dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
            
            scored_this_moment = []
            
            for symbol, df in data_map.items():
                # Find the index for this timestamp in this symbol
                row_idx = df.index[df['timestamp'] == ts]
                if row_idx.empty: continue
                idx = row_idx[0]
                
                # Check history length
                if idx < 13: continue # Need at least some candles for spike
                
                # 1. Volume Spike (using last 13 candles (~3h))
                vol_history = df['turnover'][idx-12:idx+1].tolist()
                spike = calculate_volume_spike(vol_history)
                s_spike = score_volume_spike(spike)
                
                # 2. Momentum (current candle)
                row = df.iloc[idx]
                mom = calculate_momentum_pct(row['open'], row['close'])
                s_mom = score_momentum(mom)
                
                # 3. Trade Velocity (MOCKED as 1.0 since we lack ticks)
                s_vel = 0 # No extra points for velocity in backtest unless we estimate it
                
                # 4. Volatility (ATR Ratio)
                # atr_5m proxy -> ATR of last 3 candles
                # atr_1h proxy -> ATR of last 20 candles
                closes = df['close'][idx-20:idx+1].values
                highs = df['high'][idx-20:idx+1].values
                lows = df['low'][idx-20:idx+1].values
                
                atr_short = calculate_atr(highs[-3:], lows[-3:], closes[-3:], 1)
                atr_long = calculate_atr(highs, lows, closes, 14)
                
                ratio = calculate_atr_ratio(atr_short, atr_long)
                s_vola = score_volatility_expansion(ratio)
                
                final_score = compute_final_radar_score(s_spike, s_vel, s_mom, s_vola)
                
                if final_score >= min_score:
                    scored_this_moment.append({
                        "symbol": symbol,
                        "score": final_score,
                        "spike": spike,
                        "momentum": mom,
                        "ratio": ratio
                    })

            if scored_this_moment:
                scored_this_moment.sort(key=lambda x: x['score'], reverse=True)
                top = scored_this_moment[0]
                logger.info(f"[{dt}] 🔥 GEM FOUND: {top['symbol']} | Score: {top['score']} | Spike: {top['spike']:.1f}x | Mom: {top['momentum']:.1f}%")
                results.append({"timestamp": ts, "hits": scored_this_moment})

        logger.success(f"Backtest complete. Found hits at {len(results)} time points.")

if __name__ == "__main__":
    tester = RadarV2Backtester()
    tester.run(min_score=60)
