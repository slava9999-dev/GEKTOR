import aiohttp
import asyncio
import sqlite3
import pandas as pd
from loguru import logger
import time
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

class PaperSimulator:
    def __init__(self, db_path="data_run/history.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        logger.info(f"Simulator initialized via {db_path}")

    def load_candles(self, symbol: str) -> pd.DataFrame:
        """Загружает свечи монеты из БД и сортирует по времени."""
        query = f"SELECT * FROM klines WHERE symbol = '{symbol}' ORDER BY timestamp ASC"
        try:
            df = pd.read_sql_query(query, self.conn)
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"Error loading {symbol}: {e}")
            return pd.DataFrame()

    async def run_backtest(self, symbol: str, initial_balance=1000.0):
        """
        Главная функция прогона истории.
        """
        df = self.load_candles(symbol)
        if df.empty:
            logger.error(f"No data for {symbol}")
            return
            
        logger.info(f"🚀 Starting Backtest for {symbol}. {len(df)} candles loaded. Dates: {df['datetime'].iloc[0]} -> {df['datetime'].iloc[-1]}")
        
        balance = initial_balance
        wins = 0
        losses = 0
        
        # Для того, чтобы ИИ мог анализировать или LevelDetector мог искать уровни, 
        # нам нужно "накопить" контекст из первых 160-200 свечей (как требует наш алгоритм).
        
        window_size = 168  # 168 свечей = около недели данных на 15м
        
        for i in range(window_size, len(df)):
            # "Текущее время" симулятора
            current_bar = df.iloc[i]
            # "История" симулятора - все что было ДО текущего момента
            history_slice = df.iloc[i-window_size:i]
            
            # ТУТ БУДЕТ ЛОГИКА:
            # 1. Скармливаем history_slice в LevelDetector
            # 2. Ищем пересечения current_bar с этими уровнями
            # 3. Если есть сетап -> запускаем SuperAnalyst
            # 4. Если ИИ говорит "БЕРЕМ" -> Открываем виртуальную сделку
            
            # Печатаем прогресс каждые 500 свечей, чтобы не спамить
            if i % 500 == 0:
                logger.debug(f"Simulating... reached {current_bar['datetime']} (Balance: {balance:.2f})")
                
        logger.success(f"✅ Simulation for {symbol} finished! Final balance: {balance:.2f} | W:{wins} L:{losses}")

async def main():
    sim = PaperSimulator()
    # Возьмем самую волатильную монету из собранных (например SUI или FARTCOIN), 
    # либо классику BTCUSDT
    await sim.run_backtest("SUIUSDT")

if __name__ == "__main__":
    asyncio.run(main())
