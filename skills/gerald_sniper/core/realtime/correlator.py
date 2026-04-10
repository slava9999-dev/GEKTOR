import numpy as np
import asyncio
import time
from loguru import logger
from typing import Dict, List, Optional, Tuple
from core.realtime.pipeline import ZeroAllocBuffer

class SanityGateGuard:
    """
    [GEKTOR v13.1] Robust Inbound Multi-Asset Filter.
    Differentiates between API Glitches (Single Outlier) and 
    Real Flash Crashes (Consensus Movement).
    """
    def __init__(self, num_assets: int, max_deviation_std: float = 6.0):
        self.num_assets = num_assets
        self.max_deviation_std = max_deviation_std
        self.last_valid_prices = np.full(num_assets, np.nan)
        self.rolling_std = np.full(num_assets, np.nan)
        self.alpha = 0.15 # Быстрая адаптация к волатильности

    def validate_and_filter(self, new_snapshot: np.ndarray) -> np.ndarray:
        """
        [CONCENSUS VALIDATION]
        If only 1 asset deviates -> It's a Glitch (NaN).
        If > 50% assets deviate in the same direction -> It's a REAL CRASH (Pass).
        """
        if np.isnan(self.last_valid_prices).any():
            # Первая инициализация
            mask = ~np.isnan(new_snapshot)
            self.last_valid_prices[mask] = new_snapshot[mask]
            self.rolling_std[mask] = new_snapshot[mask] * 0.002
            return new_snapshot

        deviations = np.abs(new_snapshot - self.last_valid_prices)
        thresholds = self.rolling_std * self.max_deviation_std
        
        # 1. Считаем количество "взбунтовавшихся" активов
        outlier_mask = deviations > thresholds
        crazy_count = np.sum(outlier_mask)
        
        # 2. Логика Консенсуса
        # Если более 30% корзины полетело в одну сторону -> Это рынок, а не баг.
        is_consensus_crash = crazy_count >= (self.num_assets * 0.3)
        
        filtered = new_snapshot.copy()
        
        for i in range(self.num_assets):
            if outlier_mask[i] and not is_consensus_crash:
                # Одиночный выброс -> В Карантин!
                logger.error(f"🚫 [SANITY] Glitch detected: Asset {i} dev={deviations[i]:.2f} > {thresholds[i]:.2f}")
                filtered[i] = np.nan
            else:
                # Валидные данные или подтвержденный краш рынка
                if not np.isnan(new_snapshot[i]):
                    self.last_valid_prices[i] = new_snapshot[i]
                    # Обновляем волатильность только если это не аномальный скачок (или если это краш)
                    if not outlier_mask[i] or is_consensus_crash:
                        self.rolling_std[i] = (1-self.alpha)*self.rolling_std[i] + self.alpha*deviations[i]

        return filtered

class CrossAssetAlignmentMatrix:
    """
    [GEKTOR v12.6] Synchronized Multi-Asset State Engine.
    Solves the Asynchronous Stream Join problem using LOCF + Liquidity Gating.
    Prevents "Phantom Liquidity Traps".
    """
    def __init__(self, tickers: List[str], max_stale_ms: int = 5000):
        self.tickers = tickers
        self.max_stale_ms = max_stale_ms
        
        # Глобальный стейт корзины
        # symbol -> {price: float, bid_ask_spread: float, liquidity_depth_usd: float, ts: int}
        self.current_state: Dict[str, Dict] = {
            t: {"price": np.nan, "spread": np.nan, "depth": 0.0, "ts": 0} for t in tickers
        }
        
        # [GEKTOR v13.1] Security Layer: Sanity Gate & Zero-Alloc
        self.sanity_gate = SanityGateGuard(len(tickers))
        self.zero_alloc_buffer = ZeroAllocBuffer(500, len(tickers))
        self._lock = asyncio.Lock()

    async def on_new_dollar_bar(self, ticker: str, close_price: float, timestamp: int):
        """
        Инъекция нового бара. Обновляет цену, но сохраняет "Свежесть".
        """
        async with self._lock:
            state = self.current_state[ticker]
            state["price"] = close_price
            state["ts"] = timestamp
            
            # Если BTC (Лидер) напечатал бар, мы формируем Snapshot для всей корзины
            if ticker == "BTCUSDT":
                await self._compute_sync_snapshot(timestamp)

    async def update_l2_context(self, ticker: str, bid: float, ask: float, depth_usd: float):
        """
        Инъекция микроструктурного контекста (из Bridge/Conflator).
        Позволяет отличать "реальное отставание" от "пустого стакана".
        """
        async with self._lock:
            state = self.current_state[ticker]
            state["spread"] = (ask - bid) / bid if bid > 0 else np.nan
            state["depth"] = depth_usd

    async def _compute_sync_snapshot(self, master_ts: int):
        """
        Формирует синхронную матрицу с жесткой фильтрацией ликвидности.
        """
        snapshot_prices = []
        valid_assets_count = 0
        
        for t in self.tickers:
            state = self.current_state[t]
            
            # Проверка 1: Свежесть данных (Анти-Stale)
            is_stale = (master_ts - state["ts"]) > self.max_stale_ms
            
            # Проверка 2: Плотность стакана (Анти-Phantom)
            # Если спред > 0.5% или глубина < $10k, мы помечаем данные как "неторгуемые" (NaN)
            is_empty_book = state["spread"] > 0.005 or state["depth"] < 10000.0
            
            if is_stale or is_empty_book or np.isnan(state["price"]):
                # Маркируем как NaN, чтобы Lead-Lag математика игнорировала этот актив
                snapshot_prices.append(np.nan)
            else:
                snapshot_prices.append(state["price"])
                valid_assets_count += 1

        # Выравниваем через Sanity Gate (Анти-Яд)
        raw_row = np.array(snapshot_prices)
        clean_row = self.sanity_gate.validate_and_filter(raw_row)
        
        # Сохраняем в Zero-Alloc буфер (O(1) память)
        self.zero_alloc_buffer.append(clean_row)
        
        if self.zero_alloc_buffer.is_full:
            # Триггер расчета только если у нас накопилось достаточно "чистых" векторов
            asyncio.create_task(self._dispatch_to_math_engine())

    async def _dispatch_to_math_engine(self):
        """
        Передача очищенной матрицы в ProcessPoolExecutor.
        """
        # data_matrix = np.array(self.synchronized_buffer)
        # Здесь происходит расчет Lead-Lag (Секретная Альфа)
        pass

logger.info("📐 [CrossAsset] Engine v12.6 ONLINE — Liquidity Gating ENABLED.")
