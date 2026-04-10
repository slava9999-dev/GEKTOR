import numpy as np
import asyncio
from concurrent.futures import ProcessPoolExecutor
from loguru import logger
from typing import Optional, Dict, Any, List

class MathOrchestrator:
    """
    [GEKTOR v12.7] Isolated Quantum Math Engine.
    Processes synchronized multi-asset matrices in a separate CPU pool.
    Handles NaN-induced variance and implements Lead-Lag Z-scoring.
    """
    def __init__(self, tickers: List[str], max_workers: int = 2):
        self.tickers = tickers
        # Процессный пул для защиты Event Loop от NumPy вычислений
        self.executor = ProcessPoolExecutor(max_workers=max_workers)
        self.min_valid_samples = 20 # Минимум живых точек в окне 50 баров

    async def compute_lead_lag_async(self, matrix: np.ndarray) -> Optional[Dict[str, Any]]:
        """
        Точка входа: отправка матрицы в изолированный CPU-пул.
        matrix: shape (window_size, num_assets)
        """
        loop = asyncio.get_running_loop()
        try:
            # GEKTOR v12.7: Превращаем в чистый numpy перед передачей (защита от Object Overhead)
            data_to_process = np.copy(matrix)
            
            result = await loop.run_in_executor(
                self.executor, 
                self._vectorized_quant_core, 
                data_to_process
            )
            return result
        except Exception as e:
            logger.error(f"☢️ [MATH ERROR] Сбой квантового ядра: {e}")
            return None

    async def compute_robust_z_score_async(self, symbol: str, current_val: float, history: List[float]) -> float:
        """[GEKTOR v12.7] Robust Z-Score in isolated CPU pool. (No GIL blocking)"""
        loop = asyncio.get_running_loop()
        try:
            history_np = np.array(history, dtype=np.float64)
            result = await loop.run_in_executor(
                self.executor,
                self._compute_robust_z_score_static,
                current_val,
                history_np
            )
            return float(result)
        except Exception as e:
            logger.error(f"☢️ [MATH ERROR] Z-Score failure for {symbol}: {e}")
            return 0.0

    @staticmethod
    def _compute_robust_z_score_static(current_val: float, history: np.ndarray) -> float:
        """Pure math: Median Absolute Deviation (MAD) for process pool."""
        if len(history) < 2: return 0.0
        median = np.median(history)
        mad = np.median(np.abs(history - median))
        if mad == 0:
            std = np.std(history)
            return float((current_val - np.mean(history)) / (std + 1e-9))
        robust_std = mad * 1.4826
        return float((current_val - median) / (robust_std + 1e-9))

    async def analyze_multivector(self, symbol: str, p: List[float], c: List[float], v: List[float], tick_size: float, window: int) -> Dict[str, Any]:
        """[GEKTOR v11.7] Institutional Multi-Vector Analysis (P/C/V)."""
        loop = asyncio.get_running_loop()
        try:
            p_np = np.array(p, dtype=np.float64)
            c_np = np.array(c, dtype=np.float64)
            v_np = np.array(v, dtype=np.float64)
            
            result = await loop.run_in_executor(
                self.executor,
                self._analyze_multivector_static,
                p_np, c_np, v_np, float(tick_size), int(window)
            )
            return result
        except Exception as e:
            logger.error(f"☢️ [MATH ERROR] Multivector failure for {symbol}: {e}")
            return {"div": "NORMAL", "regime": "NORMAL", "vr": 1.0}

    @staticmethod
    def _analyze_multivector_static(p: np.ndarray, c: np.ndarray, v: np.ndarray, tick_size: float, window: int) -> Dict[str, Any]:
        """Institutional-grade regime detection."""
        if len(p) < window or len(c) < window:
            return {"div": "NORMAL", "regime": "NORMAL", "vr": 1.0}

        dp = p[-1] - p[-window]
        dc = v[-1] - v[-window] # Using V as second vector for divergence
        
        div = "NORMAL"
        if dc > 0 and dp < 0: div = "ABSORPTION_BUY"
        elif dc > 0 and dp > 0: div = "AGREEMENT_BUY"
        
        vol_std = np.std(np.diff(p[-window:]))
        vol_ratio = vol_std / (np.std(np.diff(p)) + 1e-9)
        regime = "EXPANSION" if vol_ratio > 1.2 else "COMPRESSION"
        
        return {
            "div": div,
            "regime": regime,
            "vr": float(vol_ratio)
        }

    @staticmethod
    def _vectorized_quant_core(matrix: np.ndarray) -> Optional[Dict[str, Any]]:
        """
        Чистая математическая функция. Выполняется вне основного потока.
        Использует векторизованные операции для работы с "дырявыми" матрицами.
        """
        if matrix.shape[0] < 30:
            return None

        # 1. Считаем валидность (NaN-маска)
        valid_mask = ~np.isnan(matrix)
        valid_counts = np.sum(valid_mask, axis=0)
        
        # Проверка лидера (BTC обычно индекс 0)
        if valid_counts[0] < 30:
            return None

        # 2. Робастный Z-Score (игнорируем NaN при расчете среднего и отклонения)
        # Примечание: nanstd может бросать RuntimeWarning если слишком мало данных
        with np.errstate(divide='ignore', invalid='ignore'):
            means = np.nanmean(matrix, axis=0)
            stds = np.nanstd(matrix, axis=0)
            
            # Разница текущей цены (последний ряд) и среднего
            current_prices = matrix[-1]
            z_scores = (current_prices - means) / (stds + 1e-9)

        # 3. Алгоритм Lead-Lag (Синхронное отклонение)
        btc_z = z_scores[0]
        if np.isnan(btc_z):
            return None

        anomalies = []
        for i in range(1, matrix.shape[1]):
            # Проверяем "информационную плотность" актива
            confidence = float(valid_counts[i] / matrix.shape[0])
            
            if valid_counts[i] < 20: 
                continue # Слишком много NaN -> данные ненадежны
            
            # Spread Z: Насколько сильно альткоин отстал от лидера
            # Если BTC_Z = -4 (обвал), а SOL_Z = -1, значит SOL отстает на 3 сигмы
            spread_z = btc_z - z_scores[i]
            
            if abs(spread_z) > 3.0:
                anomalies.append({
                    "ticker_idx": i,
                    "spread_z": float(spread_z),
                    "confidence": confidence,
                    "score": abs(spread_z) * confidence
                })

        if not anomalies:
            return None

        # Выбираем наиболее значимую аномалию
        best = max(anomalies, key=lambda x: x['score'])
        return best

logger.info("⚛️ [MathOrchestrator] Core v12.7 INITIALIZED (Isolated CPU Mode).")
