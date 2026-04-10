# skills/gerald_sniper/core/realtime/inference.py
import asyncio
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from loguru import logger
import joblib
import os
from typing import Optional

# [GEKTOR v21.49] HARDENED ASYNCHRONOUS INFERENCE BOUNDARY
# Глобальная переменная для дочернего процесса (Isolating CPU from I/O)
_worker_model = None

def _init_worker(model_path: str):
    """
    Worker Initialization: Вызывается один раз при создании процесса в пуле.
    Загружает модель в локальную память ядра CPU.
    """
    global _worker_model
    try:
        if not os.path.exists(model_path):
            logger.error(f"Worker Error: Model file {model_path} not found.")
            return
        _worker_model = joblib.load(model_path)
        logger.debug(f"🧠 [Inference Worker] Model pre-warmed in memory.")
    except Exception as e:
        logger.critical(f"❌ [Inference Worker] Boot failure: {e}")
        raise

def _fast_predict(features_array: np.ndarray) -> float:
    """
    Hot-Path Inference: Никакого I/O. Только RAM и CPU.
    """
    global _worker_model
    if _worker_model is None:
        return 0.0
    try:
        # Извлекаем вероятность успеха (Класс 1: PROFIT)
        probs = _worker_model.predict_proba(features_array.reshape(1, -1))
        return float(probs[0][1])
    except Exception as e:
        logger.error(f"Inference Execution Error: {e}")
        return 0.0

class MetaLabelingInferenceNode:
    """
    Узел Инференса высокой готовности.
    - Изоляция процессов для обхода GIL.
    - Однократная загрузка модели (Worker Pre-warming).
    - Предохранитель (Circuit Breaker) для защиты от Concept Drift.
    """
    def __init__(self, model_path: str = "models/meta_label_v1.joblib", max_workers: int = 2):
        self.model_path = model_path
        self._pool = ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=_init_worker,
            initargs=(model_path,)
        )
        self.is_quarantined = False # Режим FLAT & HALT
        logger.info(f"🧠 [Inference] Boundary established with {max_workers} pre-warmed workers.")

    async def score_signal(self, features: list) -> float:
        """
        Межпроцессный скоринг через асинхронный экзекутор.
        """
        if self.is_quarantined:
            logger.warning("🚨 [Inference] Rejecting scoring: Circuit Breaker ACTIVE.")
            return 0.0

        try:
            loop = asyncio.get_running_loop()
            # Передаем только легковесный numpy array (Minimize IPC Overhead)
            features_np = np.array(features, dtype=np.float32)
            
            probability = await loop.run_in_executor(
                self._pool,
                _fast_predict,
                features_np
            )
            return probability
        except Exception as e:
            logger.error(f"❌ [Inference] IPC/Scoring Failure: {e}")
            return 0.0

class DriftSentinel:
    """
    Контроллер деградации ИИ. 
    Реализует протокол FLAT & HALT при потере мат. ожидания.
    """
    def __init__(self, inference_node: MetaLabelingInferenceNode, max_brier_score: float = 0.35):
        self.node = inference_node
        self.max_brier = max_brier_score
        self.history_size = 50
        self.predictions = []
        self.outcomes = []

    def evaluate(self, pred: float, actual: float):
        self.predictions.append(pred)
        self.outcomes.append(actual)
        
        if len(self.predictions) >= self.history_size:
            self.predictions.pop(0)
            self.outcomes.pop(0)
            
            brier = np.mean((np.array(self.predictions) - np.array(self.outcomes))**2)
            
            if brier > self.max_brier:
                logger.critical(f"🛑 [Sentinel] CONCEPT DRIFT DETECTED! Brier: {brier:.3f}")
                logger.critical("🛑 [Sentinel] PROTOCOL: FLAT & HALT. Neutralizing ML Strategy.")
                self.node.is_quarantined = True
                return True
        return False
