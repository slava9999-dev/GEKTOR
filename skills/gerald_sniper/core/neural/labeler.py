# core/neural/labeler.py
import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Optional, List
import numpy as np
from loguru import logger

class AsyncMetaLabeler:
    """
    [GEKTOR v21.2] Institutional ML Inference Proxy.
    Isolates CPU-bound Random Forest/XGBoost inference into a separate ProcessPool 
    to prevent blocking the asyncio Event Loop.
    """
    def __init__(self, model_path: str, feature_names: List[str], max_workers: int = 2):
        self.model_path = model_path
        self.feature_names = feature_names
        self.executor = ProcessPoolExecutor(max_workers=max_workers)
        self.model: Any = None
        # We don't load here to avoid pickle issues if passed to executor, 
        # but the USER requested loading at start.
        # Actually, in ProcessPool, the model should be loaded INSIDE the process 
        # or passed if picklable. Most sklearn models are picklable.

    def _load_model_sync(self) -> None:
        """Sинхронная загрузка модели (joblib/pickle)."""
        import joblib
        try:
            self.model = joblib.load(self.model_path)
            logger.info(f"🧠 [ML] Model loaded from {self.model_path}")
        except Exception as e:
            logger.critical(f"🧠 [ML] Model load failed: {e}")
            raise

    @staticmethod
    def _predict_sync(model: Any, X: np.ndarray) -> float:
        """
        Pure function for ProcessPool.
        Predicts probability of class 1 (Signal Success).
        """
        try:
            # sklearn: predict_proba returns [ [prob_0, prob_1], ... ]
            probabilities = model.predict_proba(X)
            return float(probabilities[0][1])
        except Exception as e:
            # We can't log to loguru here easily if it's a separate process without config
            return -1.0 

    async def get_probability(self, features: Dict[str, float]) -> Optional[float]:
        """
        Async inference entry point. 
        Converts Dict to ordered NumPy array and dispatches to ProcessPool.
        """
        if self.model is None:
            # Fallback for dev: if no model, return neutral probability
            return 0.5 

        try:
            # 1. Order features according to training schema (Critical!)
            vector = [features.get(name, 0.0) for name in self.feature_names]
            X = np.array([vector])

            # 2. Dispatch to ProcessPool
            loop = asyncio.get_running_loop()
            probability = await loop.run_in_executor(
                self.executor,
                self._predict_sync,
                self.model,
                X
            )

            if probability < 0:
                logger.error("🧠 [ML] Inference internal failure in ProcessPool.")
                return None

            return probability
        except Exception as e:
            logger.error(f"🧠 [ML] Async inference failed: {e}")
            return None

    def shutdown(self):
        """Graceful shutdown of the execution pool."""
        self.executor.shutdown(wait=True)
        logger.info("🧠 [ML] Inference pool shut down.")
