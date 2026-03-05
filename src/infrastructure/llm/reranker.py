from typing import List
from sentence_transformers import CrossEncoder
from src.shared.config import config
from src.shared.logger import logger


class Reranker:
    """
    Reranker using Cross-Encoder models.
    Follows TS v2.0: Used only at the moment of answer for Top-3 filtering.
    """

    def __init__(self):
        reranker_cfg = config.models.get("reranker")
        self.model_path = (
            reranker_cfg.path
            if reranker_cfg
            else "cross-encoder/ms-marco-MiniLM-L-6-v2"
        )
        self._model = None

    def _get_model(self):
        if self._model is None:
            import torch
            device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"Loading reranker model: '{self.model_path}' on {device.upper()}...")
            self._model = CrossEncoder(self.model_path, device=device)
        return self._model

    def rerank(self, query: str, documents: List[dict], top_n: int = 3) -> List[dict]:
        if not documents:
            return []

        model = self._get_model()

        # Prepare pairs for cross-encoder
        pairs = [[query, doc["text"]] for doc in documents]

        # Get scores
        scores = model.predict(pairs)

        # Add scores to documents and sort
        for i, doc in enumerate(documents):
            doc["rerank_score"] = float(scores[i])

        ranked_docs = sorted(documents, key=lambda x: x["rerank_score"], reverse=True)
        return ranked_docs[:top_n]
