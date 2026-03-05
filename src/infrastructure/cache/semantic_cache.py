import lancedb
import os
import pyarrow as pa
from datetime import datetime
from typing import Optional
from loguru import logger
from sentence_transformers import SentenceTransformer


class SemanticCache:
    """Semantic caching layer to reduce LLM load for repeated queries."""

    def __init__(self, db_path: str = "memory/cache.lance", threshold: float = 0.92):
        self.db_path = db_path
        self.threshold = threshold
        self.db = None
        self.table_name = "query_cache"
        self._embedder = None
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def _get_embedder(self):
        if self._embedder is None:
            import torch
            device = "cuda" if torch.cuda.is_available() else "cpu"
            # Reusing the same multilingual model as VectorDB for consistency
            model_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
            logger.info(f"Loading Semantic Cache Embedder '{model_name}' on {device.upper()}...")
            self._embedder = SentenceTransformer(model_name, device=device)
        return self._embedder

    def connect(self):
        if self.db:
            return
        self.db = lancedb.connect(os.path.dirname(self.db_path))
        tables = self.db.list_tables()
        if self.table_name not in tables:
            schema = pa.schema(
                [
                    pa.field("vector", pa.list_(pa.float32(), 384)),
                    pa.field("query", pa.string()),
                    pa.field("response", pa.string()),
                    pa.field("timestamp", pa.string()),
                ]
            )
            try:
                self.db.create_table(self.table_name, schema=schema)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise e

    def get(self, query: str) -> Optional[str]:
        try:
            self.connect()
            embedder = self._get_embedder()
            query_vector = embedder.encode(query).tolist()

            if self.db is None:
                return None
            table = self.db.open_table(self.table_name)
            # Use vector search to find similar previous queries
            results = table.search(query_vector).limit(1).to_pandas()

            if not results.empty:
                # LanceDB search returns distance, here we check similarity
                # For L2 distance, a lower value is better.
                # Roughly 'threshold' check depends on distance metric.
                # Assuming default metric is L2.
                distance = results.iloc[0]["_distance"]
                if distance < 0.15:  # Very strict similarity (~0.92+ cosine)
                    logger.info(f"Semantic Cache Hit (dist: {distance:.4f})")
                    return results.iloc[0]["response"]

            return None
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None

    def set(self, query: str, response: str):
        try:
            self.connect()
            embedder = self._get_embedder()
            vector = embedder.encode(query).tolist()

            if self.db is None:
                return
            table = self.db.open_table(self.table_name)
            table.add(
                [
                    {
                        "vector": vector,
                        "query": query,
                        "response": response,
                        "timestamp": datetime.now().isoformat(),
                    }
                ]
            )
        except Exception as e:
            logger.error(f"Cache set error: {e}")
