import os
import asyncio
from loguru import logger
import lancedb
import pyarrow as pa
import json
from datetime import datetime
from src.shared.config import config

from sentence_transformers import SentenceTransformer

class VectorDatabase:
    """
    LanceDB implementation with SentenceTransformer embeddings.
    """
    def __init__(self):
        self.db_path = config.memory.vector_store_path
        self.db = None
        self.table_name = "knowledge"
        self._embedder = None

    def _get_embedder(self):
        if self._embedder is None:
            logger.info(f"Loading embedding model: {config.memory.embedding_model}")
            # Keeping on CPU for VRAM safety to not conflict with LLM
            self._embedder = SentenceTransformer(config.memory.embedding_model, device="cpu")
        return self._embedder

    async def connect(self):
        if self.db:
            return
        
        db_dir = os.path.dirname(self.db_path)
        os.makedirs(db_dir, exist_ok=True)
        
        self.db = lancedb.connect(db_dir)
        
        if self.table_name not in self.db.table_names():
            schema = pa.schema([
                pa.field("vector", pa.list_(pa.float32(), 384)),
                pa.field("text", pa.string()),
                pa.field("metadata", pa.string()),
                pa.field("filepath", pa.string()),
                pa.field("timestamp", pa.string())
            ])
            self.db.create_table(self.table_name, schema=schema)

    async def add_documents(self, texts: list[str], metadatas: list[dict], filepaths: list[str]):
        await self.connect()
        embedder = self._get_embedder()
        
        # Batch encode
        vectors = embedder.encode(texts, convert_to_numpy=True).tolist()
        
        data = []
        for i in range(len(texts)):
            data.append({
                "vector": vectors[i],
                "text": texts[i],
                "metadata": json.dumps(metadatas[i]),
                "filepath": filepaths[i],
                "timestamp": datetime.now().isoformat()
            })
        
        table = self.db.open_table(self.table_name)
        table.add(data)

    async def search(self, query: str, limit: int = 5):
        await self.connect()
        embedder = self._get_embedder()
        query_vector = embedder.encode(query).tolist()
        
        table = self.db.open_table(self.table_name)
        results = table.search(query_vector).limit(limit).to_pandas()
        return results.to_dict('records')
