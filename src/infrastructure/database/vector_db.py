from loguru import logger
import chromadb
from datetime import datetime


class VectorDatabase:
    """
    ChromaDB implementation for RAG, matching the system indexer.
    """

    def __init__(self):
        self.chroma_host = "localhost"
        self.chroma_port = 8000
        self.client = None
        # Enhanced collection fleet
        self.collection_names = [
            "gerald-files",  # Code & general files
            "gerald-knowledge",  # Manual memories
            "trading-strategies",  # Crypto/Trading specific
            "technical-docs",  # RAG for external documentation
            "error-patterns",  # Learned fixes from Self-Improver
        ]

    async def connect(self):
        if self.client:
            return
        try:
            self.client = chromadb.HttpClient(
                host=self.chroma_host, port=self.chroma_port
            )
            logger.info(
                f"Connected to ChromaDB at {self.chroma_host}:{self.chroma_port}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to ChromaDB: {e}")
            raise

    async def add_documents(
        self,
        texts: list[str],
        metadatas: list[dict],
        filepaths: list[str],
        collection_name: str = "gerald-knowledge",
    ):
        await self.connect()
        import asyncio
        loop = asyncio.get_event_loop()
        
        # Run blocking collection retrieval/creation in executor
        collection = await loop.run_in_executor(
            None, lambda: self.client.get_or_create_collection(name=collection_name)
        )

        ids = [f"doc_{datetime.now().timestamp()}_{i}" for i in range(len(texts))]

        # ChromaDB handles embeddings automatically if configured,
        # but here we rely on it using the default or we could pass them.
        # Since the indexer uses default, we do too.
        await loop.run_in_executor(
            None, lambda: collection.add(documents=texts, metadatas=metadatas, ids=ids)
        )
        logger.info(f"Added {len(texts)} docs to Chroma collection: {collection_name}")

    async def search(self, query: str, limit: int = 5):
        await self.connect()
        all_results = []
        import asyncio
        loop = asyncio.get_event_loop()

        for col_name in self.collection_names:
            try:
                # Use get_or_create to avoid initialization errors (blocking)
                collection = await loop.run_in_executor(
                    None, lambda c=col_name: self.client.get_or_create_collection(name=c)
                )
                
                # Blocking query
                results = await loop.run_in_executor(
                    None, lambda: collection.query(query_texts=[query], n_results=limit)
                )

                if results["documents"] and results["documents"][0]:
                    for i in range(len(results["documents"][0])):
                        meta = results["metadatas"][0][i] or {}
                        all_results.append(
                            {
                                "text": results["documents"][0][i],
                                "metadata": meta,
                                "filepath": meta.get(
                                    "filepath", meta.get("source", "unknown")
                                ),
                                "distance": (
                                    results["distances"][0][i]
                                    if "distances" in results
                                    else 0
                                ),
                            }
                        )
            except Exception as e:
                logger.warning(f"Could not search collection {col_name}: {e}")

        # Sort by distance (if available, lower is better)
        all_results.sort(key=lambda x: x.get("distance", 1))
        return all_results[:limit]
