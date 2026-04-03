from loguru import logger
import chromadb
from datetime import datetime


class VectorDatabase:
    """
    ChromaDB implementation for RAG, matching the system indexer.
    """

    def __init__(self):
        self.chroma_host = "localhost"
        self.chroma_port = 8001
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
        """
        Robust connection with healthcheck and exponential backoff retries.
        Task 5.1: Fail-safe vector memory bootstrapper.
        """
        if self.client:
            return

        import os
        from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
        
        # Gektor Rule: Ensure local connections bypass proxies to avoid 502 Bad Gateway
        os.environ["no_proxy"] = "localhost,127.0.0.1"
        os.environ["NO_PROXY"] = "localhost,127.0.0.1"

        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((Exception,)),
            before_sleep=lambda retry_state: logger.warning(
                f"🛡️ [VectorDB] Connection failed. Retrying in {retry_state.next_action.sleep}s... (Attempt {retry_state.attempt_number})"
            )
        )
        def _connect_sync():
            # ChromaDB v0.4+ HttpClient is synchronous by default, or we use the underlying client
            client = chromadb.HttpClient(
                host=self.chroma_host, 
                port=self.chroma_port,
                settings=chromadb.Settings(allow_reset=True, anonymized_telemetry=False)
            )
            # Healthcheck pulse
            client.heartbeat()
            return client

        try:
            import asyncio
            loop = asyncio.get_event_loop()
            self.client = await loop.run_in_executor(None, _connect_sync)
            logger.info(f"✅ [VectorDB] System Memory Link ESTABLISHED: {self.chroma_host}:{self.chroma_port}")
        except Exception as e:
            logger.critical(f"💀 [VectorDB] FATAL: System Memory unreachable after retries. {e}")
            raise

    async def add_documents(
        self,
        texts: list[str],
        metadatas: list[dict],
        filepaths: list[str],
        collection_name: str = "gerald-knowledge",
    ):
        try:
            await self.connect()
        except Exception as e:
            logger.error(f"🛡️ [VectorDB] Indexing Deferred: Memory link offline. {e}")
            return

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
        try:
            await self.connect()
        except Exception as e:
            logger.warning(f"🛡️ [VectorDB] Memory Search Bypassed: link offline. {e}")
            return []

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
