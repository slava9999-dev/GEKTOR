import os
import time
import asyncio
import psutil
from loguru import logger
from src.shared.config import config
from src.infrastructure.database.vector_db import VectorDatabase

from src.application.services.splitter import SemanticSplitter

class BackgroundIndexer:
    """
    Background file indexer with low OS priority.
    """
    def __init__(self, vector_db: VectorDatabase):
        self.vector_db = vector_db
        self.workspace = config.paths.workspace
        self.is_running = False
        self.splitter = SemanticSplitter(
            chunk_size=config.memory.chunk_size,
            overlap=config.memory.chunk_overlap
        )

    def _set_low_priority(self):
        """Set current process priority to IDLE/LOW."""
        p = psutil.Process(os.getpid())
        if os.name == 'nt':
            p.nice(psutil.IDLE_PRIORITY_CLASS)
        else:
            p.nice(19) # Unix nice value
        logger.info("Indexer priority set to LOW")

    async def scan_and_index(self):
        if self.is_running:
            return
        
        self.is_running = True
        self._set_low_priority()
        
        logger.info(f"Starting background indexing of {self.workspace}")
        
        try:
            # Recursive scan
            for root, dirs, files in os.walk(self.workspace):
                for file in files:
                    if file.endswith(('.py', '.md', '.txt', '.js', '.ts')):
                        file_path = os.path.join(root, file)
                        await self._index_file(file_path)
                        # Minimal sleep to yield control
                        await asyncio.sleep(0.01)
        finally:
            self.is_running = False
            logger.info("Background indexing completed")

    async def _index_file(self, file_path: str):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if not content.strip():
                return

            # TS Requirement: Semantic Splitter logic
            chunks = self.splitter.split(content)
            
            if not chunks:
                return

            metadatas = [{"source": file_path} for _ in chunks]
            filepaths = [file_path for _ in chunks]
            
            await self.vector_db.add_documents(chunks, metadatas, filepaths)
            logger.debug(f"Indexed {len(chunks)} semantic chunks from {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to index {file_path}: {e}")
