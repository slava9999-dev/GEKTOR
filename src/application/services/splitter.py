from typing import List
import re


class SemanticSplitter:
    """
    Advanced text splitter for RAG.
    Attempts to break text into meaningful chunks while preserving context.
    """

    def __init__(self, chunk_size: int = 1500, overlap: int = 200):
        self.chunk_size = chunk_size
        self.overlap = overlap

    def split(self, text: str) -> List[str]:
        if not text:
            return []

        # 1. First-pass: Split by high-level structures (paragraphs)
        paragraphs = re.split(r"\n\s*\n", text)

        chunks = []
        current_chunk = ""

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            # If paragraph itself is too large, split by sentences
            if len(para) > self.chunk_size:
                sentences = re.split(r"(?<=[.!?])\s+", para)
                for sent in sentences:
                    if len(current_chunk) + len(sent) < self.chunk_size:
                        current_chunk += sent + " "
                    else:
                        if current_chunk:
                            chunks.append(current_chunk.strip())
                        # Overlap: take end of previous chunk if possible
                        current_chunk = (
                            current_chunk[-self.overlap :]
                            if len(current_chunk) > self.overlap
                            else ""
                        )
                        current_chunk += sent + " "
            else:
                if len(current_chunk) + len(para) < self.chunk_size:
                    current_chunk += para + "\n\n"
                else:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                    current_chunk = (
                        current_chunk[-self.overlap :]
                        if len(current_chunk) > self.overlap
                        else ""
                    )
                    current_chunk += para + "\n\n"

        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks
