"""
Gerald-SuperBrain: Full File System Indexer for ChromaDB
Scans configured directories, chunks text files, embeds into ChromaDB.
This is the L4 (RAG) layer — the key to "knowing everything on the computer."

Usage:
    python scripts/index_files.py                   # Full scan (configured paths)
    python scripts/index_files.py --path C:\\MyDir   # Scan specific directory
    python scripts/index_files.py --stats            # Show index statistics
    python scripts/index_files.py --clear            # Clear all indexed data
"""

import os
import sys
import json
import hashlib
import argparse
import time
from pathlib import Path
from datetime import datetime
from typing import Generator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Directories to index — all real paths on Slava's computer
SCAN_PATHS = [
    # === Core AI Projects ===
    r"C:\Gerald-superBrain",
    r"C:\NeuroGUARDIAN",
    r"C:\CryptoAgent",
    r"C:\GEKTOR",

    # === Web / Platforms ===
    r"C:\WebdashboardNeuroexpert",
    r"C:\websaitNeuroExpert-master",
    r"C:\arsenal-platform",
    r"C:\arbarea-mobile-app",

    # === Bots & Automation ===
    r"C:\BotiNstsgram",
    r"C:\kontentkombain",
    r"C:\videopublik",
    r"C:\Voiceover",
    r"C:\vk_cover_project",

    # === Trading ===
    r"C:\RUSPANKTREIDER",

    # === Business / Other ===
    r"C:\1 PROEKT",
    r"C:\prorab",
    r"C:\Svoy-Dom-NN-52-main",
    r"C:\infografika marketplase",

    # === Workspaces & Config ===
    r"C:\workspaces",
    r"C:\cline",
    os.path.expanduser(r"~\.openclaw\workspace"),

    # === User folders ===
    os.path.expanduser(r"~\Documents"),
    os.path.expanduser(r"~\Desktop"),
]

# File extensions to index
TEXT_EXTENSIONS = {
    # Code
    ".py", ".js", ".ts", ".jsx", ".tsx", ".mjs", ".cjs",
    ".go", ".rs", ".c", ".cpp", ".h", ".hpp",
    ".java", ".kt", ".scala", ".rb", ".php",
    ".cs", ".fs", ".vb",
    ".sql", ".graphql", ".gql",
    ".sh", ".bash", ".zsh", ".ps1", ".psm1", ".bat", ".cmd",
    # Config
    ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg",
    ".env.example", ".editorconfig",
    ".dockerfile", ".dockerignore",
    # Docs
    ".md", ".txt", ".rst", ".adoc", ".org",
    ".csv", ".tsv",
    # Web
    ".html", ".htm", ".css", ".scss", ".sass", ".less",
    ".xml", ".svg",
    # Data
    ".prisma", ".proto", ".thrift",
    # Logs (selective)
    ".log",
}

# Directories to always skip
SKIP_DIRS = {
    ".git", ".hg", ".svn",
    "node_modules", "__pycache__", ".venv", "venv", "env",
    ".next", ".nuxt", "dist", "build", "target", "out",
    ".tox", ".mypy_cache", ".pytest_cache", ".ruff_cache",
    ".cargo", ".rustup",
    "vendor", "bower_components",
    ".idea", ".vscode",
    "chroma", "chromadb-data",
    "AppData",  # Skip AppData — too much noise
}

# File size limits
MAX_FILE_SIZE_KB = 500       # Skip files > 500 KB
MAX_TOTAL_FILES = 50_000     # Safety cap

# Chunking
CHUNK_SIZE_CHARS = 1500      # ~375 tokens per chunk
CHUNK_OVERLAP_CHARS = 200    # Overlap for context continuity

# ChromaDB
CHROMA_HOST = "localhost"
CHROMA_PORT = 8000
COLLECTION_NAME = "gerald-files"  # Separate from gerald-knowledge

# Index state file (for incremental indexing)
INDEX_STATE_FILE = os.path.join(os.path.dirname(__file__), ".index_state.json")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def file_hash(filepath: str) -> str:
    """Fast hash of file content for change detection."""
    h = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
    except (OSError, PermissionError):
        return ""
    return h.hexdigest()


def chunk_text(text: str, filepath: str) -> Generator[dict, None, None]:
    """Split text into overlapping chunks with metadata."""
    if not text.strip():
        return

    lines = text.split("\n")
    current_chunk = []
    current_size = 0
    chunk_idx = 0
    line_start = 1

    for i, line in enumerate(lines, 1):
        current_chunk.append(line)
        current_size += len(line) + 1

        if current_size >= CHUNK_SIZE_CHARS:
            chunk_text_str = "\n".join(current_chunk)
            yield {
                "text": chunk_text_str,
                "chunk_index": chunk_idx,
                "line_start": line_start,
                "line_end": i,
            }
            chunk_idx += 1

            # Keep overlap
            overlap_lines = []
            overlap_size = 0
            for ol in reversed(current_chunk):
                if overlap_size + len(ol) > CHUNK_OVERLAP_CHARS:
                    break
                overlap_lines.insert(0, ol)
                overlap_size += len(ol) + 1

            current_chunk = overlap_lines
            current_size = overlap_size
            line_start = i - len(overlap_lines) + 1

    # Final chunk
    if current_chunk:
        chunk_text_str = "\n".join(current_chunk)
        if chunk_text_str.strip():
            yield {
                "text": chunk_text_str,
                "chunk_index": chunk_idx,
                "line_start": line_start,
                "line_end": len(lines),
            }


def scan_files(paths: list[str]) -> Generator[str, None, None]:
    """Walk directories and yield indexable file paths."""
    seen = set()
    count = 0

    for base_path in paths:
        base = Path(base_path)
        if not base.exists():
            continue

        for root, dirs, files in os.walk(base):
            # Filter out skip directories (in-place for os.walk)
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]

            for fname in files:
                if count >= MAX_TOTAL_FILES:
                    return

                fpath = os.path.join(root, fname)
                ext = os.path.splitext(fname)[1].lower()

                # Filter
                if ext not in TEXT_EXTENSIONS:
                    continue

                try:
                    size_kb = os.path.getsize(fpath) / 1024
                except OSError:
                    continue

                if size_kb > MAX_FILE_SIZE_KB:
                    continue

                real = os.path.realpath(fpath)
                if real in seen:
                    continue
                seen.add(real)

                yield fpath
                count += 1


def read_file_safe(filepath: str) -> str:
    """Read file with encoding fallback."""
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            with open(filepath, "r", encoding=encoding) as f:
                return f.read()
        except (UnicodeDecodeError, UnicodeError):
            continue
        except (OSError, PermissionError):
            return ""
    return ""


def load_index_state() -> dict:
    """Load previous index state for incremental indexing."""
    if os.path.exists(INDEX_STATE_FILE):
        try:
            with open(INDEX_STATE_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"files": {}, "last_run": None}


def save_index_state(state: dict):
    """Save index state after run."""
    state["last_run"] = datetime.now().isoformat()
    with open(INDEX_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Main Indexer
# ---------------------------------------------------------------------------

def index_to_chroma(paths: list[str] = None, force: bool = False):
    """Main indexing loop: scan → chunk → embed → store in ChromaDB."""
    try:
        import chromadb
    except ImportError:
        print("ERROR: chromadb not installed. Run: pip install chromadb")
        sys.exit(1)

    scan_targets = paths or SCAN_PATHS
    state = load_index_state()

    print("=" * 60)
    print("Gerald-SuperBrain: File System Indexer")
    print("=" * 60)
    print(f"  Scan targets: {len(scan_targets)} paths")
    print(f"  Mode: {'FULL' if force else 'INCREMENTAL'}")
    print(f"  Last run: {state.get('last_run', 'never')}")
    print()

    # Connect to ChromaDB
    client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    try:
        heartbeat = client.heartbeat()
        print(f"  ChromaDB: connected (heartbeat {heartbeat})")
    except Exception as e:
        print(f"  ERROR: ChromaDB unreachable at {CHROMA_HOST}:{CHROMA_PORT} -> {e}")
        sys.exit(1)

    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={
            "description": "Indexed files from local filesystem",
            "created_by": "Gerald-SuperBrain file indexer",
        }
    )
    print(f"  Collection: {COLLECTION_NAME} ({collection.count()} existing docs)")
    print()

    # Scan and index
    stats = {"scanned": 0, "indexed": 0, "skipped": 0, "chunks": 0, "errors": 0}
    batch_ids = []
    batch_docs = []
    batch_metas = []
    BATCH_SIZE = 100

    def flush_batch():
        if batch_ids:
            collection.upsert(ids=batch_ids, documents=batch_docs, metadatas=batch_metas)
            batch_ids.clear()
            batch_docs.clear()
            batch_metas.clear()

    t0 = time.time()

    for fpath in scan_files(scan_targets):
        stats["scanned"] += 1

        # Incremental check
        fhash = file_hash(fpath)
        if not force and fpath in state["files"] and state["files"][fpath] == fhash:
            stats["skipped"] += 1
            continue

        # Read and chunk
        content = read_file_safe(fpath)
        if not content.strip():
            stats["skipped"] += 1
            continue

        file_ext = os.path.splitext(fpath)[1].lower()
        rel_path = fpath  # Full path for findability

        for chunk in chunk_text(content, fpath):
            chunk_id = f"{hashlib.md5(fpath.encode()).hexdigest()}_{chunk['chunk_index']}"

            # Prepend file path context to chunk for better retrieval
            doc_text = f"[File: {rel_path}] (lines {chunk['line_start']}-{chunk['line_end']})\n{chunk['text']}"

            meta = {
                "filepath": fpath,
                "filename": os.path.basename(fpath),
                "extension": file_ext,
                "chunk_index": chunk["chunk_index"],
                "line_start": chunk["line_start"],
                "line_end": chunk["line_end"],
                "file_hash": fhash,
                "indexed_at": datetime.now().isoformat(),
            }

            batch_ids.append(chunk_id)
            batch_docs.append(doc_text)
            batch_metas.append(meta)
            stats["chunks"] += 1

            if len(batch_ids) >= BATCH_SIZE:
                try:
                    flush_batch()
                except Exception as e:
                    stats["errors"] += 1
                    print(f"  ERROR batch upsert: {e}")
                    batch_ids.clear()
                    batch_docs.clear()
                    batch_metas.clear()

        # Update state
        state["files"][fpath] = fhash
        stats["indexed"] += 1

        # Progress
        if stats["indexed"] % 100 == 0:
            print(f"  ... indexed {stats['indexed']} files, {stats['chunks']} chunks")

    # Final flush
    try:
        flush_batch()
    except Exception as e:
        stats["errors"] += 1
        print(f"  ERROR final batch: {e}")

    elapsed = time.time() - t0
    save_index_state(state)

    # Report
    print()
    print("=" * 60)
    print("INDEXING COMPLETE")
    print("=" * 60)
    print(f"  Files scanned:   {stats['scanned']}")
    print(f"  Files indexed:   {stats['indexed']}")
    print(f"  Files skipped:   {stats['skipped']} (unchanged)")
    print(f"  Chunks created:  {stats['chunks']}")
    print(f"  Errors:          {stats['errors']}")
    print(f"  Time:            {elapsed:.1f}s")
    print(f"  Collection size: {collection.count()} total documents")
    print(f"  Index state:     {INDEX_STATE_FILE}")
    print()


def show_stats():
    """Show current index statistics."""
    import chromadb

    client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    print("Gerald-SuperBrain: Index Statistics")
    print("=" * 60)

    try:
        collections = client.list_collections()
        for col in collections:
            c = client.get_collection(col.name)
            print(f"  {col.name}: {c.count()} documents")
    except Exception as e:
        print(f"  ERROR: {e}")

    state = load_index_state()
    print(f"\n  Last indexing run: {state.get('last_run', 'never')}")
    print(f"  Files tracked: {len(state.get('files', {}))}")


def clear_index():
    """Clear the file index collection."""
    import chromadb

    client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    try:
        client.delete_collection(COLLECTION_NAME)
        print(f"Deleted collection: {COLLECTION_NAME}")
    except Exception:
        print(f"Collection {COLLECTION_NAME} not found")

    if os.path.exists(INDEX_STATE_FILE):
        os.remove(INDEX_STATE_FILE)
        print(f"Removed index state: {INDEX_STATE_FILE}")


def search_index(query: str, n_results: int = 5):
    """Search the file index (for testing)."""
    import chromadb

    client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    collection = client.get_collection(COLLECTION_NAME)

    results = collection.query(query_texts=[query], n_results=n_results)

    print(f"Search: '{query}' ({n_results} results)")
    print("-" * 60)

    if results["documents"] and results["documents"][0]:
        for i, (doc, meta, dist) in enumerate(zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0]
        )):
            print(f"\n  [{i+1}] Score: {1 - dist:.3f} | {meta.get('filepath', '?')}")
            print(f"      Lines {meta.get('line_start')}-{meta.get('line_end')}")
            # Show first 200 chars
            preview = doc[:200].replace("\n", " ")
            print(f"      {preview}...")
    else:
        print("  No results found.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gerald-SuperBrain File Indexer")
    parser.add_argument("--path", type=str, help="Scan specific directory")
    parser.add_argument("--force", action="store_true", help="Force full re-index")
    parser.add_argument("--stats", action="store_true", help="Show index statistics")
    parser.add_argument("--clear", action="store_true", help="Clear all indexed data")
    parser.add_argument("--search", type=str, help="Test search query")
    parser.add_argument("--results", type=int, default=5, help="Number of search results")

    args = parser.parse_args()

    if args.stats:
        show_stats()
    elif args.clear:
        clear_index()
    elif args.search:
        search_index(args.search, args.results)
    else:
        paths = [args.path] if args.path else None
        index_to_chroma(paths=paths, force=args.force)
