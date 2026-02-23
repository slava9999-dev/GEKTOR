---
name: vector-knowledge
description: Full ChromaDB RAG integration — store, search, and retrieve knowledge semantically across all Gerald's memory, skills, projects, and conversation history.
version: 1.0.0
author: Gerald-SuperBrain
triggers:
  - "remember this"
  - "search knowledge"
  - "what do you know about"
  - "store in memory"
  - "rag search"
  - "find in knowledge"
---

# Vector Knowledge — ChromaDB RAG Engine

## Purpose

This skill provides Gerald with **persistent semantic memory** via ChromaDB. It enables:

- Storing any text/document as vector embeddings
- Semantic (meaning-based) search across all knowledge
- Automatic knowledge ingestion from conversations
- RAG (Retrieval-Augmented Generation) for informed answers

## Architecture

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
│   Gerald     │────▶│ vector-knowledge│────▶│   ChromaDB   │
│  (Agent)     │◀────│    (Skill)      │◀────│  localhost:8k│
└──────────────┘     └─────────────────┘     └──────────────┘
                              │
                     ┌────────┴────────┐
                     │  Collections:   │
                     │  - conversations│
                     │  - skills-docs  │
                     │  - projects     │
                     │  - web-research │
                     │  - personal     │
                     └─────────────────┘
```

## Configuration

```json
{
  "chroma_url": "http://localhost:8000",
  "chroma_api": "v2",
  "chroma_tenant": "default_tenant",
  "chroma_database": "default_database",
  "persist_path": "~/gerald-memory",
  "default_collection": "gerald-knowledge",
  "embedding_model": "all-MiniLM-L6-v2",
  "max_results": 10,
  "similarity_threshold": 0.7
}
```

## Operations

### Store Knowledge

```
TRIGGER: "remember this", "store in memory"
INPUT: Text content + optional metadata (source, type, tags)
PROCESS:
  1. Chunk text if > 1000 tokens
  2. Generate embeddings
  3. Store in appropriate collection with metadata
  4. Confirm storage with chunk count and collection name
```

### Search Knowledge

```
TRIGGER: "search knowledge", "what do you know about", "rag search"
INPUT: Query string + optional filters (collection, date range, tags)
PROCESS:
  1. Generate query embedding
  2. Search across collections (or specific collection)
  3. Filter by similarity threshold
  4. Return top-K results with source metadata
  5. Synthesize a coherent answer from retrieved chunks
```

### Auto-Ingest

```
TRIGGER: End of significant conversation
PROCESS:
  1. Summarize conversation key points
  2. Extract entities, decisions, code snippets
  3. Store in "conversations" collection with timestamp
  4. Link to related entries in other collections
```

## Collections Schema

### gerald-knowledge (default)

- `content`: The text content
- `metadata.source`: Where it came from (conversation, file, web, manual)
- `metadata.type`: Type (fact, code, decision, instruction, reference)
- `metadata.tags`: Array of tags
- `metadata.timestamp`: When it was stored
- `metadata.relevance_score`: How important (1-10)

### skills-docs

- All skill documentation and usage patterns
- Updated when skills are installed/modified

### projects

- Project-specific knowledge (architectures, decisions, patterns)

### web-research

- Information gathered from web searches and documentation

## Python Integration

```python
import chromadb

# ChromaDB v1.0.0+ uses v2 API internally via HttpClient
client = chromadb.HttpClient(host="localhost", port=8000)

# Collections available:
# gerald-knowledge, conversations, skills-docs, projects, web-research, error-patterns
collection = client.get_or_create_collection("gerald-knowledge")

# Store
collection.add(
    documents=["Important fact about X"],
    metadatas=[{"source": "conversation", "type": "fact"}],
    ids=["unique-id-1"]
)

# Search
results = collection.query(
    query_texts=["What do I know about X?"],
    n_results=5
)

# REST API (v2 format):
# GET http://localhost:8000/api/v2/tenants/default_tenant/databases/default_database/collections
# GET http://localhost:8000/api/v2/heartbeat
# GET http://localhost:8000/api/v2/version
```

## Error Handling

- If ChromaDB is unreachable: Fall back to local file-based search
- If collection doesn't exist: Create it automatically
- If embedding fails: Log error, store as plain text with manual tags
