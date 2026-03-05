"""Test ChromaDB search functionality."""

import chromadb

client = chromadb.HttpClient(host="localhost", port=8000)
col = client.get_collection("gerald-knowledge")
print(f"Collection: gerald-knowledge, docs: {col.count()}")

# Test semantic search
results = col.query(query_texts=["What model does Gerald use?"], n_results=3)
hits = results.get("ids", [[]])[0] if results.get("ids") else []
print(f"Search results: {len(hits)} hits")
for i in range(len(hits)):
    docs = results.get("documents")
    dists = results.get("distances")
    if docs and dists and docs[0] and dists[0]:
        doc = docs[0][i][:120]
        dist = dists[0][i]
        print(f"  [{i+1}] dist={dist:.4f}: {doc}...")

# Test different query
print()
results2 = col.query(query_texts=["Who is Slava?"], n_results=2)
hits2 = results2.get("ids", [[]])[0] if results2.get("ids") else []
print(f"Search 'Who is Slava?': {len(hits2)} hits")
for i in range(len(hits2)):
    docs2 = results2.get("documents")
    dists2 = results2.get("distances")
    if docs2 and dists2 and docs2[0] and dists2[0]:
        doc = docs2[0][i][:120]
        dist = dists2[0][i]
        print(f"  [{i+1}] dist={dist:.4f}: {doc}...")

# List all collections
print()
print("All collections:")
for c in client.list_collections():
    count = client.get_collection(c.name).count()
    print(f"  {c.name}: {count} docs")
