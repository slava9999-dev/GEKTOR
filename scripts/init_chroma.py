"""
Gerald-SuperBrain: ChromaDB Collection Initializer
Creates all required collections in ChromaDB for RAG functionality.
"""

import chromadb
import sys
from datetime import datetime


def init_collections():
    """Initialize all Gerald-SuperBrain ChromaDB collections."""
    try:
        client = chromadb.HttpClient(host="localhost", port=8000)

        # Verify connection
        heartbeat = client.heartbeat()
        print(f"✅ ChromaDB connected (heartbeat: {heartbeat})")

        # Define collections with metadata for V2.1
        collections_config = {
            "gerald-knowledge": "Default knowledge base — facts, insights, core memory",
            "gerald-files": "Automatically indexed project files (Code/Logs/Docs)",
            "trading-strategies": "Crypto/DeFi trading patterns and quantitative logic",
            "technical-docs": "Technical documentation (API refs, SDK guides, Blockchains)",
            "error-patterns": "Learned error fixes and system patterns for self-improvement",
            "web-research": "Data snippets captured from the internet during sessions",
        }

        created = 0
        existing = 0

        for name, description in collections_config.items():
            try:
                collection = client.get_or_create_collection(
                    name=name,
                    metadata={
                        "description": description,
                        "created_by": "Gerald-SuperBrain",
                        "created_at": datetime.now().isoformat(),
                    },
                )
                count = collection.count()
                if count == 0:
                    print(f"  ✅ Created collection: {name}")
                    created += 1
                else:
                    print(f"  📦 Existing collection: {name} ({count} documents)")
                    existing += 1
            except Exception as e:
                print(f"  ❌ Failed to create {name}: {e}")

        print(f"\n📊 Summary: {created} created, {existing} existing")

        # Seed initial knowledge
        knowledge = client.get_or_create_collection("gerald-knowledge")

        seed_docs = [
            {
                "id": "gerald-identity",
                "document": "Gerald is a personal JARVIS 2026 AI assistant owned by Вячеслав (Slava). "
                "Gerald runs on OpenClaw with Ollama local models. Primary model: qwen2.5:7b "
                "(32k context, num_gpu=35). Fallbacks: mistral:latest (7B), qwen2.5-coder:14b (heavy code only). "
                "Gerald uses advanced reasoning: CoT, ToT, self-critique, multi-step planning. "
                "Optimized for RTX 4070 8GB VRAM. RAM: 16GB.",
                "metadata": {
                    "source": "setup",
                    "type": "identity",
                    "tags": "gerald,identity,core",
                },
            },
            {
                "id": "gerald-architecture",
                "document": "Gerald-SuperBrain architecture: OpenClaw agent framework → Ollama for local LLM inference → "
                "ChromaDB for vector memory (RAG). Collections: gerald-knowledge (core facts), "
                "gerald-files (26k+ chunks from 4200+ indexed files across all projects). "
                "Skills: skill-master, vector-knowledge, model-manager, "
                "antigravity-bridge, self-improver, reasoning-engine. "
                "Bridge Daemon (bridge/bridge_daemon.py) for Antigravity connectivity.",
                "metadata": {
                    "source": "setup",
                    "type": "architecture",
                    "tags": "architecture,skills,system",
                },
            },
            {
                "id": "gerald-models",
                "document": "Model fleet (RTX 4070, 8GB VRAM, 16GB RAM): "
                "1) qwen2.5:7b — PRIMARY. Russian + tool-calling. "
                "Params: num_ctx=32768, num_gpu=35, temp=0.7. "
                "2) mistral:latest (7B) — fallback, English only. num_ctx=8192. "
                "3) qwen2.5-coder:14b — heavy code ONLY (causes tool-loop as primary!). "
                "4) mistral-nemo (12B) — DO NOT LOAD (CUDA OOM). "
                "Thermal limit: 80C GPU, auto-unload 20min idle.",
                "metadata": {
                    "source": "setup",
                    "type": "configuration",
                    "tags": "models,ollama,thermal",
                },
            },
            {
                "id": "owner-profile",
                "document": "Owner: Вячеслав (Slava). Experienced developer working on: crypto trading systems "
                "(Red Citadel V3.0 - Bybit perpetual futures), Telegram bots, AI solutions, smart assistants, "
                "Web3/DeFi, marketplace integrations, NeuroExpert platform, NeuroGUARDIAN. "
                "Preferred stack: Python, TypeScript. Uses Bybit exchange. "
                "Has NVIDIA GeForce RTX 4070 Laptop GPU (8GB VRAM), 16GB RAM. OS: Windows.",
                "metadata": {
                    "source": "setup",
                    "type": "owner",
                    "tags": "slava,owner,profile",
                },
            },
        ]

        # Upsert seed documents
        knowledge.upsert(
            ids=[d["id"] for d in seed_docs],
            documents=[d["document"] for d in seed_docs],
            metadatas=[d["metadata"] for d in seed_docs],
        )
        print(f"\n🧠 Seeded/Updated {len(seed_docs)} initial knowledge documents")

        # Final verification
        print("\n📋 Final collection status:")
        for name in collections_config:
            col = client.get_collection(name)
            print(f"  {name}: {col.count()} documents")

        return True

    except Exception as e:
        print(f"❌ ChromaDB initialization failed: {e}")
        return False


if __name__ == "__main__":
    success = init_collections()
    sys.exit(0 if success else 1)
