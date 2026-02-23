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
        
        # Define collections with metadata
        collections_config = {
            "gerald-knowledge": "Default knowledge base — facts, insights, decisions",
            "conversations": "Conversation history summaries and key points",
            "skills-docs": "Skill documentation and usage patterns",
            "projects": "Project-specific knowledge (architectures, decisions, patterns)",
            "web-research": "Information gathered from web searches and documentation",
            "error-patterns": "Error analysis patterns for self-improvement",
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
                    }
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
                           "Gerald runs on OpenClaw with Ollama local models. Primary model: qwen2.5-coder:14b "
                           "(8k optimized context). Heavy model: mistral-nemo:latest (16k context). "
                           "Gerald uses advanced reasoning: CoT, ToT, self-critique, multi-step planning. "
                           "Optimized for RTX 4070 8GB VRAM.",
                "metadata": {"source": "setup", "type": "identity", "tags": "gerald,identity,core"}
            },
            {
                "id": "gerald-architecture", 
                "document": "Gerald-SuperBrain architecture: OpenClaw agent framework → Ollama for local LLM inference → "
                           "ChromaDB for vector memory (RAG). Skills: skill-master, vector-knowledge, model-manager, "
                           "antigravity-bridge, self-improver, reasoning-engine. Active Bridge Daemon "
                           "(bridge/bridge_daemon.py) ensures 24/7 connectivity with Antigravity UI.",
                "metadata": {"source": "setup", "type": "architecture", "tags": "architecture,skills,system"}
            },
            {
                "id": "gerald-models",
                "document": "Model fleet (RTX 4070 Optimized): 1) qwen2.5-coder:14b — primary. "
                           "Params: num_ctx=8192, num_gpu=26, temp=0.65. "
                           "2) mistral-nemo:latest — heavy. "
                           "Params: num_ctx=16384, num_gpu=28, temp=0.55. "
                           "Thermal limits: max 80°C GPU, auto-unload after 20min idle.",
                "metadata": {"source": "setup", "type": "configuration", "tags": "models,ollama,thermal"}
            },
            {
                "id": "owner-profile",
                "document": "Owner: Вячеслав (Slava). Experienced developer working on: crypto trading systems "
                           "(Red Citadel), Telegram bots, AI solutions, smart assistants, Web3/DeFi, marketplace "
                           "integrations. Preferred stack: Python, TypeScript. Uses Bybit exchange. "
                           "Has NVIDIA GeForce RTX 4070 Laptop GPU (8GB VRAM). OS: Windows.",
                "metadata": {"source": "setup", "type": "owner", "tags": "slava,owner,profile"}
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
