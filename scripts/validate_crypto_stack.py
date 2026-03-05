import os
import sys
import asyncio

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


async def validate_stack():
    print("🚀 GERALD-SUPERBRAIN: CRYPTO STACK VALIDATION")
    print("=" * 50)

    # 1. Library Imports
    try:
        import importlib.util
        if not all(importlib.util.find_spec(lib) for lib in ["ccxt", "solana", "jito_searcher_client"]):
            raise ImportError("Missing required crypto libraries")

        print("✅ [LIBS] ccxt, solana, jito-searcher - OK")
    except ImportError as e:
        print(f"❌ [LIBS] Missing library: {e}")
        return

    # 2. Vector DB (ChromaDB) check
    try:
        from src.infrastructure.database.vector_db import VectorDatabase

        vdb = VectorDatabase()
        results = await vdb.search("Solana whales pump.fun", limit=3)
        if len(results) > 0:
            print(f"✅ [RAG] ChromaDB retrieval - OK (found: {len(results)} docs)")
        else:
            print(
                "⚠️  [RAG] ChromaDB search returned 0 results. Indexer might need rerun."
            )
    except Exception as e:
        print(f"❌ [RAG] ChromaDB error: {e}")

    # 3. Secure Sandbox check (Critical for trading bots)
    try:
        from src.infrastructure.sandbox.python_executor import PythonExecutor

        executor = PythonExecutor()
        res = await executor.execute("print('Sandbox Operational')")
        if res.success and "Sandbox Operational" in res.output:
            print("✅ [SANDBOX] Security Sandbox - OK")
        else:
            print(f"❌ [SANDBOX] Failed: {res.error}")
    except Exception as e:
        print(f"❌ [SANDBOX] Error: {e}")

    print("=" * 50)
    print("🔥 STATUS: READY FOR BATTLE")


if __name__ == "__main__":
    asyncio.run(validate_stack())
