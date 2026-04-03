import asyncio
import os
import sys

BASE_DIR = r"c:\Gerald-superBrain"
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "skills", "gerald-sniper"))

async def test_sql():
    try:
        from src.application.tools.gerald_tools import tool_execute_sql
        query = "SELECT symbol, timestamp, price, volume_spike, liquidity_tier FROM watchlist_history WHERE price IS NOT NULL LIMIT 5"
        print(f"Executing: {query}")
        result = await tool_execute_sql({"query": query})
        
        if result.success:
            print("✅ DB CONNECTION & SCHEMA VERIFIED")
            print(result.output)
        else:
            print(f"❌ DB ERROR: {result.error}")
            
    except Exception as e:
        print(f"Test Execution Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_sql())
