import asyncio
import os
import sys

# Correct path logic
BASE_DIR = r"c:\Gerald-superBrain"
SNIPER_DIR = os.path.join(BASE_DIR, "skills", "gerald-sniper")

if SNIPER_DIR not in sys.path:
    sys.path.insert(0, SNIPER_DIR)

async def check_schema():
    try:
        from data.database import DatabaseManager
        db = DatabaseManager()
        await db.initialize()
        
        # Check current columns in watchlist_history
        rows = await db.execute_read_safe("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'watchlist_history'
        """)
        print("Schema for watchlist_history:")
        for row in rows:
            print(f" - {row['column_name']} ({row['data_type']})")
        
        await db.close()
    except Exception as e:
        print(f"Error checking schema: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_schema())
