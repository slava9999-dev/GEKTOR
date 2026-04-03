import asyncio
import aiosqlite
import os

async def check_db():
    db_path = "skills/gerald-sniper/data_run/sniper.db"
    if not os.path.exists(db_path):
        print(f"DB not found at {db_path}")
        return

    async with aiosqlite.connect(db_path) as db:
        print("--- Table Info: detected_levels ---")
        async with db.execute("PRAGMA table_info(detected_levels)") as cursor:
            async for row in cursor:
                print(row)
        
        print("\n--- Row Count ---")
        async with db.execute("SELECT count(*) FROM detected_levels") as cursor:
            row = await cursor.fetchone()
            print(f"Total levels: {row[0]}")
            
        print("\n--- Latest 5 Levels ---")
        async with db.execute("SELECT * FROM detected_levels ORDER BY id DESC LIMIT 5") as cursor:
            async for row in cursor:
                print(row)

if __name__ == "__main__":
    asyncio.run(check_db())
