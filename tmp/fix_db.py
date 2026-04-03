import asyncio
import aiosqlite
import os
from loguru import logger

async def fix_database():
    db_path = "skills/gerald-sniper/data_run/sniper.db"
    if not os.path.exists(db_path):
        logger.error(f"DB not found at {db_path}")
        return

    async with aiosqlite.connect(db_path) as db:
        async with db.execute("PRAGMA table_info(detected_levels)") as cursor:
            cols = [row[1] for row in await cursor.fetchall()]
            
        if "confluence_score" not in cols:
            logger.info("🛠️ Adding missing column confluence_score to detected_levels")
            try:
                await db.execute("ALTER TABLE detected_levels ADD COLUMN confluence_score INTEGER DEFAULT 0")
                await db.commit()
                logger.info("✅ Column confluence_score added successfully.")
            except Exception as e:
                logger.error(f"❌ Failed to add column: {e}")
        else:
            logger.info("✅ Column confluence_score already exists.")
            
        # Verify schema version
        async with db.execute("SELECT MAX(version) FROM schema_migrations") as cursor:
            row = await cursor.fetchone()
            v = row[0] if row else 0
            logger.info(f"Current schema version in DB: {v}")

if __name__ == "__main__":
    asyncio.run(fix_database())
