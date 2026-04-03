import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def test():
    db_url = os.getenv("ASYNC_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/sniper")
    print(f"Connecting to {db_url}...")
    try:
        engine = create_async_engine(db_url)
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT version()"))
            v = result.fetchone()
            print(f"SUCCESS: {v}")
    except Exception as e:
        print(f"FAILED: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(test())
