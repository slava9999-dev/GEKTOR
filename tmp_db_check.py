import asyncio
import os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

async def check():
    url = "postgresql+asyncpg://tekton_admin:CRITICAL_SECURE_PASSWORD_CHANGE_IMMEDIATELY_9912@localhost:5433/tekton_alpha"
    engine = create_async_engine(url)
    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT count(*) FROM alerts;"))
        print(f"Alerts count: {res.scalar()}")
        res = await conn.execute(text("SELECT count(*) FROM watchlist_history;"))
        print(f"Watchlist history count: {res.scalar()}")
        res = await conn.execute(text("SELECT * FROM watchlist_history LIMIT 3;"))
        for row in res.mappings():
            print(row)
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(check())
