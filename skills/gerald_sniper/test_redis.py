import asyncio
import redis.asyncio as aioredis
import os
from dotenv import load_dotenv

load_dotenv()

async def test():
    url = os.getenv("REDIS_URL")
    print(f"Connecting to {url}...")
    try:
        r = aioredis.from_url(url, decode_responses=True)
        t0 = asyncio.get_event_loop().time()
        pong = await asyncio.wait_for(r.ping(), timeout=5.0)
        t1 = asyncio.get_event_loop().time()
        print(f"PONG! ({(t1-t0)*1000:.2f}ms)")
        await r.aclose()
    except Exception as e:
        print(f"REDIS FAILED: {e}")

if __name__ == "__main__":
    asyncio.run(test())
