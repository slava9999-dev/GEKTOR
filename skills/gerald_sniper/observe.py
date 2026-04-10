import asyncio
import os
import json
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

async def observe():
    print("Observer starting...", flush=True)
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6381/0")
    password = os.getenv("REDIS_PASSWORD", None)
    
    r = aioredis.from_url(redis_url, password=password, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.psubscribe("*")
    
    print("Watching all channels (filtering out RawWS, PriceUpdate)...", flush=True)

    try:
        async with asyncio.timeout(30.0):
            async for msg in pubsub.listen():
                if msg["type"] in ("message", "pmessage"):
                    channel = msg["channel"]
                    if "RawWSEvent" in channel or "PriceUpdate" in channel or "orderbook" in channel:
                        continue
                    
                    data = msg["data"]
                    if isinstance(data, str) and len(data) > 300:
                        data = data[:300] + "..."
                    print(f"\n[RECV] {channel}: {data}", flush=True)
    except asyncio.TimeoutError:
        print("\nTimeout reached. Finished observing.", flush=True)
            
if __name__ == "__main__":
    asyncio.run(observe())
