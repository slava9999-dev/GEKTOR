import asyncio
import os
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

async def read_detectors():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6381/0")
    password = os.getenv("REDIS_PASSWORD", None)
    
    r = aioredis.from_url(redis_url, password=password, decode_responses=True)
    
    print("--- Historical Stream: DetectorEvent ---")
    try:
        events = await r.xrevrange("stream:DetectorEvent", max="+", min="-", count=10)
        for e in events:
            print(e)
    except Exception as e:
        print(f"Error: {e}")
            
if __name__ == "__main__":
    asyncio.run(read_detectors())
