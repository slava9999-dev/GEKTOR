import asyncio
import os
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

async def read_history():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6381/0")
    password = os.getenv("REDIS_PASSWORD", None)
    
    r = aioredis.from_url(redis_url, password=password, decode_responses=True)
    
    print("--- Historical Stream: SignalEvent ---")
    try:
        signals = await r.xrevrange("stream:SignalEvent", max="+", min="-", count=10)
        for s in signals:
            print(s)
    except Exception as e:
        print(f"No SignalEvent: {e}")

    print("\n--- Historical Stream: AlertEvent ---")
    try:
        alerts = await r.xrevrange("stream:AlertEvent", max="+", min="-", count=10)
        for a in alerts:
            print(a)
    except Exception as e:
        print(f"No AlertEvent: {e}")

    print("\n--- Redis Keys ---")
    keys = await r.keys("*")
    print(keys)
    
if __name__ == "__main__":
    asyncio.run(read_history())
