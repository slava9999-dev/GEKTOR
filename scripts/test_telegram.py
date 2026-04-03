
import asyncio
import os
import sys

async def main():
    # Add project root and skill root to path BEFORE imports
    root = os.getcwd()
    skill_path = os.path.join(root, "skills", "gerald-sniper")
    if skill_path not in sys.path:
        sys.path.append(skill_path)
    
    # Delayed imports
    from dotenv import load_dotenv
    from utils.config import config
    from utils.telegram_bot import send_telegram_direct
    
    load_dotenv()
    print("🚀 Sending test Telegram message via Taiwan proxy...")
    res = await send_telegram_direct("💎 <b>Gerald Sniper Online</b>\nProxy: Taiwan 🇹🇼\nStatus: Connected")
    if res:
        print(f"✅ Success! Message ID: {res}")
    else:
        print("❌ Failed. Check console logs and PROXY_URL.")

if __name__ == "__main__":
    asyncio.run(main())
