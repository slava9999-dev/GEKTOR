"""Quick .env validation — prints key status without exposing secrets."""
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"), override=True)

keys = {
    "TELEGRAM_BOT_TOKEN": True,
    "TELEGRAM_CHAT_ID": True,
    "SNIPER_BOT_TOKEN": False,
    "SNIPER_CHAT_ID": False,
    "OPENROUTER_API_KEY": True,
    "DEEPSEEK_API_KEY": False,
}

print("=" * 50)
print("ENV CHECK — Gerald-SuperBrain")
print("=" * 50)

all_ok = True
for key, required in keys.items():
    val = os.getenv(key, "")
    if val:
        # Show length + first 4 chars only
        safe = val[:4] + "..." + val[-4:] if len(val) > 10 else "****"
        status = f"✅ SET ({len(val)} chars, {safe})"
    elif required:
        status = "❌ MISSING (REQUIRED)"
        all_ok = False
    else:
        status = "⚪ NOT SET (optional)"
    print(f"  {key}: {status}")

print("=" * 50)
if all_ok:
    print("✅ All required keys present. Ready to launch.")
else:
    print("❌ Some required keys missing! Fix .env before launch.")
