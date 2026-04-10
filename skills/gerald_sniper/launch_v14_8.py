import sys
import os
import asyncio
from pathlib import Path

# Add project root to sys.path
root = Path(__file__).parent.absolute()
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

print(f"DEBUG: PYTHONPATH={sys.path[0]}")
print(f"DEBUG: core exists={os.path.exists('core')}")
print(f"DEBUG: core/risk exists={os.path.exists('core/risk')}")
print(f"DEBUG: core/risk/__init__.py exists={os.path.exists('core/risk/__init__.py')}")

if __name__ == "__main__":
    try:
        from main import bootstrap
        asyncio.run(bootstrap())
    except Exception as e:
        import traceback
        traceback.print_exc()
