import sys
import os
import time

PROJECT_ROOT = "c:\\Gerald-superBrain"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

try:
    from src.infrastructure.llm.router import SmartRouter
    print(f"SmartRouter: {SmartRouter}")
except Exception as e:
    print(f"FAILED TO IMPORT: {e}")
    import traceback
    traceback.print_exc()
