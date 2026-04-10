import asyncio
import os
from dotenv import load_dotenv
from pre_flight import PreflightCommander
from loguru import logger

# Load Environment Variables from .env
load_dotenv()

async def main():
    # Force some env vars if missing (mocking behavior for the test)
    # But pre_flight already reads them from os.environ
    commander = PreflightCommander()
    try:
        report = await commander.execute()
        print("\n" + "="*40)
        print("PREFLIGHT SUCCESSFUL")
        print("="*40)
        print(commander.format_boot_report_telegram())
        print("="*40)
    except SystemExit:
        print("\n" + "!"*40)
        print("PREFLIGHT FAILED (CRITICAL)")
        print("!"*40)
    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())
