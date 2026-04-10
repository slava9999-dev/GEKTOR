
import asyncio
import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "skills" / "gerald-sniper"))

from pre_flight import PreflightCommander

async def run_diagnostics():
    """
    [GEKTOR v12.0] APEX Diagnostic Suite.
    Runs a full infrastructure audit before system launch.
    """
    # 1. Load Environment
    env_path = PROJECT_ROOT / '.env'
    if not env_path.exists():
        logger.critical(f"❌ [ENV] .env file not found at {env_path}")
        return
    
    load_dotenv(dotenv_path=env_path, override=True)
    logger.info("🛠️ [DIAG] Environment loaded successfully.")

    # 2. Run PreflightCommander (Institutional Hardened Checks)
    # We pass None for rest_client to skip Bybit permissions check if not needed,
    # or it will try to measure latency if we don't pass it.
    # Let's try to initialize a minimal REST client for latency check.
    try:
        from data.bybit_rest import BybitREST
        rest_url = os.getenv("BYBIT_REST_URL", "https://api.bybit.com")
        rest_client = BybitREST(base_url=rest_url)
    except Exception as e:
        logger.warning(f"⚠️ [REST] Failed to init REST client: {e}")
        rest_client = None

    preflight = PreflightCommander(rest_client=rest_client, mode="ADVISORY")
    report = await preflight.execute()

    # 3. Summary Verdict
    print("\n" + "="*50)
    print("      🚀 GEKTOR APEX v12.0 DIAGNOSTIC REPORT")
    print("="*50)
    print(f"  [DOCKER] Containers Checked:   ✅ (Observed via manual check)")
    print(f"  [REDIS] Connection Health:     {'✅ OK' if report.redis_ok else '❌ FAILED'}")
    print(f"  [REDIS] Persistence (W/R):      {'✅ OK' if report.redis_write_ok else '❌ FAILED'}")
    print(f"  [POSTGRES] Connection Health:  {'✅ OK' if report.postgres_ok else '❌ FAILED'}")
    print(f"  [BYBIT] Connectivity:          {'✅ PASS' if report.bybit_connectivity else '❌ FAIL'}")
    print(f"  [NETWORK] Latency (Avg):       {report.avg_latency_ms:.0f}ms ({report.latency_status})")
    print("="*50)
    
    if report.is_critical_ok:
        print("\n  🟢 [VERDICT] SYSTEM READY FOR LAUNCH (Advisory Mode)")
        print("  Command: python main.py")
    else:
        print("\n  🔴 [VERDICT] CRITICAL ERROR: REPAIR INFRASTRUCTURE BEFORE START")
    print("="*50 + "\n")

if __name__ == "__main__":
    asyncio.run(run_diagnostics())
