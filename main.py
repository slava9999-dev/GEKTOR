# main.py
import asyncio
import signal
import sys
import os
from dotenv import load_dotenv
from loguru import logger
from src.application.orchestrator import GektorOrchestrator

# [GEKTOR v2.0] Load environment variables
load_dotenv()

# Logger setup
logger.remove()
logger.add(
    sys.stdout, 
    level="INFO", 
    colorize=True, 
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
)

async def shutdown(loop, orchestrator=None, signal=None):
    """[GEKTOR v2.0] BEAZLEY PROTOCOL: Global Task Sweep."""
    if signal:
        logger.info(f"🛑 [SHUTDOWN] Received exit signal {signal.name}...")
    
    if orchestrator:
        try:
            await orchestrator.stop()
        except Exception as e:
            logger.error(f"⚠️ [SHUTDOWN] Orchestrator stop error: {e}")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    logger.warning(f"🧹 [SHUTDOWN] Cancelling {len(tasks)} pending tasks...")
    for task in tasks:
        task.cancel()
    
    # Wait for tasks to cancel with timeout
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.success("✅ [SHUTDOWN] Lifecycle complete. No zombies detected.")
    loop.stop()

def main():
    """Unified Lifecycle Manager with Monotonic Resilience."""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    orchestrator = GektorOrchestrator()
    
    # Signal handling for Unix
    if sys.platform != 'win32':
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(loop, orchestrator, s)))
    
    try:
        logger.info("🟢 [CORE] Radar Pipeline ENGAGED.")
        loop.run_until_complete(orchestrator.start())
        loop.run_forever()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.critical(f"☠️ [FATAL] Critical System Failure: {e}")
    finally:
        # Final Sweep - THE BEAZLEY GUARDIAN
        try:
            # Timeout the shutdown to prevent hanging on dead ProcessPool queues
            loop.run_until_complete(
                asyncio.wait_for(shutdown(loop, orchestrator), timeout=10.0)
            )
        except (asyncio.TimeoutError, Exception) as e:
            logger.error(f"❌ [SHUTDOWN] Final teardown error (forced): {e}")
        finally:
            # Suppress BrokenPipeError noise from killed ProcessPool workers
            import logging
            logging.getLogger("concurrent.futures").setLevel(logging.CRITICAL)
            if not loop.is_closed():
                loop.close()
            logger.info("🔌 [OFFLINE]")
            sys.exit(0)

if __name__ == "__main__":
    main()
