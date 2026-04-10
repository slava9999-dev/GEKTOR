# main.py
import asyncio
import signal
import sys
import os
from loguru import logger
from src.application.orchestrator import GektorOrchestrator

# Logger setup
logger.remove()
logger.add(
    sys.stdout, 
    level="INFO", 
    colorize=True, 
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
)

async def graceful_shutdown(loop: asyncio.AbstractEventLoop, orchestrator: GektorOrchestrator):
    """[GEKTOR v2.0] PROTOCOL CLEAN EXIT."""
    logger.warning("🚨 [CORE] Получен сигнал завершения. Начинаем Graceful Shutdown...")
    
    # 1. Принудительный сброс данных и остановка
    try:
        await orchestrator.stop()
        logger.success("🏁 [CORE] Система остановлена чисто. Зомби-процессов нет.")
    except Exception as e:
        logger.error(f"💥 [CORE] Ошибка при завершении: {e}")
    finally:
        # Cancel all pending tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

def main():
    """Unified Lifecycle Manager."""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    orchestrator = GektorOrchestrator()
    
    # Signal Handlers (SIGINT = Ctrl+C, SIGTERM = Docker stop)
    if sys.platform != 'win32':
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, 
                lambda: asyncio.create_task(graceful_shutdown(loop, orchestrator))
            )
    
    try:
        logger.info("🟢 [CORE] Radar Pipeline ENGAGED.")
        # Start and run until cancelled
        loop.run_until_complete(orchestrator.start())
        
        # Keep alive for signals
        if sys.platform == 'win32':
            # Windows doesn't support loop.add_signal_handler well, use loop runner
            loop.run_forever()
        else:
            loop.run_forever()
            
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.warning("🛑 [CORE] KeyboardInterrupt received. Triggering cleaning...")
        if not loop.is_closed():
            loop.run_until_complete(graceful_shutdown(loop, orchestrator))
    except Exception as e:
        logger.opt(exception=True).error(f"☠️ [FATAL] System crashed: {e}")
    finally:
        if not loop.is_closed():
            loop.close()

if __name__ == "__main__":
    main()
