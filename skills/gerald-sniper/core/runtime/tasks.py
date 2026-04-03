import asyncio
from loguru import logger
from typing import Coroutine, Any, Set


class TaskRegistry:
    _background_tasks: Set[asyncio.Task] = set()
    _is_shutting_down: bool = False

    @classmethod
    def fire_and_forget(cls, coro: Coroutine[Any, Any, Any], name: str) -> None:
        """Safe launch of background tasks with hard exception handling."""
        if cls._is_shutting_down:
            logger.warning(f"🚫 [SHUTDOWN] Ignoring task {name}, system stopping.")
            return

        task = asyncio.create_task(cls._safe_wrapper(coro, name), name=name)
        cls._background_tasks.add(task)
        task.add_done_callback(cls._background_tasks.discard)

    @classmethod
    async def _safe_wrapper(cls, coro: Coroutine, name: str) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            logger.info(f"🛑 [TASK CANCELLED] {name} cancelled.")
            raise
        except Exception as e:
            logger.critical(f"💀 [FATAL] Exception in {name}: {repr(e)}", exc_info=True)

    @classmethod
    def shield_and_track(cls, coro: Coroutine[Any, Any, Any]) -> asyncio.Future:
        """
        True Titanium Shield.
        Protects coroutine from cancellation and GUARANTEES graceful_shutdown 
        waits for completion before closing Event Loop.
        """
        if cls._is_shutting_down:
            logger.warning("🚫 [SHIELD] System stopping, critical section rejected.")
            fut = asyncio.Future()
            fut.set_exception(RuntimeError("System shutting down"))
            return fut

        inner_task = asyncio.create_task(coro)
        cls._background_tasks.add(inner_task)
        inner_task.add_done_callback(cls._background_tasks.discard)
        
        return asyncio.shield(inner_task)

    @classmethod
    async def shutdown(cls, timeout: float = 5.0) -> None:
        """Atomic stop of all background processes."""
        cls._is_shutting_down = True
        logger.info(f"⏳ [SHUTDOWN] Graceful Shutdown initiated. Active tasks: {len(cls._background_tasks)}")
        
        for task in list(cls._background_tasks):
            if not task.done():
                task.cancel()
            
        if cls._background_tasks:
            try:
                # Wait for tasks to complete (including shielded ones)
                await asyncio.wait_for(
                    asyncio.gather(*cls._background_tasks, return_exceptions=True),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"⚠️ [SHUTDOWN] Timeout waiting for {len(cls._background_tasks)} tasks.")
        
        logger.info("✅ [SHUTDOWN] Event Loop cleared.")

# Shorthand for creation
def safe_task(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task:
    # Legacy wrapper for fire_and_forget logic or simple create_task tracker
    return TaskRegistry.fire_and_forget(coro, name)

def setup_loop_exception_handler() -> None:
    loop = asyncio.get_event_loop()
    def handler(loop, context):
        exc = context.get("exception")
        if exc:
            logger.opt(exception=exc).error(f"Loop unhandled exception: {context.get('message')}")
        else:
            logger.error(f"Loop context error: {context}")
    loop.set_exception_handler(handler)

