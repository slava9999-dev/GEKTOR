import asyncio
from loguru import logger
from typing import Coroutine, Any, Set


class TaskRegistry:
    _tasks: Set[asyncio.Task] = set()

    @classmethod
    def create(
        cls,
        coro: Coroutine[Any, Any, Any],
        name: str
    ) -> asyncio.Task:
        """
        Creates a task and adds it to the registry with an error callback.
        """
        task = asyncio.create_task(coro, name=name)
        cls._tasks.add(task)
        
        # Remove from set when done
        task.add_done_callback(cls._tasks.discard)
        
        # Add error handling callback
        task.add_done_callback(
            lambda t: cls._error_callback(t, name)
        )
        return task

    @classmethod
    def _error_callback(
        cls,
        task: asyncio.Task,
        name: str
    ) -> None:
        if task.cancelled():
            logger.warning(f"Task cancelled: {name}")
            return
        
        try:
            exc = task.exception()
            if exc:
                logger.opt(exception=exc).error(
                    f"Background task crashed: {name}"
                )
        except asyncio.InvalidStateError:
            # Task not finished yet (shouldn't happen in done_callback)
            pass

    @classmethod
    async def shutdown(cls, timeout: float = 5.0) -> None:
        if not cls._tasks:
            return
            
        logger.info(
            f"Cancelling {len(cls._tasks)} background tasks..."
        )
        
        for t in cls._tasks:
            if not t.done():
                t.cancel()
                
        # Wait for all tasks to acknowledge cancellation
        await asyncio.gather(
            *cls._tasks, return_exceptions=True
        )
        logger.info("All tasks cancelled and cleaned up.")


def setup_loop_exception_handler() -> None:
    """
    Sets a global exception handler for the current asyncio loop.
    """
    loop = asyncio.get_event_loop()

    def handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        msg = context.get("message", "")
        if exc:
            logger.opt(exception=exc).error(
                f"Loop unhandled exception: {msg}"
            )
        else:
            logger.error(f"Loop context error: {context}")

    loop.set_exception_handler(handler)

# Shorthand for creation
def safe_task(coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task:
    return TaskRegistry.create(coro, name=name)
