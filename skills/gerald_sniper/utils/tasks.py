import asyncio
from loguru import logger
from typing import Coroutine, Any, Dict, List

class TaskRegistry:
    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}

    def create_safe_task(self, coro: Coroutine[Any, Any, Any], name: str = "custom_task") -> asyncio.Task:
        """
        Creates a task and adds a callback to log any exceptions.
        Ensures unique names in registry.
        """
        # Ensure unique name to avoid overwriting in registry
        base_name = name
        counter = 1
        while name in self._tasks:
            name = f"{base_name}_{counter}"
            counter += 1

        task = asyncio.create_task(coro, name=name)
        self._tasks[name] = task
        
        def _done_callback(t: asyncio.Task):
            # Use task name from the task object to ensure we pop the right one
            self._tasks.pop(t.get_name(), None)
            try:
                exc = t.exception()
                if exc:
                    logger.error(f"❌ Background task '{t.get_name()}' failed: {exc}")
                    logger.exception(exc) # Log full traceback
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error in task callback for '{t.get_name()}': {e}")
                
        task.add_done_callback(_done_callback)
        return task

    async def shutdown(self, timeout: float = 5.0):
        """Gracefully cancels all registered tasks."""
        active_tasks = list(self._tasks.values())
        logger.info(f"Shutting down {len(active_tasks)} background tasks...")
        for task in active_tasks:
            task.cancel()
            
        # Wait for tasks to finish cancelling
        try:
            await asyncio.wait(active_tasks, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Timeout reached while waiting for tasks to cancel.")
            
        for name, task in self._tasks.items():
            if not task.done():
                logger.warning(f"Task '{name}' did not shut down gracefully.")

task_registry = TaskRegistry()

def safe_task(coro: Coroutine[Any, Any, Any], name: str = "custom_task") -> asyncio.Task:
    """Legacy wrapper for TaskRegistry.create_safe_task."""
    return task_registry.create_safe_task(coro, name=name)

def global_exception_handler(loop, context):
    """Asyncio loop exception handler to catch unhandled errors."""
    msg = context.get("message")
    exception = context.get("exception")
    handle = context.get("handle")
    
    source = f" - source: {handle!r}" if handle else ""
    if exception:
        logger.error(f"🔥 UNHANDLED ERROR in loop: {msg}{source}")
        logger.exception(exception)
    else:
        logger.error(f"🔥 UNHANDLED MESSAGE in loop: {msg}{source}")
