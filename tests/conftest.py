import pytest
import asyncio
import logging

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(autouse=True)
def silent_logger():
    """Disable logging during tests to keep output clean."""
    logging.getLogger("GEKTOR").setLevel(logging.CRITICAL)
    yield
