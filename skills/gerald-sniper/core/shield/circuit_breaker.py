import time
import asyncio
from enum import Enum
from functools import wraps
from typing import Callable, Any, Tuple, Type
from loguru import logger

class CircuitBreakerState(Enum):
    CLOSED = 1
    OPEN = 2
    HALF_OPEN = 3

class CircuitBreakerOpenError(Exception):
    pass

class AsyncCircuitBreaker:
    """
    Zero-Allocation Async Circuit Breaker.
    Protects the Event Loop from hanging I/O connections.
    """
    __slots__ = (
        'name', 'failure_threshold', 'recovery_timeout', 'expected_exceptions',
        'state', 'failure_count', 'last_failure_time_mono'
    )

    def __init__(
        self, 
        name: str, 
        failure_threshold: int = 3, 
        recovery_timeout: float = 10.0,
        expected_exceptions: Tuple[Type[Exception], ...] = (asyncio.TimeoutError, ConnectionError)
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time_mono = 0.0

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            loop = asyncio.get_running_loop()
            current_time = loop.time()

            # Мгновенная проверка стейта (Fast Fail защита Event Loop'a)
            if self.state == CircuitBreakerState.OPEN:
                if (current_time - self.last_failure_time_mono) >= self.recovery_timeout:
                    logger.warning(f"CIRCUIT BREAKER [{self.name}]: Entering HALF-OPEN state. Testing connection...")
                    self.state = CircuitBreakerState.HALF_OPEN
                else:
                    raise CircuitBreakerOpenError(f"Circuit Breaker '{self.name}' is OPEN. Fast failing I/O call.")

            try:
                # Попытка I/O
                result = await func(*args, **kwargs)
                
                # Успешный ответ
                if self.state == CircuitBreakerState.HALF_OPEN:
                    logger.success(f"CIRCUIT BREAKER [{self.name}]: Connection Recovered. State set to CLOSED.")
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count > 0:
                    # Частичное восстановление (ошибки обнулились до достижения порога)
                    self.failure_count = 0
                    
                return result

            except self.expected_exceptions as e:
                self.failure_count += 1
                self.last_failure_time_mono = loop.time()
                
                if self.state == CircuitBreakerState.HALF_OPEN:
                    logger.critical(f"CIRCUIT BREAKER [{self.name}]: HALF-OPEN test failed. Reverting to OPEN.")
                    self.state = CircuitBreakerState.OPEN
                
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    logger.critical(f"CIRCUIT BREAKER [{self.name}]: Failure threshold ({self.failure_threshold}) reached. State set to OPEN.")
                    self.state = CircuitBreakerState.OPEN
                
                raise e

        return wrapper
