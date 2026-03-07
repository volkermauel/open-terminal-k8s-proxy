"""Circuit breaker for terminal pod health."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import StrEnum

logger = logging.getLogger(__name__)


class CircuitState(StrEnum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker implementation for managing pod health failures."""

    failure_threshold: int = 5
    recovery_timeout: int = 30
    half_open_max_calls: int = 3

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float | None = field(default=None, init=False)
    _half_open_calls: int = field(default=0, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    @property
    def state(self) -> CircuitState:
        """Get the current circuit breaker state."""
        return self._state

    async def can_execute(self) -> bool:
        """Check if a request can be executed based on circuit state."""
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                if self._last_failure_time and time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info("Circuit breaker entering half-open state")
                    return True
                return False

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls < self.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False

        return False

    async def record_success(self) -> None:
        """Record a successful operation, potentially closing the circuit."""
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                logger.info("Circuit breaker recovered, returning to closed state")
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._half_open_calls = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    async def record_failure(self) -> None:
        """Record a failed operation, potentially opening the circuit."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning("Circuit breaker failed in half-open state, reopening")
                self._state = CircuitState.OPEN
                self._half_open_calls = 0
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    logger.error(
                        f"Circuit breaker opened after {self._failure_count} failures"
                    )
                    self._state = CircuitState.OPEN


class CircuitBreakerRegistry:
    """Registry for managing circuit breakers per terminal pod."""

    def __init__(self) -> None:
        """Initialize the circuit breaker registry."""
        self._breakers: dict[str, CircuitBreaker] = {}

    def get(self, key: str) -> CircuitBreaker:
        """Get or create a circuit breaker for the given key."""
        if key not in self._breakers:
            self._breakers[key] = CircuitBreaker()
        return self._breakers[key]

    def remove(self, key: str) -> None:
        """Remove a circuit breaker from the registry."""
        self._breakers.pop(key, None)


circuit_breaker_registry = CircuitBreakerRegistry()
