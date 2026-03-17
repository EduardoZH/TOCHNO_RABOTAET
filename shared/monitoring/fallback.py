"""Circuit breaker for external service calls."""

import logging
import time
import threading

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Simple circuit breaker: open after N consecutive failures,
    half-open after timeout, close on success."""

    def __init__(self, name: str, failure_threshold: int = 3,
                 recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failure_count = 0
        self._state = "closed"  # closed | open | half-open
        self._last_failure_time = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            if self._state == "open":
                if time.time() - self._last_failure_time > self.recovery_timeout:
                    self._state = "half-open"
            return self._state

    def record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            if self._state in ("half-open", "open"):
                logger.info("CircuitBreaker[%s] closed after recovery", self.name)
            self._state = "closed"

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.failure_threshold:
                if self._state != "open":
                    logger.warning("CircuitBreaker[%s] OPEN after %d failures",
                                   self.name, self._failure_count)
                self._state = "open"

    def allow_request(self) -> bool:
        state = self.state
        return state in ("closed", "half-open")
