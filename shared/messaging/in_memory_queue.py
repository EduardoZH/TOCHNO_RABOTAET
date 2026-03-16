"""
In-memory queue transport for single-container deployment.

Thread-safe queues with DLQ support and graceful shutdown.
"""
import json
import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, Optional


class _FakeCh:
    """Mock RabbitMQ channel for in-memory transport compatibility."""
    def basic_ack(self, delivery_tag=None):
        pass

    def basic_nack(self, delivery_tag=None, requeue=False):
        pass


class _FakeMethod:
    """Mock RabbitMQ method for in-memory transport compatibility."""
    delivery_tag = None

logger = logging.getLogger(__name__)


class InMemoryQueue:
    """Thread-safe in-memory queue with DLQ support."""

    def __init__(self):
        self._queues: Dict[str, queue.Queue] = {}
        self._dlq_queues: Dict[str, queue.Queue] = {}
        self._lock = threading.Lock()
        self._consumers: Dict[str, threading.Thread] = {}
        self._running = True

    def declare(self, name: str, dlq_name: Optional[str] = None) -> None:
        """Declare a queue with optional DLQ."""
        with self._lock:
            if name not in self._queues:
                self._queues[name] = queue.Queue()
                logger.info("InMemoryQueue: declared queue '%s'", name)

            if dlq_name and dlq_name not in self._dlq_queues:
                self._dlq_queues[dlq_name] = queue.Queue()
                logger.info("InMemoryQueue: declared DLQ '%s'", dlq_name)

    def publish(self, name: str, payload: Any) -> None:
        """Publish a message to a queue."""
        with self._lock:
            if name not in self._queues:
                raise ValueError(f"Queue '{name}' not declared")
        self._queues[name].put(payload)
        logger.debug("InMemoryQueue: published to '%s'", name)

    def publish_dlq(self, dlq_name: str, payload: Any) -> None:
        """Publish a message to a DLQ."""
        with self._lock:
            if dlq_name not in self._dlq_queues:
                self._dlq_queues[dlq_name] = queue.Queue()
                logger.info("InMemoryQueue: created DLQ '%s' on demand", dlq_name)
        self._dlq_queues[dlq_name].put(payload)
        logger.warning("InMemoryQueue: published to DLQ '%s'", dlq_name)

    def consume(
        self,
        name: str,
        callback: Callable[[Any], None],
        dlq_name: Optional[str] = None,
        prefetch: int = 1,
    ) -> threading.Thread:
        """Start consuming messages from a queue in a background thread."""
        with self._lock:
            if name not in self._queues:
                raise ValueError(f"Queue '{name}' not declared")

        def _consume_loop():
            logger.info("InMemoryQueue: started consumer for '%s'", name)
            while self._running:
                try:
                    # Non-blocking get with timeout to allow graceful shutdown
                    payload = self._queues[name].get(timeout=0.5)
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error("InMemoryQueue: error getting message: %s", e)
                    continue

                try:
                    body = json.dumps(payload, ensure_ascii=False).encode()
                    callback(_FakeCh(), _FakeMethod(), None, body)
                except json.JSONDecodeError as e:
                    logger.error("InMemoryQueue: invalid JSON in '%s': %s", name, e)
                    if dlq_name:
                        self.publish_dlq(dlq_name, payload)
                except Exception as e:
                    logger.exception("InMemoryQueue: error processing message from '%s'", name)
                    if dlq_name:
                        self.publish_dlq(dlq_name, payload)
                else:
                    logger.debug("InMemoryQueue: processed message from '%s'", name)

            logger.info("InMemoryQueue: consumer for '%s' stopped", name)

        thread = threading.Thread(target=_consume_loop, daemon=True, name=f"consumer-{name}")
        thread.start()
        with self._lock:
            self._consumers[name] = thread
        return thread

    def get_queue_size(self, name: str) -> int:
        """Get approximate queue size (for health checks)."""
        with self._lock:
            if name in self._queues:
                return self._queues[name].qsize()
            return 0

    def get_dlq_size(self, dlq_name: str) -> int:
        """Get approximate DLQ size (for health checks)."""
        with self._lock:
            if dlq_name in self._dlq_queues:
                return self._dlq_queues[dlq_name].qsize()
            return 0

    def shutdown(self, timeout: float = 5.0) -> None:
        """Gracefully shutdown all consumers."""
        logger.info("InMemoryQueue: shutting down...")
        self._running = False

        # Wait for consumers to finish
        with self._lock:
            consumers = list(self._consumers.values())

        for thread in consumers:
            thread.join(timeout=timeout / len(consumers) if consumers else timeout)

        logger.info("InMemoryQueue: shutdown complete")
