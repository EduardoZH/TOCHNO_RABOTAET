"""
Unified Transport class — works with both RabbitMQ and in-memory queues.

Drop-in replacement for RabbitClient in services.
"""
import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

# Transport mode: 'in_memory' or 'rabbitmq'
TRANSPORT_MODE = os.getenv("TRANSPORT_MODE", "in_memory")


class Transport:
    """
    Unified messaging transport.
    
    For in-memory mode: uses shared InMemoryQueue instance (singleton).
    For RabbitMQ mode: uses RabbitClient.
    """
    
    _in_memory_instance: Optional["InMemoryQueue"] = None
    
    def __init__(self):
        self.mode = TRANSPORT_MODE
        self.channel = self  # For compatibility with RabbitClient API
        self._shutting_down = False
        
        if self.mode == "in_memory":
            # Use singleton in-memory queue
            if Transport._in_memory_instance is None:
                from shared.messaging.in_memory_queue import InMemoryQueue
                Transport._in_memory_instance = InMemoryQueue()
            self._backend = Transport._in_memory_instance
        else:
            from shared.messaging.rabbitmq_client import RabbitClient
            self._backend = RabbitClient()
    
    def connect(self):
        """Connect to backend."""
        if hasattr(self._backend, 'connect'):
            self._backend.connect()
    
    def declare_queue(self, name: str, durable: bool = True, dlq_name: Optional[str] = None):
        """Declare a queue."""
        if self.mode == "in_memory":
            self._backend.declare(name, dlq_name=dlq_name)
        else:
            self._backend.declare_queue(name, durable=durable, dlq_name=dlq_name)
    
    def publish(self, queue: str, payload: dict):
        """Publish a message."""
        self._backend.publish(queue, payload)
    
    def consume(
        self,
        queue: str,
        callback: Callable,
        prefetch: int = 1,
        dlq_name: Optional[str] = None,
    ) -> threading.Thread:
        """Start consuming messages."""
        if self.mode == "in_memory":
            return self._backend.consume(queue, callback, dlq_name=dlq_name)
        else:
            return self._backend.consume(queue, callback, prefetch=prefetch, dlq_name=dlq_name)
    
    def close(self):
        """Close connection."""
        self._shutting_down = True
        if hasattr(self._backend, 'shutdown'):
            self._backend.shutdown()
        elif hasattr(self._backend, 'close'):
            self._backend.close()
    
    # For RabbitMQ compatibility
    @property
    def connection(self):
        if self.mode == "rabbitmq":
            return self._backend.connection
        return None
    
    @property
    def parameters(self):
        if self.mode == "rabbitmq":
            return self._backend.parameters
        return None
