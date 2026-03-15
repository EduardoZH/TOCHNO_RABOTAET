import json
import threading
from typing import Callable

import pika

from shared.config.settings import queue_names, rabbit_config


class RabbitClient:
    def __init__(self):
        credentials = pika.PlainCredentials(rabbit_config.user, rabbit_config.password)
        self.parameters = pika.ConnectionParameters(
            host=rabbit_config.host,
            port=rabbit_config.port,
            credentials=credentials,
            heartbeat=rabbit_config.heartbeat,
            blocked_connection_timeout=rabbit_config.blocked_connection_timeout,
        )
        self.connection = None
        self.channel = None

    def connect(self):
        if self.connection and self.connection.is_open:
            return
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()

    def declare_queue(self, name: str, durable: bool = True):
        self.connect()
        self.channel.queue_declare(queue=name, durable=durable)

    def publish(self, queue: str, payload: dict):
        self.connect()
        message = json.dumps(payload).encode()
        self.channel.basic_publish(exchange="", routing_key=queue, body=message)

    def consume(self, queue: str, callback: Callable, prefetch: int = 1):
        self.connect()
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_qos(prefetch_count=prefetch)

        def wrapped(ch, method, properties, body):
            try:
                callback(ch, method, properties, body)
            except Exception:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=queue, on_message_callback=wrapped)
        thread = threading.Thread(target=self.channel.start_consuming, daemon=True)
        thread.start()
        return thread

    def close(self):
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()
