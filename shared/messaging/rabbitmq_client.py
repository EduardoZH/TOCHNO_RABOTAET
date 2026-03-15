import json
import logging
import threading
import time
from typing import Callable

import pika

from shared.config.settings import rabbit_config

logger = logging.getLogger(__name__)


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
        delay = rabbit_config.reconnect_delay
        while True:
            try:
                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                logger.info("Connected to RabbitMQ at %s:%s", rabbit_config.host, rabbit_config.port)
                return
            except pika.exceptions.AMQPConnectionError:
                logger.warning("RabbitMQ unavailable, retrying in %.1fs...", delay)
                time.sleep(delay)
                delay = min(delay * 2, rabbit_config.reconnect_max_delay)

    def declare_queue(self, name: str, durable: bool = True, dlq_name: str | None = None):
        self.connect()
        if dlq_name:
            self.channel.queue_declare(queue=dlq_name, durable=True)
            args = {
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": dlq_name,
            }
            self.channel.queue_declare(queue=name, durable=durable, arguments=args)
        else:
            self.channel.queue_declare(queue=name, durable=durable)

    def publish(self, queue: str, payload: dict):
        self.connect()
        message = json.dumps(payload, ensure_ascii=False).encode()
        self.channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def consume(self, queue: str, callback: Callable, prefetch: int = 1):
        self.connect()
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_qos(prefetch_count=prefetch)

        def wrapped(ch, method, properties, body):
            try:
                callback(ch, method, properties, body)
            except Exception:
                logger.exception("Error processing message from %s", queue)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        def _consume_loop():
            while True:
                try:
                    self.connect()
                    self.channel.queue_declare(queue=queue, durable=True)
                    self.channel.basic_qos(prefetch_count=prefetch)
                    self.channel.basic_consume(queue=queue, on_message_callback=wrapped)
                    logger.info("Started consuming from %s", queue)
                    self.channel.start_consuming()
                except pika.exceptions.AMQPConnectionError:
                    logger.warning("Lost connection to RabbitMQ, reconnecting...")
                    time.sleep(rabbit_config.reconnect_delay)
                    self.connection = None
                    self.channel = None
                except Exception:
                    logger.exception("Unexpected error in consumer, reconnecting...")
                    time.sleep(rabbit_config.reconnect_delay)
                    self.connection = None
                    self.channel = None

        thread = threading.Thread(target=_consume_loop, daemon=True)
        thread.start()
        return thread

    def close(self):
        if self.channel and self.channel.is_open:
            try:
                self.channel.close()
            except Exception:
                pass
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except Exception:
                pass
