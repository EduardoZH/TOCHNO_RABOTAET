import json
import logging
import time

import pika

from services.dedup_service.deduplicator import Deduplicator
from services.dedup_service.redis_store import RedisStore
from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _make_handler(dedup):
    def _handle_message(ch, method, properties, body):
        payload = json.loads(body)
        text = " ".join([payload.get("title", ""), payload.get("content", "")])
        duplicate, fingerprint = dedup.is_duplicate(text)
        payload["simhash"] = fingerprint
        if duplicate:
            logger.info("Dedup: drop post %s", payload.get("post_id"))
            return
        ch.basic_publish(
            exchange="",
            routing_key=queue_names.unique,
            body=json.dumps(payload, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logger.info("Dedup: forwarded post %s", payload.get("post_id"))
    return _handle_message


def run() -> None:
    redis_store = RedisStore()
    dedup = Deduplicator(redis_store)
    client = RabbitClient()
    client.declare_queue(queue_names.filtered, dlq_name=queue_names.filtered_dlq)
    client.declare_queue(queue_names.unique)
    thread = client.consume(queue_names.filtered, _make_handler(dedup),
                            dlq_name=queue_names.filtered_dlq)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Dedup service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
