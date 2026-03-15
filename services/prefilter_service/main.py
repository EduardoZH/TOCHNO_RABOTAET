import json
import logging
import time

import pika

from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient
from shared.preprocessing.filters import should_process

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = RabbitClient()


def _handle_message(ch, method, properties, body):
    payload = json.loads(body)
    keywords = payload.get("keywords", [])
    exclusions = payload.get("exclusions", [])

    if should_process(payload, keywords, exclusions):
        ch.basic_publish(
            exchange="",
            routing_key=queue_names.filtered,
            body=json.dumps(payload, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logger.info("Prefilter: forwarded post %s", payload.get("post_id"))
    else:
        logger.info("Prefilter: filtered out post %s", payload.get("post_id"))


def run() -> None:
    client.declare_queue(queue_names.raw)
    client.declare_queue(queue_names.filtered)
    thread = client.consume(queue_names.raw, _handle_message)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Prefilter service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
