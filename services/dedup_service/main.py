import json
import logging
import time

from services.dedup_service.deduplicator import Deduplicator
from services.dedup_service.redis_store import RedisStore
from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


redis_store = RedisStore()
dedup = Deduplicator(redis_store)
client = RabbitClient()


def _handle_message(ch, method, properties, body):
    payload = json.loads(body)
    content = payload.get("content", "")
    duplicate, fingerprint = dedup.is_duplicate(content)
    payload["simhash"] = fingerprint
    if duplicate:
        logger.info("Dedup: drop post %s", payload.get("post_id"))
        return
    client.publish(queue_names.unique, payload)
    logger.debug("Dedup: forwarded post %s", payload.get("post_id"))


def run() -> None:
    client = RabbitClient()
    client.declare_queue(queue_names.filtered)
    client.declare_queue(queue_names.unique)
    thread = client.consume(queue_names.filtered, _handle_message)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Dedup service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
