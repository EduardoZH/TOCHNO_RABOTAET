import json
import logging
import time

from services.dedup_service.deduplicator import Deduplicator
from services.dedup_service.redis_store import RedisStore
from shared.config.settings import queue_names
from shared.messaging.transport import Transport
from shared.monitoring.metrics import PipelineMetrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metrics = PipelineMetrics("dedup")


def _make_handler(dedup, transport):
    def _handle_message(ch, method, properties, body):
        t0 = time.time()
        payload = json.loads(body)
        text = " ".join([payload.get("title", ""), payload.get("content", "")])
        duplicate, fingerprint = dedup.is_duplicate(text)
        payload["simhash"] = fingerprint
        if duplicate:
            metrics.inc("messages_duplicated")
            logger.info("Dedup: drop post %s", payload.get("post_id"))
        else:
            transport.publish(queue_names.unique, payload)
            logger.info("Dedup: forwarded post %s", payload.get("post_id"))
        metrics.inc("messages_processed")
        metrics.observe_latency(time.time() - t0)
        if hasattr(ch, 'basic_ack'):
            ch.basic_ack(delivery_tag=method.delivery_tag)
    return _handle_message


def run() -> None:
    redis_store = RedisStore()
    dedup = Deduplicator(redis_store)
    client = Transport()
    client.declare_queue(queue_names.filtered, dlq_name=queue_names.filtered_dlq)
    client.declare_queue(queue_names.unique)
    thread = client.consume(queue_names.filtered, _make_handler(dedup, client),
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
