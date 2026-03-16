import json
import logging
import time

import pika

from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient
from shared.preprocessing.filters import should_process, matched_keywords
from shared.monitoring.metrics import PipelineMetrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metrics = PipelineMetrics("prefilter")


def _handle_message(ch, method, properties, body):
    t0 = time.time()
    payload = json.loads(body)
    keywords = payload.get("keywords", [])
    exclusions = payload.get("exclusions", [])

    if should_process(payload, keywords, exclusions):
        text = " ".join([payload.get("title", ""), payload.get("content", "")])
        payload["matched_keywords"] = matched_keywords(text, keywords)
        ch.basic_publish(
            exchange="",
            routing_key=queue_names.filtered,
            body=json.dumps(payload, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logger.info("Prefilter: forwarded post %s (matched: %s)",
                    payload.get("post_id"), payload["matched_keywords"])
    else:
        metrics.inc("messages_filtered")
        logger.info("Prefilter: filtered out post %s", payload.get("post_id"))
    metrics.inc("messages_processed")
    metrics.observe_latency(time.time() - t0)


def run() -> None:
    client = RabbitClient()
    client.declare_queue(queue_names.raw, dlq_name=queue_names.raw_dlq)
    client.declare_queue(queue_names.filtered)
    thread = client.consume(queue_names.raw, _handle_message, dlq_name=queue_names.raw_dlq)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Prefilter service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
