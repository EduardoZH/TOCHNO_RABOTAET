import json
import logging
import time

from shared.config.settings import queue_names
from shared.messaging.transport import Transport
from shared.preprocessing.filters import should_process, matched_keywords
from shared.monitoring.metrics import PipelineMetrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metrics = PipelineMetrics("prefilter")


def _make_handler(transport):
    def _handle_message(ch, method, properties, body):
        t0 = time.time()
        payload = json.loads(body)
        keywords = payload.get("keywords", [])
        exclusions = payload.get("exclusions", [])

        if should_process(payload, keywords, exclusions):
            text = " ".join([payload.get("title", ""), payload.get("content", "")])
            payload["matched_keywords"] = matched_keywords(text, keywords)
            transport.publish(queue_names.filtered, payload)
            logger.info("Prefilter: forwarded post %s (matched: %s)",
                        payload.get("post_id"), payload["matched_keywords"])
        else:
            metrics.inc("messages_filtered")
            logger.info("Prefilter: filtered out post %s", payload.get("post_id"))
        metrics.inc("messages_processed")
        metrics.observe_latency(time.time() - t0)
        # Ack for RabbitMQ, no-op for in-memory
        if hasattr(ch, 'basic_ack'):
            ch.basic_ack(delivery_tag=method.delivery_tag)
    return _handle_message


def run() -> None:
    client = Transport()
    client.declare_queue(queue_names.raw, dlq_name=queue_names.raw_dlq)
    client.declare_queue(queue_names.filtered)
    thread = client.consume(queue_names.raw, _make_handler(client), dlq_name=queue_names.raw_dlq)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Prefilter service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
