import json
import logging
import time

from shared.config.settings import queue_names
from shared.models.rubert_model import RuBertModel
from shared.messaging.rabbitmq_client import RabbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = RabbitClient()
model = RuBertModel()


def _handle_message(ch, method, properties, body):
    payload = json.loads(body)
    text = payload.get("content", "")
    result = model.predict(text)
    payload["nlp_analysis"] = result
    client.publish(queue_names.analysis, payload)
    logger.debug("NLP: enriched post %s", payload.get("post_id"))


def run() -> None:
    client.declare_queue(queue_names.clustered)
    client.declare_queue(queue_names.analysis)
    thread = client.consume(queue_names.clustered, _handle_message)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("NLP service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
