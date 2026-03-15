import json
import logging
import time

from shared.config.settings import queue_names
from shared.embeddings.embedder import text_to_embedding
from shared.messaging.rabbitmq_client import RabbitClient
from shared.vector_store.qdrant_store import QdrantStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = RabbitClient()
vector_store = QdrantStore()


def _handle_message(ch, method, properties, body):
    payload = json.loads(body)
    text = payload.get("content", "")
    embedding = text_to_embedding(text)
    payload["embedding"] = embedding
    vector_store.upsert(
        point_id=payload.get("post_id", str(time.time())),
        vector=embedding,
        payload=payload,
    )
    client.publish(queue_names.embedded, payload)
    logger.debug("Embedding: indexed post %s", payload.get("post_id"))


def run() -> None:
    client.declare_queue(queue_names.unique)
    client.declare_queue(queue_names.embedded)
    thread = client.consume(queue_names.unique, _handle_message)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Embedding service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
