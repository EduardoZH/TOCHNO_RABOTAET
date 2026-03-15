import json
import logging
import time

import numpy as np

from shared.config.settings import queue_names
from shared.embeddings.embedder import text_to_embedding, texts_to_embeddings
from shared.messaging.rabbitmq_client import RabbitClient
from shared.vector_store.qdrant_store import QdrantStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = RabbitClient()
vector_store = QdrantStore()

_keyword_cache: dict = {}


def _get_keyword_embedding(keywords: list) -> list | None:
    if not keywords:
        return None
    key = tuple(sorted(keywords))
    if key not in _keyword_cache:
        keyword_text = " ".join(keywords)
        _keyword_cache[key] = text_to_embedding(keyword_text)
    return _keyword_cache[key]


def _cosine_similarity(a: list, b: list) -> float:
    a_arr = np.array(a)
    b_arr = np.array(b)
    dot = np.dot(a_arr, b_arr)
    norm_a = np.linalg.norm(a_arr)
    norm_b = np.linalg.norm(b_arr)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot / (norm_a * norm_b))


def _handle_message(ch, method, properties, body):
    payload = json.loads(body)
    text = payload.get("content", "")
    post_id = payload.get("post_id", str(time.time()))

    embedding = text_to_embedding(text)

    keywords = payload.get("keywords", [])
    kw_embedding = _get_keyword_embedding(keywords)
    if kw_embedding:
        relevancy = _cosine_similarity(embedding, kw_embedding)
        payload["relevancy"] = round(relevancy * 100)
    else:
        payload["relevancy"] = 0

    vector_store.upsert(
        point_id=post_id,
        vector=embedding,
        payload={k: v for k, v in payload.items() if k != "embedding"},
    )

    payload["embedding"] = embedding
    client.publish(queue_names.embedded, payload)
    logger.debug("Embedding: indexed post %s (relevancy=%s)", post_id, payload["relevancy"])


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
