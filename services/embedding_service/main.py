import json
import logging
import time
import uuid
from functools import lru_cache

import numpy as np
import pika

from shared.config.settings import queue_names
from shared.embeddings.embedder import text_to_embedding, texts_to_embeddings
from shared.messaging.rabbitmq_client import RabbitClient
from shared.vector_store.qdrant_store import QdrantStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@lru_cache(maxsize=128)
def _get_keyword_embedding(keywords: tuple) -> tuple | None:
    if not keywords:
        return None
    embeddings = texts_to_embeddings(list(keywords))
    return tuple(tuple(e) for e in embeddings)


def _max_keyword_similarity(embedding: list, keyword_embeddings: list) -> float:
    a = np.array(embedding)
    best = 0.0
    for kw_emb in keyword_embeddings:
        sim = float(np.dot(a, np.array(kw_emb)))
        if sim > best:
            best = sim
    return best


def _make_handler(vector_store):
    def _handle_message(ch, method, properties, body):
        payload = json.loads(body)
        text = payload.get("content", "")
        raw_id = payload.get("post_id", str(time.time()))
        post_id = str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))

        embedding = text_to_embedding(text)

        keywords = payload.get("keywords", [])
        kw_embeddings = _get_keyword_embedding(tuple(sorted(keywords)))
        if kw_embeddings:
            relevancy = _max_keyword_similarity(embedding, kw_embeddings)
            payload["relevancy"] = min(100, max(0, round(relevancy * 100)))
        else:
            payload["relevancy"] = 0

        payload["qdrant_point_id"] = post_id
        payload["post_id"] = post_id

        vector_store.upsert(
            point_id=post_id,
            vector=embedding,
            payload={k: v for k, v in payload.items() if k not in ("embedding", "qdrant_point_id")},
        )
        ch.basic_publish(
            exchange="",
            routing_key=queue_names.embedded,
            body=json.dumps(payload, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logger.info("Embedding: indexed post %s (relevancy=%s)", raw_id, payload["relevancy"])
    return _handle_message


def run() -> None:
    client = RabbitClient()
    vector_store = QdrantStore()
    client.declare_queue(queue_names.unique, dlq_name=queue_names.unique_dlq)
    client.declare_queue(queue_names.embedded)
    thread = client.consume(queue_names.unique, _make_handler(vector_store),
                            dlq_name=queue_names.unique_dlq)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Embedding service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
