import json
import logging
import time
import uuid
from functools import lru_cache

import numpy as np

from shared.config.settings import queue_names
from shared.embeddings.embedder import text_to_embedding, texts_to_embeddings
from shared.messaging.transport import Transport
from shared.vector_store.qdrant_store import QdrantStore
from shared.monitoring.metrics import PipelineMetrics
from shared.monitoring.drift_detector import DriftDetector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metrics = PipelineMetrics("embedding")
drift = DriftDetector(window_size=500)


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


def _make_handler(vector_store, transport):
    def _handle_message(ch, method, properties, body):
        t0 = time.time()
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
        transport.publish(queue_names.embedded, payload)
        metrics.inc("messages_processed")
        metrics.observe_latency(time.time() - t0)
        drift.record(payload["relevancy"])
        if metrics._counters["messages_processed"] == 500:
            drift.set_baseline()
        if metrics._counters["messages_processed"] % 100 == 0:
            drift_status = drift.check_drift()
            if drift_status.get("drift_detected"):
                logger.warning("RELEVANCY DRIFT: %s", drift_status)
        logger.info("Embedding: indexed post %s (relevancy=%s)", raw_id, payload["relevancy"])
        if hasattr(ch, 'basic_ack'):
            ch.basic_ack(delivery_tag=method.delivery_tag)
    return _handle_message


def run() -> None:
    client = Transport()
    vector_store = QdrantStore()
    client.declare_queue(queue_names.unique, dlq_name=queue_names.unique_dlq)
    client.declare_queue(queue_names.embedded)
    thread = client.consume(queue_names.unique, _make_handler(vector_store, client),
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
