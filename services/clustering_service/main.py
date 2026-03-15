import json
import logging
import time

import pika

from shared.clustering.cluster_manager import ClusterManager
from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient
from shared.vector_store.qdrant_store import QdrantStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _make_handler(cluster_manager, vector_store):
    def _handle_message(ch, method, properties, body):
        payload = json.loads(body)
        point_id = payload.get("qdrant_point_id")
        post_id = payload.get("post_id")
        if not point_id or not post_id:
            logger.error("Clustering: missing point_id/post_id, sending to DLQ: %s", payload)
            raise ValueError(f"Missing required fields: point_id={post_id}, qdrant_point_id={point_id}")
        embedding = vector_store.get_vector(point_id)
        if not embedding:
            logger.error("Clustering: vector not found in Qdrant for %s, sending to DLQ", point_id)
            raise ValueError(f"Vector not found for point_id={point_id}")
        cluster_id = cluster_manager.assign_cluster(post_id, embedding, payload)
        logger.info("Clustering: assigned %s to %s", post_id, cluster_id)
        ch.basic_publish(
            exchange="",
            routing_key=queue_names.clustered,
            body=json.dumps(payload, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    return _handle_message


def run() -> None:
    client = RabbitClient()
    vector_store = QdrantStore()
    cluster_manager = ClusterManager(qdrant=vector_store)
    client.declare_queue(queue_names.embedded, dlq_name=queue_names.embedded_dlq)
    client.declare_queue(queue_names.clustered)
    thread = client.consume(queue_names.embedded, _make_handler(cluster_manager, vector_store),
                            dlq_name=queue_names.embedded_dlq)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Clustering service stopping")
    finally:
        cluster_manager.close()
        client.close()


if __name__ == "__main__":
    run()
