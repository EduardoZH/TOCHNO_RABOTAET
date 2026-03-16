import json
import logging
import time

from shared.clustering.cluster_manager import ClusterManager
from shared.config.settings import queue_names
from shared.messaging.transport import Transport
from shared.vector_store.qdrant_store import QdrantStore
from shared.monitoring.metrics import PipelineMetrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metrics = PipelineMetrics("clustering")


def _make_handler(cluster_manager, vector_store, transport):
    def _handle_message(ch, method, properties, body):
        t0 = time.time()
        payload = json.loads(body)
        point_id = payload.get("qdrant_point_id")
        post_id = payload.get("post_id")
        if not point_id or not post_id:
            metrics.inc("messages_errored")
            raise ValueError(f"Missing required fields: point_id={post_id}, qdrant_point_id={point_id}")

        try:
            embedding = vector_store.get_vector(point_id)
            if not embedding:
                raise ValueError(f"Vector not found for point_id={point_id}")
            cluster_id = cluster_manager.assign_cluster(post_id, embedding, payload)
        except Exception:
            # Fallback: assign "unknown" cluster so message isn't lost
            logger.warning("Clustering fallback for %s: assigning unknown cluster", post_id)
            cluster_id = "unknown"
            payload["cluster_id"] = cluster_id
            payload["similarity_score"] = 0.0
            payload["fallback"] = True

        logger.info("Clustering: assigned %s to %s", post_id, cluster_id)
        transport.publish(queue_names.clustered, payload)
        metrics.inc("messages_processed")
        metrics.observe_latency(time.time() - t0)
        if hasattr(ch, 'basic_ack'):
            ch.basic_ack(delivery_tag=method.delivery_tag)
    return _handle_message


def run() -> None:
    client = Transport()
    vector_store = QdrantStore()
    cluster_manager = ClusterManager(qdrant=vector_store)
    client.declare_queue(queue_names.embedded, dlq_name=queue_names.embedded_dlq)
    client.declare_queue(queue_names.clustered)
    thread = client.consume(queue_names.embedded, _make_handler(cluster_manager, vector_store, client),
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
