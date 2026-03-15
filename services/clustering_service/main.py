import json
import logging
import time

from shared.clustering.cluster_manager import ClusterManager
from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = RabbitClient()
cluster_manager = ClusterManager()


def _handle_message(ch, method, properties, body):
    payload = json.loads(body)
    embedding = payload.get("embedding")
    post_id = payload.get("post_id")
    if not embedding or not post_id:
        logger.warning("Clustering: insufficient data for %s", post_id)
        return
    cluster_id = cluster_manager.assign_cluster(post_id, embedding, payload)
    logger.debug("Clustering: assigned %s to %s", post_id, cluster_id)
    client.publish(queue_names.clustered, payload)


def run() -> None:
    client.declare_queue(queue_names.embedded)
    client.declare_queue(queue_names.clustered)
    thread = client.consume(queue_names.embedded, _handle_message)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Clustering service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
