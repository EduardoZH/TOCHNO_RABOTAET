import json
import logging
import time

from shared.config.settings import queue_names
from shared.messaging.transport import Transport

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _handle_result(payload):
    cluster_id = payload.get("clusterId")
    project_id = payload.get("projectId")
    posts = payload.get("posts", [])
    for post in posts:
        metrics = post.get("metrics", {})
        logger.info(
            "Result: project=%s cluster=%s relevancy=%s tone=%s title=%s",
            project_id,
            cluster_id,
            metrics.get("relevancy"),
            metrics.get("tone"),
            post.get("title", "")[:50],
        )


def main() -> None:
    client = Transport()
    client.declare_queue(queue_names.analysis)
    thread = client.consume(queue_names.analysis, _handle_result)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Benchmark: stopping")
    finally:
        client.close()


if __name__ == "__main__":
    main()
