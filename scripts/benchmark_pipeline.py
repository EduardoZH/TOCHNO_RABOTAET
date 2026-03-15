import json
import logging
import time

from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _handle_result(ch, method, properties, body):
    payload = json.loads(body)
    logger.info("Result: %s (cluster %s)", payload.get("post_id"), payload.get("cluster_id"))


def main() -> None:
    client = RabbitClient()
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
