"""
Splitter service — принимает batch-сообщение по API-контракту и раскладывает
в отдельные сообщения для pipeline.

Вход (очередь batch_input):
{
    "projectId": "uuid",
    "keywords": ["string"],
    "risk_words": ["string"],
    "posts": [{"title": "...", "content": "...", "type": "...", "url": "..."}]
}

Выход (очередь raw_posts): по одному сообщению на пост:
{
    "post_id": "uuid",
    "projectId": "uuid",
    "keywords": [...],
    "exclusions": [...],   # маппинг risk_words -> exclusions
    "title": "...",
    "content": "...",
    "type": "...",
    "url": "...",
    "timestamp": float
}
"""
import json
import logging
import time
import uuid

from shared.config.settings import queue_names
from shared.messaging.transport import Transport

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_QUEUE = "batch_input"


def _make_handler(transport):
    def _handle_message(ch, method, properties, body):
        batch = json.loads(body)
        project_id = batch.get("projectId", "")
        keywords = batch.get("keywords", [])
        exclusions = batch.get("risk_words", batch.get("exclusions", []))
        posts = batch.get("posts", [])
        if not posts:
            logger.warning("Splitter: empty batch for project %s", project_id)
            return

        total_posts = len(posts)
        for post_data in posts:
            message = {
                "post_id": str(uuid.uuid4()),
                "projectId": project_id,
                "keywords": keywords,
                "exclusions": exclusions,
                "title": post_data.get("title", ""),
                "content": post_data.get("content", ""),
                "type": post_data.get("type", ""),
                "url": post_data.get("url", ""),
                "timestamp": time.time(),
                "total_posts": total_posts,  # Для агрегации по проектам
            }
            transport.publish(queue_names.raw, message)
        logger.info("Splitter: split batch of %d posts for project %s", len(posts), project_id)
        if hasattr(ch, 'basic_ack'):
            ch.basic_ack(delivery_tag=method.delivery_tag)
    return _handle_message


def run() -> None:
    client = Transport()
    logger.info("Splitter: Transport backend id=%s", id(client._backend))
    client.declare_queue(BATCH_QUEUE)
    client.declare_queue(queue_names.raw)
    thread = client.consume(BATCH_QUEUE, _make_handler(client))
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Splitter service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
