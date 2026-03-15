import json
import random
import time

from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient


def build_post(index: int) -> dict:
    keywords = ["мошен", "недвижимост"]
    exclusions = ["spam", "ad"]
    return {
        "post_id": f"post-{index}",
        "title": f"Сообщение номер {index}",
        "content": "Тестовое сообщение про недвижимость и новые технологии.",
        "keywords": keywords,
        "exclusions": exclusions,
        "timestamp": time.time(),
        "projectId": "demo",
    }


def main() -> None:
    client = RabbitClient()
    client.declare_queue(queue_names.raw)
    for i in range(10):
        post = build_post(i)
        print('Sent message number ', i)
        client.publish(queue_names.raw, post)
        time.sleep(random.uniform(0.1, 0.5))


if __name__ == "__main__":
    main()
