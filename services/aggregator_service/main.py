"""
Aggregator service — группирует посты по projectId и отправляет одним сообщением на проект.

Вход (очередь results):
{
    "clusterId": "uuid",
    "projectId": "uuid",
    "posts": [{
        "title": "string",
        "content": "string",
        "type": "string",
        "url": "string",
        "metrics": {...}
    }]
}

Выход (очередь project_results):
{
    "projectId": "uuid",
    "posts": [{
        "title": "string",
        "content": "string (сжатое)",
        "type": "string",
        "metrics": {...},
        "cluster_id": "uuid"
    }]
}

Механизм группировки:
- Счётчик постов: когда собраны все (post_count == total_posts) — отправляем
- Таймаут: если прошёл AGGREGATOR_TIMEOUT_SEC после последнего поста — отправляем что есть
"""
import json
import logging
import threading
import time
from collections import defaultdict

from shared.config.settings import queue_names
from shared.messaging.transport import Transport
from shared.monitoring.metrics import PipelineMetrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metrics = PipelineMetrics("aggregator")

# Таймаут отправки проекта (секунды после последнего поста)
AGGREGATOR_TIMEOUT_SEC = float(__import__("os").getenv("AGGREGATOR_TIMEOUT_SEC", 2.0))


class ProjectBuffer:
    """Буфер для сбора постов одного проекта."""

    def __init__(self, project_id: str, total_posts: int, on_complete, on_timeout):
        self.project_id = project_id
        self.total_posts = total_posts
        self.posts = []
        self.lock = threading.Lock()
        self.timer: threading.Timer | None = None
        self.sent = False
        self._on_complete = on_complete
        self._on_timeout = on_timeout

    def add_post(self, post_data: dict):
        with self.lock:
            if self.sent:
                return
            self.posts.append(post_data)
            self._reset_timer()
            # Проверка: все ли посты собраны
            if len(self.posts) >= self.total_posts:
                self._flush()

    def _reset_timer(self):
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(AGGREGATOR_TIMEOUT_SEC, self._on_timeout_callback)
        self.timer.start()

    def _on_timeout_callback(self):
        with self.lock:
            if not self.sent:
                logger.info("Aggregator: timeout for project %s, sending %d posts",
                            self.project_id, len(self.posts))
                self._flush()

    def _flush(self):
        if self.sent:
            return
        self.sent = True
        if self.timer:
            self.timer.cancel()
        self._on_complete(self.project_id, self.posts)


class Aggregator:
    """Агрегатор постов по проектам."""

    def __init__(self, transport):
        self.transport = transport
        self.buffers: dict[str, ProjectBuffer] = {}
        self.lock = threading.Lock()

    def add_post(self, project_id: str, post_data: dict, total_posts: int):
        with self.lock:
            if project_id not in self.buffers:
                self.buffers[project_id] = ProjectBuffer(
                    project_id, total_posts,
                    on_complete=self._send_project,
                    on_timeout=self._remove_and_timeout
                )
                logger.info("Aggregator: new project %s, expecting %d posts",
                            project_id, total_posts)
            self.buffers[project_id].add_post(post_data)

    def _send_project(self, project_id: str, posts: list):
        """Отправить сгруппированное сообщение проекта."""
        output = {
            "projectId": project_id,
            "posts": posts,
        }
        self.transport.publish(queue_names.project_results, output)
        metrics.inc("projects_sent")
        metrics.inc("posts_aggregated", len(posts))
        logger.info("Aggregator: sent project %s with %d posts", project_id, len(posts))
        # Очистка буфера
        with self.lock:
            self.buffers.pop(project_id, None)

    def _remove_and_timeout(self, project_id: str):
        """Обработка таймаута — отправить что есть и удалить буфер."""
        with self.lock:
            buffer = self.buffers.pop(project_id, None)
        if buffer and not buffer.sent:
            buffer.sent = True
            self._send_project(project_id, buffer.posts)


def _make_handler(aggregator):
    def _handle_message(ch, method, properties, body):
        t0 = time.time()
        payload = json.loads(body)

        # Извлекаем данные из формата NLP-сервиса
        project_id = payload.get("projectId")
        cluster_id = payload.get("clusterId")
        posts = payload.get("posts", [])

        if not project_id or not posts:
            logger.warning("Aggregator: invalid message, skipping")
            metrics.inc("messages_errored")
            if hasattr(ch, 'basic_ack'):
                ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # total_posts берём из первого поста (передаётся через весь pipeline от splitter)
        total_posts = payload.get("total_posts", 1)
        post_data = posts[0]

        # Добавляем cluster_id в пост
        post_data["cluster_id"] = cluster_id

        # Формируем выходной пост в нужном формате
        output_post = {
            "title": post_data.get("title", ""),
            "content": post_data.get("content", ""),
            "type": post_data.get("type", ""),
            "url": post_data.get("url", ""),
            "metrics": post_data.get("metrics", {}),
            "cluster_id": cluster_id,
        }

        aggregator.add_post(project_id, output_post, total_posts)
        metrics.inc("messages_processed")
        metrics.observe_latency(time.time() - t0)
        if hasattr(ch, 'basic_ack'):
            ch.basic_ack(delivery_tag=method.delivery_tag)

    return _handle_message


def run() -> None:
    client = Transport()
    client.declare_queue(queue_names.analysis)
    client.declare_queue(queue_names.project_results)

    aggregator = Aggregator(client)

    thread = client.consume(queue_names.analysis, _make_handler(aggregator))
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Aggregator service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
