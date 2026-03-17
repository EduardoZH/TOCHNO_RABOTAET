"""
Слушает очередь project_results и наглядно показывает выходные данные pipeline.
Запуск: $env:PYTHONPATH="."; .venv\Scripts\python scripts\demo_receive.py
"""
import json
import signal
import sys
import time
import threading

from shared.config.settings import queue_names
from shared.messaging.transport import Transport

received = 0
start_time = time.time()
shutdown_event = threading.Event()


def tone_icon(tone: str) -> str:
    return {"negative": "🔴 negative", "neutral": "🟡 neutral", "positive": "🟢 positive"}.get(tone, f"? {tone}")


def relevancy_bar(score: int) -> str:
    filled = score // 10
    bar = "█" * filled + "░" * (10 - filled)
    return f"[{bar}] {score}%"


def print_result(payload: dict):
    global received
    received += 1

    project_id = payload.get("projectId", "N/A")
    posts = payload.get("posts", [])

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print(f"║  ПРОЕКТ #{received:<58}║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║  projectId:  {project_id:<55}║")
    print(f"║  Постов в проекте: {len(posts):<48}║")
    print("╠══════════════════════════════════════════════════════════════════╣")

    for i, post in enumerate(posts, 1):
        title = post.get("title", "")[:50]
        content = post.get("content", "")[:50]
        post_type = post.get("type", "")
        cluster_id = str(post.get("cluster_id", "N/A"))[:20]
        metrics = post.get("metrics", {})
        relevancy = metrics.get("relevancy", 0)
        tone = metrics.get("tone", "unknown")

        print(f"║                                                                    ║")
        print(f"║  ПОСТ #{i:<54}║")
        print(f"║  Заголовок:  {title:<55}║")
        print(f"║  Контент:    {content:<55}║")
        print(f"║  Тип: {post_type:<12} Кластер: {cluster_id:<40}║")
        print(f"║  МЕТРИКИ:                                                        ║")
        rel_bar = relevancy_bar(relevancy)
        print(f"║    Тональность: {tone_icon(tone):<52}║")
        print(f"║    Релевантность: {rel_bar:<50}║")
        if i < len(posts):
            print(f"║  ────────────────────────────────────────────────────────────  ║")

    print("╚══════════════════════════════════════════════════════════════════╝")


def on_message(payload):
    print_result(payload)


def main():
    global shutdown_event

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║              ОЖИДАНИЕ РЕЗУЛЬТАТОВ ИЗ PIPELINE                   ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print(f"  Очередь: {queue_names.project_results}")
    print("  Ctrl+C для остановки")
    print()
    print("  Ожидаем сообщения...")

    client = Transport()
    client.declare_queue(queue_names.project_results)
    thread = client.consume(queue_names.project_results, on_message)

    def shutdown(sig, frame):
        elapsed = round(time.time() - start_time)
        print()
        print(f"  Остановка. Получено проектов: {received} за {elapsed}с")
        shutdown_event.set()
        client.close()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        shutdown_event.wait()
    except KeyboardInterrupt:
        pass

    print("  Demo receive stopped.")


if __name__ == "__main__":
    main()
