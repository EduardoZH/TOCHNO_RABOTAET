"""
Слушает очередь results и наглядно показывает выходные данные pipeline.
Запуск: $env:PYTHONPATH="."; .venv\Scripts\python scripts\demo_receive.py
"""
import json
import signal
import sys
import time

import pika

from shared.config.settings import queue_names

received = 0
start_time = time.time()


def tone_icon(tone: str) -> str:
    return {"negative": "🔴 negative", "neutral": "🟡 neutral", "positive": "🟢 positive"}.get(tone, f"? {tone}")


def relevancy_bar(score: int) -> str:
    filled = score // 10
    bar = "█" * filled + "░" * (10 - filled)
    return f"[{bar}] {score}%"


def print_result(body: bytes):
    global received
    received += 1
    msg = json.loads(body)

    cluster_id = str(msg.get("clusterId", "N/A"))
    project_id = msg.get("projectId", "N/A")
    posts = msg.get("posts", [{}])
    post = posts[0] if posts else {}
    metrics = post.get("metrics", {})
    relevancy = metrics.get("relevancy", 0)
    tone = metrics.get("tone", "unknown")
    title = post.get("title", "")
    content = post.get("content", "")

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print(f"║  РЕЗУЛЬТАТ #{received:<59}║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║  projectId:  {project_id:<55}║")
    print(f"║  clusterId:  {cluster_id[:55]:<55}║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║  Заголовок:  {title[:55]:<55}║")
    print(f"║  Контент:    {content[:55]:<55}║")
    if len(content) > 55:
        print(f"║              {content[55:110]:<55}║")
    print("╠══════════════════════════════════════════════════════════════════╣")
    print(f"║  МЕТРИКИ:                                                        ║")
    print(f"║    Тональность: {tone_icon(tone):<52}║")
    rel_bar = relevancy_bar(relevancy)
    print(f"║    Релевантность: {rel_bar:<50}║")
    print("╚══════════════════════════════════════════════════════════════════╝")


def on_message(ch, method, properties, body):
    print_result(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║              ОЖИДАНИЕ РЕЗУЛЬТАТОВ ИЗ PIPELINE                   ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print(f"  Очередь: {queue_names.analysis}")
    print("  Ctrl+C для остановки")
    print()
    print("  Ожидаем сообщения...")

    creds = pika.PlainCredentials("guest", "guest")
    conn = pika.BlockingConnection(
        pika.ConnectionParameters("localhost", 5672, credentials=creds)
    )
    ch = conn.channel()
    ch.queue_declare(queue=queue_names.analysis, durable=True)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=queue_names.analysis, on_message_callback=on_message)

    def shutdown(sig, frame):
        elapsed = round(time.time() - start_time)
        print()
        print(f"  Остановка. Получено результатов: {received} за {elapsed}с")
        ch.stop_consuming()

    signal.signal(signal.SIGINT, shutdown)

    try:
        ch.start_consuming()
    except Exception:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
