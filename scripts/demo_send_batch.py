"""
Отправляет batch-сообщение по API-контракту через splitter-сервис.
Запуск: $env:PYTHONPATH="."; .venv\Scripts\python scripts\demo_send_batch.py
"""
import json
import time

from shared.messaging.rabbitmq_client import RabbitClient

BATCH_QUEUE = "batch_input"

BATCH_MESSAGE = {
    "projectId": "demo-project-batch",
    "keywords": ["мошен", "недвижимост", "афер", "квартир"],
    "risk_words": ["спам", "реклама"],
    "posts": [
        {
            "title": "Мошенники продают несуществующие квартиры",
            "content": "Аферисты обманули десятки покупателей недвижимости в Москве.",
            "type": "article",
            "url_string": "https://news.example.com/fraud-1",
        },
        {
            "title": "Рынок недвижимости бьёт рекорды",
            "content": "Цены на квартиры выросли на 15% за год.",
            "type": "article",
            "url_string": "https://news.example.com/market-2",
        },
        {
            "title": "Котики захватили интернет",
            "content": "Смешные видео с кошками набирают миллионы просмотров.",
            "type": "post",
            "url_string": "https://cats.example.com/viral-5",
        },
    ],
}


def main():
    client = RabbitClient()
    client.declare_queue(BATCH_QUEUE)

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║         ОТПРАВКА BATCH-СООБЩЕНИЯ ЧЕРЕЗ SPLITTER                ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print(f"  Проект:          {BATCH_MESSAGE['projectId']}")
    print(f"  Ключевые слова:  {BATCH_MESSAGE['keywords']}")
    print(f"  Risk words:      {BATCH_MESSAGE['risk_words']}")
    print(f"  Постов в batch:  {len(BATCH_MESSAGE['posts'])}")
    print()

    client.publish(BATCH_QUEUE, BATCH_MESSAGE)

    print("  >>> Batch отправлен в очередь: batch_input")
    print("  Splitter разобьёт на отдельные посты → raw_posts → pipeline")
    print("  Смотри результаты в demo_receive.py")
    print()


if __name__ == "__main__":
    main()
