"""
Отправляет тестовые посты в pipeline и наглядно показывает входные данные.
Запуск: $env:PYTHONPATH="."; .venv\Scripts\python scripts\demo_send.py
"""
import json
import time
import uuid

from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient

POSTS = [
    {
        "title": "Мошенники продают несуществующие квартиры",
        "content": "Аферисты обманули десятки покупателей недвижимости в Москве. Жертвы лишились сбережений.",
        "type": "article",
        "url_string": "https://news.example.com/fraud-1",
    },
    {
        "title": "Рынок недвижимости в Москве бьёт рекорды",
        "content": "Цены на квартиры выросли на 15% за год. Эксперты связывают рост с ипотечными программами.",
        "type": "article",
        "url_string": "https://news.example.com/market-2",
    },
    {
        "title": "Мошенническая схема с арендой жилья раскрыта",
        "content": "Полиция задержала группу аферистов, сдававших одни и те же квартиры разным людям одновременно.",
        "type": "post",
        "url_string": "https://news.example.com/fraud-3",
    },
    {
        "title": "Новый ЖК открылся в Подмосковье",
        "content": "Застройщик сдал 500 квартир. Инфраструктура включает школы и детские сады.",
        "type": "article",
        "url_string": "https://news.example.com/new-complex-4",
    },
    {
        "title": "Котики захватили интернет",
        "content": "Смешные видео с кошками набирают миллионы просмотров. Зоопсихологи объясняют феномен.",
        "type": "post",
        "url_string": "https://cats.example.com/viral-5",
    },
]

KEYWORDS = ["мошен", "недвижимост", "афер", "квартир"]
EXCLUSIONS = ["спам", "реклама"]


def print_separator():
    print("─" * 70)


def main():
    client = RabbitClient()
    client.declare_queue(queue_names.raw)

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║              ОТПРАВКА ТЕСТОВЫХ ПОСТОВ В PIPELINE                ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print(f"  Ключевые слова: {KEYWORDS}")
    print(f"  Исключения:     {EXCLUSIONS}")
    print(f"  Всего постов:   {len(POSTS)}")
    print()

    for i, post_data in enumerate(POSTS, 1):
        post_id = str(uuid.uuid4())
        message = {
            "post_id": post_id,
            "title": post_data["title"],
            "content": post_data["content"],
            "type": post_data["type"],
            "url_string": post_data["url_string"],
            "keywords": KEYWORDS,
            "exclusions": EXCLUSIONS,
            "timestamp": time.time(),
            "projectId": "demo-project",
        }

        print_separator()
        print(f"  ПОСТ #{i}")
        print(f"  ID:       {post_id[:8]}...")
        print(f"  Тип:      {post_data['type']}")
        print(f"  Заголовок: {post_data['title']}")
        print(f"  Текст:    {post_data['content'][:80]}...")
        print()
        print(f"  >>> Отправлено в очередь: {queue_names.raw}")

        client.publish(queue_names.raw, message)
        time.sleep(1.5)

    print_separator()
    print()
    print("  Все посты отправлены!")
    print("  Смотри результаты в demo_receive.py")
    print()


if __name__ == "__main__":
    main()
