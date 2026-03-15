# RU MLOps Pipeline

Микросервис мониторинга репутации бренда. Принимает сообщения из RabbitMQ, обрабатывает через pipeline из 6 сервисов и возвращает с метриками анализа.

## API-контракт

**Вход** (очередь `batch_input` — через splitter-сервис):
```json
{
    "projectId": "uuid",
    "keywords": ["string"],
    "risk_words": ["string"],
    "posts": [
        {"title": "string", "content": "string", "type": "string", "url_string": "string"}
    ]
}
```

Также поддерживается прямая отправка отдельных постов в `raw_posts`:
```json
{
    "post_id": "uuid",
    "projectId": "uuid",
    "keywords": ["string"],
    "exclusions": ["string"],
    "title": "string",
    "content": "string",
    "type": "string",
    "url_string": "string",
    "timestamp": 1234567890.0
}
```

**Выход** (очередь `results`):
```json
{
    "clusterId": "uuid",
    "projectId": "uuid",
    "posts": [
        {
            "title": "string",
            "content": "string (до 500 символов)",
            "type": "string",
            "url_string": "string",
            "metrics": {"relevancy": 0-100, "tone": "negative|neutral|positive"}
        }
    ]
}
```

## Как работает проект

```
batch_input → [SPLITTER] → raw_posts → [PREFILTER] → filtered_posts → [DEDUP] → unique_posts → [EMBEDDING] → embedded_posts → [CLUSTERING] → clustered_posts → [NLP] → results
```

1. **Splitter.** Принимает batch по API-контракту, разбивает на отдельные посты в `raw_posts`.
2. **Prefilter.** Фильтрация по ключевым словам (морфология через pymorphy3) и исключениям.
3. **Dedup.** SimHash + LSH-бакеты в Redis. Фильтрует дубликаты по title+content.
4. **Embedding.** Векторизация через `sentence-transformers` (rubert-tiny2, 312 dim). Сохраняет в Qdrant. Вычисляет relevancy (max cosine similarity к каждому ключевому слову).
5. **Clustering.** Ищет ближайших соседей в Qdrant, назначает кластер. Вектор получает из Qdrant по point_id (не через RabbitMQ).
6. **NLP (stub).** Заглушка для тональности — будет заменена на реальную модель.

## Надёжность

- RabbitMQ: reconnection с exponential backoff
- Dead Letter Queues (DLQ): при ошибке обработки сообщения попадают в `*_dlq` очереди
- Redis: graceful degradation — при недоступности dedup пропускает посты (лучше дубли, чем потеря)
- Qdrant: безопасное создание коллекции (только при 404, без удаления существующей)
- Graceful shutdown: `stop_grace_period=10s`, корректное завершение consumer threads

## Инфраструктура

- `Dockerfile` — единый образ для всех сервисов (CPU по умолчанию, комменты для GPU)
- `docker-compose.yml` — RabbitMQ, Redis, Qdrant + 6 сервисов с resource limits
- `main.py` — роутер по `SERVICE_STAGE` (ленивый импорт, загружает только нужный сервис)

## Как запустить

```bash
docker-compose build && docker-compose up -d
```

## Тестирование

```bash
# Отправка отдельных постов
PYTHONPATH=. python scripts/demo_send.py

# Отправка batch через splitter
PYTHONPATH=. python scripts/demo_send_batch.py

# Приём результатов
PYTHONPATH=. python scripts/demo_receive.py
```

RabbitMQ UI: `localhost:15672` (guest/guest)
