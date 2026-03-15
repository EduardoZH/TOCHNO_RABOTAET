# RU MLOps Pipeline

Данный проект должен представлять собой микросервис в рамках одного приложения по мониторингу статуса бренда в сети. Основная концепция состоит в том, что микросервис должен принимать сообщения из очереди RabbitMQ, обрабатывать и возвращать с добавленными метриками анализа. Сообщения - спаршенные из инета статьи, комментарии и отзывы. Сервис принимает сообщения из очереди, дальше должна происходить фильтрация по ключевым словам (оставляем релевантные сообщения). Потом дедупликация, оценка сообщения по трем метрикам: релеватность к бренду, тональность, отношение к кластеру. Соответственно должна быть кластеризация. Ниже представлена существующая реализация. Необходимо довести его до рабочего состояния, чтобы результатом команды docker compose был функционирующий микросервис. Используется Python. Есть возможность переработки реализации

API-контракт:
Вход:
{
    projectId:uuid,
    keywords:[string],
    risk_words:[string],
    posts:[title:string,
     content:string,
     type:string,
     url_string:string]
}

{
    clusterId:uuid
    projectId:uuid,
    posts:[title:string,
     content:string(сжатое),
     type:string,
     url_string:string,
     metrics:[relevancy:int,
            tone:str]]
}

## Как работает проект

1. **Prefilter.** Сервис `services/prefilter_service/main.py` читает сообщения из `raw_posts`, применяет ключи/исключения и публикует подходящие в очередь `filtered_posts`.  
2. **Dedup.** `services/dedup_service/main.py` считывает `filtered_posts`, считает `SimHash` через `shared/hashing/simhash.py`, проверяет Redis по bucket-ключам (`shared/services/dedup_service/redis_store.py`) и пропускает только уникальные сообщения в `unique_posts`.  
3. **Embedding.** `services/embedding_service/main.py` получает `unique_posts`, вычисляет вектор через `shared/embeddings/embedder.py`, индексирует его в Qdrant (`shared/vector_store/qdrant_store.py`) и публикует `embedded_posts`.  
4. **Clustering.** `services/clustering_service/main.py` делает поиск ближних соседей в Qdrant, принимает решение (через `shared/clustering/cluster_manager.py`) объединять сообщение и обновляет Redis-метаданные, затем отправляет `clustered_posts`.  
5. **NLP (stub).** `services/nlp_service/main.py` использует `shared/models/rubert_model.py` — сейчас заглушку — добавляет поля `nlp_analysis` и публикует финальный результат в `results`.

## Связи между сервисами

Raw producer → `raw_posts` → prefilter → `filtered_posts` → dedup → `unique_posts` → embedding → `embedded_posts` → clustering → `clustered_posts` → NLP → `results`.  
RabbitMQ очереди служат буферами; каждый сервис stateless и повторно использует `shared/messaging/rabbitmq_client.py`.

## Сторонние компоненты

- **Redis** хранит SimHash-пальцы и метаданные кластеров (TTL).  
- **Qdrant** хранит embedding'и и возвращает ближайших соседей.

## Инфраструктура

- `Dockerfile` собирает единый образ.  
- `docker-compose.yml` поднимает RabbitMQ, Redis, Qdrant и все сервисы.  
- `main.py` выбирает сервис по `SERVICE_STAGE`, если запускать контейнер из образа напрямую.

## Тестирование

- `scripts/send_test_messages.py` отправляет набор тестовых сообщений в `raw_posts`.  
- `scripts/benchmark_pipeline.py` слушает `results` и логирует результаты.  
- RabbitMQ UI (`localhost:15672`, guest/guest) показывает очереди и DLQ.

## Как запустить

1. `docker-compose build && docker-compose up -d`  
2. `PYTHONPATH=. python3 scripts/send_test_messages.py`  
3. `PYTHONPATH=. python3 scripts/benchmark_pipeline.py`  
4. Проверить очереди через UI, Redis и Qdrant, если нужно.
