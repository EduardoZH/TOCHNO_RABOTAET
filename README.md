# ML Pipeline — Brand Reputation Monitoring

Микросервис мониторинга репутации бренда в русскоязычных соцсетях.

## Архитектура

```
Внешний продюсер
      ↓
RabbitMQ [batch_input]
      ↓ bridge
┌─────────────────────────────────────────────┐
│  mlpipeline (единый контейнер)              │
│                                             │
│  Splitter → Prefilter → Dedup →             │
│  Embedding → Clustering → NLP → Aggregator  │
│                                             │
│  (in-memory очереди внутри)                 │
└─────────────────────────────────────────────┘
      ↓ bridge
RabbitMQ [project_results]
      ↓
Внешний консьюмер
```

**4 контейнера:**
- `rabbitmq` — внешний вход/выход
- `mlpipeline` — все сервисы pipeline
- `redis` — дедупликация (SimHash LSH-бакеты)
- `qdrant` — векторное хранилище

## Путь данных

1. **Splitter** — разбивает batch на отдельные посты, добавляет `post_id`, `total_posts`
2. **Prefilter** — морфологическая фильтрация по keywords/exclusions (pymorphy3), нерелевантные дропаются
3. **Dedup** — SimHash fingerprint + Redis LSH-бакеты, порог Hamming ≤ 3, дубликаты дропаются
4. **Embedding** — векторизация rubert-tiny2 (312-dim), relevancy через cosine similarity с keyword-векторами, вектор сохраняется в Qdrant
5. **Clustering** — ANN поиск в Qdrant top-5, порог similarity ≥ 0.82 → существующий кластер, иначе новый
6. **NLP** — тональность текста (заглушка, будет заменена на реальную модель)
7. **Aggregator** — группирует посты по projectId, отправляет когда собраны все посты или таймаут 2 сек

## API-контракт

**Вход** — RabbitMQ очередь `batch_input`:
```json
{
    "projectId": "uuid",
    "keywords": ["мошен", "афер"],
    "exclusions": ["реклама"],
    "risk_words": ["спам"],
    "posts": [
        {"title": "string", "content": "string", "type": "string", "url": "string"}
    ]
}
```

**Выход** — RabbitMQ очередь `project_results`:
```json
{
    "projectId": "uuid",
    "posts": [
        {
            "title": "string",
            "content": "string (≤500 символов)",
            "type": "string",
            "url": "string",
            "metrics": {
                "relevancy": 55,
                "relevancy_score": 0.55,
                "sentiment": "negative|neutral|positive",
                "sentiment_score": 0.85
            },
            "cluster_id": "uuid"
        }
    ]
}
```

> Посты не прошедшие фильтрацию или дедупликацию в выходе не появятся.

## Запуск

```powershell
docker-compose up -d
```

Дождаться пока поднимется RabbitMQ (healthcheck) и загрузится модель (~15 сек):
```powershell
docker-compose logs -f mlpipeline
# Ждём строку: Ready. Listening on port 8000
```

## Тестирование

### Health check
```powershell
curl http://localhost:8000/health
# {"status": "healthy"}
```

### Отправить batch через HTTP
```powershell
$env:PYTHONPATH="."; python -c "
import urllib.request, json, random
rand = random.randint(10000, 99999)
data = {
    'projectId': f'test-{rand}',
    'keywords': ['мошен', 'афер', 'обман'],
    'exclusions': ['реклама'],
    'risk_words': ['спам'],
    'posts': [
        {'title': f'Мошенники схема {rand}', 'content': f'Аферисты обманули покупателей {rand}', 'type': 'article', 'url': f'http://example.com/{rand}/1'},
        {'title': f'Котики {rand}', 'content': f'Смешные кошки {rand}', 'type': 'post', 'url': f'http://example.com/{rand}/2'},
    ]
}
req = urllib.request.Request('http://localhost:8000/send', json.dumps(data, ensure_ascii=False).encode(), {'Content-Type': 'application/json'})
print(json.loads(urllib.request.urlopen(req).read()))
print('projectId:', data['projectId'])
"
```

### Получить результаты (~16 сек после отправки)
```powershell
curl http://localhost:8000/results
```

> Котики не появятся в результатах — prefilter отфильтрует их как нерелевантные.

### Запуск тестов
```powershell
# Быстрые unit-тесты (~3 сек, без загрузки модели)
$env:PYTHONPATH="."; pytest tests/unit/ -v -m "not slow"

# Все тесты включая validation на датасете (~40 сек)
$env:PYTHONPATH="."; pytest tests/ -v
```

## Мониторинг

```powershell
# Логи pipeline
docker-compose logs -f mlpipeline

# RabbitMQ UI — очереди, глубина, throughput
# http://localhost:15672  (guest / guest)

# Qdrant UI — коллекции, векторы
# http://localhost:6333/dashboard
```

## Конфигурация

| Переменная | Описание | Default |
|---|---|---|
| `RABBIT_HOST` | RabbitMQ хост | `rabbitmq` |
| `REDIS_URL` | Redis для дедупликации | `redis://redis:6379` |
| `QDRANT_HOST` | Qdrant хост | `qdrant` |
| `EMBEDDING_MODEL` | Модель эмбеддингов | `cointegrated/rubert-tiny2` |
| `EMBEDDING_DIM` | Размерность | `312` |
| `SIMILARITY_THRESHOLD` | Порог кластеризации | `0.82` |
| `DEDUP_HAMMING_THRESHOLD` | Порог дедупликации | `3` |
| `AGGREGATOR_TIMEOUT_SEC` | Таймаут агрегатора | `2.0` |
