# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start all services + infrastructure
docker-compose up -d

# Build images
docker-compose build

# Send test messages into the pipeline
PYTHONPATH=. python scripts/send_test_messages.py

# Monitor pipeline output (results queue)
PYTHONPATH=. python scripts/benchmark_pipeline.py

# RabbitMQ management UI: http://localhost:15672 (guest/guest)
# Qdrant dashboard: http://localhost:6333/dashboard
# Redis CLI:
docker exec -it <redis-container> redis-cli
```

## Architecture

A **linear microservices ML pipeline** for brand reputation monitoring of Russian-language social media posts.

### Data Flow

```
raw_posts (RabbitMQ)
    → [Prefilter]   – keyword/exclusion filtering (Russian lemmatization via pymorphy3)
    → filtered_posts
    → [Dedup]       – SimHash fingerprinting + Redis bucket storage, Hamming distance dedup
    → unique_posts
    → [Embedding]   – RuBERT-tiny2 vectors (312-dim), relevancy score vs keyword, stored in Qdrant
    → embedded_posts
    → [Clustering]  – top-K Qdrant search, threshold-based cluster assign/create, metadata in Redis
    → clustered_posts
    → [NLP]         – sentiment analysis (currently STUBBED with random values)
    → results
```

### Infrastructure

| Service  | Port  | Purpose                              |
|----------|-------|--------------------------------------|
| RabbitMQ | 5672 / 15672 | Message broker + management UI |
| Redis    | 6379  | Dedup buckets + cluster metadata (TTL 24h) |
| Qdrant   | 6333  | Vector DB for embeddings + similarity search |

### Service Entry Points

Each service lives in `services/<name>/main.py` and is started by `main.py` at root, which routes by `SERVICE_STAGE` environment variable.

Each service creates **two** `RabbitClient` instances — one consumer, one publisher — to avoid channel conflicts.

### Shared Modules (`shared/`)

- **`config/settings.py`** — frozen dataclasses, all config from env vars with defaults. Key thresholds: `HAMMING_THRESHOLD=3`, `SIMILARITY_THRESHOLD=0.82`
- **`messaging/rabbitmq_client.py`** — single connection wrapper, exponential backoff reconnect, DLQ on error (nack without requeue)
- **`preprocessing/filters.py`** — Russian lemmatization-based keyword/exclusion matching
- **`hashing/simhash.py`** — 64-bit SimHash, bucket generation for Redis sets
- **`embeddings/embedder.py`** — lazy-loaded SentenceTransformer, batch encoding, returns normalized vectors
- **`clustering/cluster_manager.py`** — Qdrant search + Redis metadata, assigns or creates cluster UUID

### Message Payload

Payload is mutated as it flows through the pipeline. The NLP service transforms it to the final API contract:

```json
{
  "clusterId": "uuid",
  "projectId": "...",
  "posts": [{
    "title": "...",
    "content": "...(max 500 chars)...",
    "type": "...",
    "url_string": "...",
    "metrics": { "relevancy": 0-100, "tone": "positive|neutral|negative" }
  }]
}
```

### Known Gaps

- **NLP service is a stub** — returns random sentiment; needs real model integration
- No unit tests exist
- No CI/CD configuration
