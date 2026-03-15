import os
from dataclasses import dataclass


@dataclass(frozen=True)
class RabbitConfig:
    host: str = os.getenv("RABBIT_HOST", "localhost")
    port: int = int(os.getenv("RABBIT_PORT", 5672))
    user: str = os.getenv("RABBIT_USER", "guest")
    password: str = os.getenv("RABBIT_PASS", "guest")
    heartbeat: int = 600
    blocked_connection_timeout: int = 300


@dataclass(frozen=True)
class QueueNames:
    raw: str = os.getenv("RAW_POSTS_QUEUE", "raw_posts")
    filtered: str = os.getenv("FILTERED_POSTS_QUEUE", "filtered_posts")
    unique: str = os.getenv("UNIQUE_POSTS_QUEUE", "unique_posts")
    embedded: str = os.getenv("EMBEDDED_POSTS_QUEUE", "embedded_posts")
    clustered: str = os.getenv("CLUSTERED_POSTS_QUEUE", "clustered_posts")
    analysis: str = os.getenv("ANALYSIS_QUEUE", "results")
    raw_dlq: str = os.getenv("RAW_POSTS_DLQ", "raw_posts_dlq")
    filtered_dlq: str = os.getenv("FILTERED_POSTS_DLQ", "filtered_posts_dlq")


@dataclass(frozen=True)
class RedisConfig:
    url: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    dedup_ttl: int = int(os.getenv("DEDUP_TTL_SECONDS", 60 * 60 * 24))


@dataclass(frozen=True)
class VectorConfig:
    host: str = os.getenv("QDRANT_HOST", "localhost")
    port: int = int(os.getenv("QDRANT_PORT", 6333))
    collection: str = os.getenv("VECTOR_COLLECTION", "posts")
    embedding_dim: int = int(os.getenv("EMBEDDING_DIM", 512))
    top_k: int = int(os.getenv("CLUSTER_TOP_K", 5))


@dataclass(frozen=True)
class ModelConfig:
    rubert_model: str = os.getenv("RUBERT_MODEL", "DeepPavlov/rubert-base-cased-sentiment")


rabbit_config = RabbitConfig()
queue_names = QueueNames()
redis_config = RedisConfig()
vector_config = VectorConfig()
model_config = ModelConfig()
