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
    reconnect_delay: float = float(os.getenv("RABBIT_RECONNECT_DELAY", 2.0))
    reconnect_max_delay: float = float(os.getenv("RABBIT_RECONNECT_MAX_DELAY", 60.0))


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
    embedding_dim: int = int(os.getenv("EMBEDDING_DIM", 312))
    top_k: int = int(os.getenv("CLUSTER_TOP_K", 5))


@dataclass(frozen=True)
class ModelConfig:
    rubert_model: str = os.getenv("RUBERT_MODEL", "DeepPavlov/rubert-base-cased-sentiment")
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "cointegrated/rubert-tiny2")


@dataclass(frozen=True)
class ThresholdConfig:
    similarity_threshold: float = float(os.getenv("SIMILARITY_THRESHOLD", 0.82))
    dedup_hamming_threshold: int = int(os.getenv("DEDUP_HAMMING_THRESHOLD", 3))


rabbit_config = RabbitConfig()
queue_names = QueueNames()
redis_config = RedisConfig()
vector_config = VectorConfig()
model_config = ModelConfig()
threshold_config = ThresholdConfig()
