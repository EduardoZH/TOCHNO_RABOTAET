import logging
import uuid
from typing import Optional

import redis

from qdrant_client.models import ScoredPoint

from shared.config.settings import redis_config, vector_config, threshold_config
from shared.vector_store.qdrant_store import QdrantStore


logger = logging.getLogger(__name__)


class ClusterManager:
    def __init__(self, similarity_threshold: float = threshold_config.similarity_threshold):
        self.redis = redis.from_url(redis_config.url, decode_responses=True)
        self.qdrant = QdrantStore()
        self.similarity_threshold = similarity_threshold

    def assign_cluster(self, post_id: str, vector: list, payload: dict) -> str:
        hits = self.qdrant.search(vector, limit=vector_config.top_k)
        best: Optional[ScoredPoint] = None
        best_score = 0.0
        for hit in hits:
            if not hit.payload:
                continue
            if hit.payload.get("cluster_id") is None:
                continue
            score = hit.score or 0.0
            if score > best_score:
                best_score = score
                best = hit

        if best and best_score >= self.similarity_threshold:
            cluster_id = best.payload["cluster_id"]
        else:
            cluster_id = f"cluster-{uuid.uuid4()}"

        metadata_key = f"cluster_meta:{cluster_id}"
        self.redis.hset(metadata_key, mapping={
            "post_id": post_id,
            "cluster_id": cluster_id,
            "last_similarity": str(best_score),
        })
        self.redis.expire(metadata_key, redis_config.dedup_ttl)
        payload["cluster_id"] = cluster_id
        payload["similarity_score"] = best_score
        self.qdrant.upsert(point_id=post_id, vector=vector, payload=payload)
        return cluster_id
