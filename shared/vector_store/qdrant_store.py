from typing import Any, Dict, List

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams

from shared.config.settings import vector_config


class QdrantStore:
    def __init__(self):
        self.client = QdrantClient(host=vector_config.host, port=vector_config.port)
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        try:
            self.client.get_collection(vector_config.collection)
        except Exception:
            self.client.recreate_collection(
                collection_name=vector_config.collection,
                vectors_config=VectorParams(size=vector_config.embedding_dim, distance=Distance.COSINE),
            )

    def upsert(self, point_id: str, vector: List[float], payload: Dict[str, Any]) -> None:
        self.client.upsert(
            collection_name=vector_config.collection,
            points=[PointStruct(id=point_id, vector=vector, payload=payload)],
        )

    def search(self, vector: List[float], limit: int = 5):
        return self.client.search(
            collection_name=vector_config.collection,
            query_vector=vector,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )
