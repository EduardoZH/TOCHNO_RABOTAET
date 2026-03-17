import logging
from typing import Any, Dict, List

from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.http.models import Distance, PointStruct, VectorParams

from shared.config.settings import vector_config

logger = logging.getLogger(__name__)


class QdrantStore:
    def __init__(self):
        self.client = QdrantClient(host=vector_config.host, port=vector_config.port,
                                   timeout=10)
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        try:
            self.client.get_collection(vector_config.collection)
        except UnexpectedResponse as e:
            if e.status_code == 404:
                try:
                    self.client.create_collection(
                        collection_name=vector_config.collection,
                        vectors_config=VectorParams(size=vector_config.embedding_dim, distance=Distance.COSINE),
                    )
                    logger.info("Created Qdrant collection '%s'", vector_config.collection)
                except UnexpectedResponse as ce:
                    if ce.status_code == 409:
                        logger.info("Collection '%s' already created by another service", vector_config.collection)
                    else:
                        raise
            else:
                raise

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

    def get_vector(self, point_id: str) -> List[float] | None:
        results = self.client.retrieve(
            collection_name=vector_config.collection,
            ids=[point_id],
            with_vectors=True,
            with_payload=False,
        )
        if results:
            return results[0].vector
        return None

    def update_payload(self, point_id: str, payload: Dict[str, Any]) -> None:
        self.client.set_payload(
            collection_name=vector_config.collection,
            payload=payload,
            points=[point_id],
        )
