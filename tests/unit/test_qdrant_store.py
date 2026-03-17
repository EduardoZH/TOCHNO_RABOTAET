"""Tests for shared/vector_store/qdrant_store.py — mocked QdrantClient."""

from unittest.mock import patch, MagicMock
import pytest
from qdrant_client.http.exceptions import UnexpectedResponse


@pytest.fixture
def mock_qdrant_client():
    with patch("shared.vector_store.qdrant_store.QdrantClient") as MockClient:
        client_instance = MagicMock()
        MockClient.return_value = client_instance
        yield client_instance


class TestQdrantStore:
    def test_ensure_collection_exists(self, mock_qdrant_client):
        """If collection exists, no create call."""
        mock_qdrant_client.get_collection.return_value = MagicMock()
        from shared.vector_store.qdrant_store import QdrantStore
        store = QdrantStore()
        mock_qdrant_client.create_collection.assert_not_called()

    def test_ensure_collection_creates_on_404(self, mock_qdrant_client):
        resp = MagicMock()
        resp.status_code = 404
        mock_qdrant_client.get_collection.side_effect = UnexpectedResponse(
            status_code=404, reason_phrase="Not Found", content=b"", headers={})
        from shared.vector_store.qdrant_store import QdrantStore
        store = QdrantStore()
        mock_qdrant_client.create_collection.assert_called_once()

    def test_ensure_collection_race_409(self, mock_qdrant_client):
        """Concurrent creation produces 409 — handled gracefully."""
        mock_qdrant_client.get_collection.side_effect = UnexpectedResponse(
            status_code=404, reason_phrase="Not Found", content=b"", headers={})
        mock_qdrant_client.create_collection.side_effect = UnexpectedResponse(
            status_code=409, reason_phrase="Conflict", content=b"", headers={})
        from shared.vector_store.qdrant_store import QdrantStore
        store = QdrantStore()  # Should not raise

    def test_upsert(self, mock_qdrant_client):
        mock_qdrant_client.get_collection.return_value = MagicMock()
        from shared.vector_store.qdrant_store import QdrantStore
        store = QdrantStore()
        store.upsert("point-1", [0.1] * 312, {"key": "value"})
        mock_qdrant_client.upsert.assert_called_once()

    def test_search(self, mock_qdrant_client):
        mock_qdrant_client.get_collection.return_value = MagicMock()
        mock_qdrant_client.search.return_value = []
        from shared.vector_store.qdrant_store import QdrantStore
        store = QdrantStore()
        results = store.search([0.1] * 312, limit=5)
        assert results == []
        mock_qdrant_client.search.assert_called_once()

    def test_get_vector_found(self, mock_qdrant_client):
        mock_qdrant_client.get_collection.return_value = MagicMock()
        point = MagicMock()
        point.vector = [0.5] * 312
        mock_qdrant_client.retrieve.return_value = [point]
        from shared.vector_store.qdrant_store import QdrantStore
        store = QdrantStore()
        vec = store.get_vector("point-1")
        assert vec == [0.5] * 312

    def test_get_vector_not_found(self, mock_qdrant_client):
        mock_qdrant_client.get_collection.return_value = MagicMock()
        mock_qdrant_client.retrieve.return_value = []
        from shared.vector_store.qdrant_store import QdrantStore
        store = QdrantStore()
        vec = store.get_vector("nonexistent")
        assert vec is None

    def test_update_payload(self, mock_qdrant_client):
        mock_qdrant_client.get_collection.return_value = MagicMock()
        from shared.vector_store.qdrant_store import QdrantStore
        store = QdrantStore()
        store.update_payload("point-1", {"cluster_id": "c-1"})
        mock_qdrant_client.set_payload.assert_called_once()
