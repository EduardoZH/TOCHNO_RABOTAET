"""Tests for shared/clustering/cluster_manager.py."""

from unittest.mock import MagicMock, patch
import pytest
import redis as redis_lib

from shared.clustering.cluster_manager import ClusterManager


@pytest.fixture
def mock_qdrant():
    store = MagicMock()
    store.search.return_value = []
    store.update_payload.return_value = None
    return store


@pytest.fixture
def mock_redis_client():
    client = MagicMock()
    return client


@pytest.fixture
def manager(mock_qdrant, mock_redis_client):
    mgr = ClusterManager.__new__(ClusterManager)
    mgr.qdrant = mock_qdrant
    mgr.redis = mock_redis_client
    mgr.similarity_threshold = 0.82
    return mgr


def _make_hit(cluster_id, score):
    hit = MagicMock()
    hit.payload = {"cluster_id": cluster_id}
    hit.score = score
    return hit


class TestClusterManager:
    def test_assign_existing_cluster(self, manager, mock_qdrant):
        mock_qdrant.search.return_value = [_make_hit("cluster-abc", 0.90)]
        payload = {}
        cid = manager.assign_cluster("post-1", [0.1] * 312, payload)
        assert cid == "cluster-abc"
        assert payload["cluster_id"] == "cluster-abc"
        assert payload["similarity_score"] == 0.90

    def test_create_new_cluster_below_threshold(self, manager, mock_qdrant):
        mock_qdrant.search.return_value = [_make_hit("cluster-abc", 0.50)]
        payload = {}
        cid = manager.assign_cluster("post-1", [0.1] * 312, payload)
        assert cid.startswith("cluster-")
        assert cid != "cluster-abc"

    def test_create_new_cluster_no_hits(self, manager, mock_qdrant):
        mock_qdrant.search.return_value = []
        payload = {}
        cid = manager.assign_cluster("post-1", [0.1] * 312, payload)
        assert cid.startswith("cluster-")

    def test_redis_metadata_updated(self, manager, mock_qdrant, mock_redis_client):
        mock_qdrant.search.return_value = [_make_hit("cluster-abc", 0.90)]
        manager.assign_cluster("post-1", [0.1] * 312, {})
        mock_redis_client.hincrby.assert_called_once()
        mock_redis_client.hset.assert_called_once()

    def test_redis_unavailable_graceful(self, manager, mock_qdrant, mock_redis_client):
        """When Redis raises ConnectionError, cluster assignment still works."""
        mock_qdrant.search.return_value = [_make_hit("cluster-abc", 0.90)]
        mock_redis_client.hincrby.side_effect = redis_lib.ConnectionError("down")
        payload = {}
        cid = manager.assign_cluster("post-1", [0.1] * 312, payload)
        assert cid == "cluster-abc"  # still assigned despite Redis failure

    def test_qdrant_payload_updated(self, manager, mock_qdrant):
        mock_qdrant.search.return_value = []
        manager.assign_cluster("post-1", [0.1] * 312, {})
        mock_qdrant.update_payload.assert_called_once()

    def test_custom_threshold(self, mock_qdrant, mock_redis_client):
        mgr = ClusterManager.__new__(ClusterManager)
        mgr.qdrant = mock_qdrant
        mgr.redis = mock_redis_client
        mgr.similarity_threshold = 0.50
        mock_qdrant.search.return_value = [_make_hit("cluster-abc", 0.60)]
        payload = {}
        cid = mgr.assign_cluster("post-1", [0.1] * 312, payload)
        assert cid == "cluster-abc"  # 0.60 >= 0.50 threshold
