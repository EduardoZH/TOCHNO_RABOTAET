"""Tests for services/dedup_service/redis_store.py."""

from unittest.mock import patch, MagicMock
import pytest
import redis as redis_lib


class TestRedisStore:
    def test_add_and_members(self):
        """Using fakeredis for real Redis behavior without a server."""
        try:
            import fakeredis
        except ImportError:
            pytest.skip("fakeredis not installed")

        with patch("services.dedup_service.redis_store.redis") as mock_redis_mod:
            fake_server = fakeredis.FakeServer()
            fake_client = fakeredis.FakeRedis(server=fake_server, decode_responses=True)
            mock_redis_mod.from_url.return_value = fake_client
            mock_redis_mod.ConnectionError = redis_lib.ConnectionError

            from services.dedup_service.redis_store import RedisStore
            store = RedisStore()
            store.client = fake_client
            store.add_bucket("bucket:test", "12345")
            result = store.members("bucket:test")
            assert "12345" in result

    def test_members_empty_bucket(self):
        try:
            import fakeredis
        except ImportError:
            pytest.skip("fakeredis not installed")

        with patch("services.dedup_service.redis_store.redis") as mock_redis_mod:
            fake_client = fakeredis.FakeRedis(decode_responses=True)
            mock_redis_mod.from_url.return_value = fake_client
            mock_redis_mod.ConnectionError = redis_lib.ConnectionError

            from services.dedup_service.redis_store import RedisStore
            store = RedisStore()
            store.client = fake_client
            result = store.members("nonexistent")
            assert result == set()

    def test_connection_error_members_returns_empty(self):
        """When Redis is down, members() returns empty set."""
        from services.dedup_service.redis_store import RedisStore
        store = RedisStore.__new__(RedisStore)
        store.client = MagicMock()
        store.client.smembers.side_effect = redis_lib.ConnectionError("connection refused")
        result = store.members("bucket:test")
        assert result == set()

    def test_connection_error_add_no_raise(self):
        """When Redis is down, add_bucket() logs but doesn't raise."""
        from services.dedup_service.redis_store import RedisStore
        store = RedisStore.__new__(RedisStore)
        store.client = MagicMock()
        store.client.sadd.side_effect = redis_lib.ConnectionError("connection refused")
        store.add_bucket("bucket:test", "12345")  # Should not raise
