"""Tests for services/splitter_service/main.py — batch splitting logic."""

import json
from unittest.mock import MagicMock

from services.splitter_service.main import _make_handler


class FakeTransport:
    """Mock transport for testing."""
    def __init__(self):
        self.published = []

    def publish(self, queue, payload):
        self.published.append({"queue": queue, "payload": payload})

    def declare_queue(self, name, dlq_name=None):
        pass

    def consume(self, queue, callback, dlq_name=None):
        pass

    def close(self):
        pass


class TestSplitterHandler:
    def setup_method(self):
        self.transport = FakeTransport()
        self.handler = _make_handler(self.transport)

    def _call_handler(self, batch: dict):
        body = json.dumps(batch, ensure_ascii=False).encode()
        method = MagicMock()
        self.handler(MagicMock(), method, None, body)

    def test_split_batch_creates_individual_messages(self):
        batch = {
            "projectId": "proj-1",
            "keywords": ["мошен"],
            "risk_words": ["спам"],
            "posts": [
                {"title": "Пост 1", "content": "Текст 1", "type": "article", "url": "http://a"},
                {"title": "Пост 2", "content": "Текст 2", "type": "post", "url": "http://b"},
                {"title": "Пост 3", "content": "Текст 3", "type": "article", "url": "http://c"},
            ],
        }
        self._call_handler(batch)
        assert len(self.transport.published) == 3

    def test_split_empty_batch_no_publish(self):
        batch = {"projectId": "proj-1", "keywords": [], "posts": []}
        self._call_handler(batch)
        assert len(self.transport.published) == 0

    def test_split_preserves_keywords_and_exclusions(self):
        batch = {
            "projectId": "proj-1",
            "keywords": ["мошен", "афер"],
            "risk_words": ["спам"],
            "posts": [{"title": "T", "content": "C", "type": "t", "url": "u"}],
        }
        self._call_handler(batch)
        payload = self.transport.published[0]["payload"]
        assert payload["keywords"] == ["мошен", "афер"]
        assert payload["exclusions"] == ["спам"]
        assert payload["projectId"] == "proj-1"
        assert "post_id" in payload
        assert "timestamp" in payload
        assert payload["total_posts"] == 1
