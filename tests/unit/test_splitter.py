"""Tests for services/splitter_service/main.py — batch splitting logic."""

import json
from unittest.mock import MagicMock

from services.splitter_service.main import _make_handler


class TestSplitterHandler:
    def setup_method(self):
        self.handler = _make_handler()
        self.ch = MagicMock()

    def _call_handler(self, batch: dict):
        body = json.dumps(batch, ensure_ascii=False).encode()
        method = MagicMock()
        self.handler(self.ch, method, None, body)

    def test_split_batch_creates_individual_messages(self):
        batch = {
            "projectId": "proj-1",
            "keywords": ["мошен"],
            "risk_words": ["спам"],
            "posts": [
                {"title": "Пост 1", "content": "Текст 1", "type": "article", "url_string": "http://a"},
                {"title": "Пост 2", "content": "Текст 2", "type": "post", "url_string": "http://b"},
                {"title": "Пост 3", "content": "Текст 3", "type": "article", "url_string": "http://c"},
            ],
        }
        self._call_handler(batch)
        assert self.ch.basic_publish.call_count == 3

    def test_split_empty_batch_no_publish(self):
        batch = {"projectId": "proj-1", "keywords": [], "posts": []}
        self._call_handler(batch)
        self.ch.basic_publish.assert_not_called()

    def test_split_preserves_keywords_and_exclusions(self):
        batch = {
            "projectId": "proj-1",
            "keywords": ["мошен", "афер"],
            "risk_words": ["спам"],
            "posts": [{"title": "T", "content": "C", "type": "t", "url_string": "u"}],
        }
        self._call_handler(batch)
        call_args = self.ch.basic_publish.call_args
        body = json.loads(call_args.kwargs["body"])
        assert body["keywords"] == ["мошен", "афер"]
        assert body["exclusions"] == ["спам"]
        assert body["projectId"] == "proj-1"
        assert "post_id" in body
        assert "timestamp" in body
