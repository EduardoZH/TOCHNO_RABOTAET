"""Validation: boundary cases — where the pipeline breaks or degrades.

Tests model limitations and error handling at system boundaries.
"""

import json
import pytest
from collections import defaultdict

from shared.preprocessing.filters import should_process, contains_keyword
from shared.hashing.simhash import compute_simhash


class TestEmptyInputs:
    def test_empty_text_prefilter(self):
        post = {"title": "", "content": ""}
        assert not should_process(post, ["мошен"], [])

    def test_none_fields_prefilter(self):
        post = {"title": None, "content": None}
        # should_process uses .get() with defaults, but None is not ""
        # This tests graceful handling
        try:
            result = should_process(post, ["мошен"], [])
            assert isinstance(result, bool)
        except (TypeError, AttributeError):
            # Documented limitation: None fields may cause TypeError
            pass

    def test_empty_text_simhash(self):
        assert compute_simhash("") == 0

    def test_empty_text_dedup(self):
        from services.dedup_service.deduplicator import Deduplicator

        class FakeStore:
            def __init__(self):
                self._b = defaultdict(set)
            def add_bucket(self, b, v):
                self._b[b].add(v)
            def members(self, b):
                return self._b.get(b, set())

        dedup = Deduplicator(FakeStore(), threshold=3)
        is_dup, fp = dedup.is_duplicate("")
        assert not is_dup
        assert fp == 0


class TestVeryLongText:
    def test_long_text_prefilter(self):
        long_content = "мошенники " * 5000  # ~50K chars
        post = {"title": "", "content": long_content}
        result = should_process(post, ["мошен"], [])
        assert result is True

    def test_long_text_simhash(self):
        long_text = "слово " * 5000
        h = compute_simhash(long_text)
        assert isinstance(h, int)


class TestNonRussianText:
    def test_english_text_prefilter(self):
        post = {"title": "Breaking news about fraud", "content": "Scammers caught"}
        result = should_process(post, ["мошен"], [])
        assert not result  # English doesn't match Russian keywords

    def test_chinese_text_prefilter(self):
        post = {"title": "诈骗案件", "content": "受害者被骗"}
        result = should_process(post, ["мошен"], [])
        assert not result

    def test_english_simhash_no_crash(self):
        h = compute_simhash("The quick brown fox jumps over the lazy dog")
        assert isinstance(h, int)


class TestSpecialCharacters:
    def test_sql_injection_string(self):
        post = {"title": "'; DROP TABLE posts; --", "content": "' OR 1=1 --"}
        result = should_process(post, ["мошен"], [])
        assert isinstance(result, bool)

    def test_html_tags(self):
        post = {"title": "<script>alert('xss')</script>", "content": "<b>мошенники</b>"}
        result = should_process(post, ["мошен"], [])
        # HTML tags are treated as text — keyword might be inside tags
        assert isinstance(result, bool)

    def test_null_bytes(self):
        post = {"title": "тест\x00нулевой\x00байт", "content": "мошенники"}
        result = should_process(post, ["мошен"], [])
        assert result is True

    def test_emoji_heavy_text(self):
        post = {"title": "😀🎉🔥💯🚨", "content": "🤖🦊🐱‍👤"}
        assert not should_process(post, ["мошен"], [])
        h = compute_simhash("😀🎉🔥💯🚨")
        assert isinstance(h, int)


class TestMalformedPayload:
    def test_missing_title(self):
        post = {"content": "мошенники"}
        result = should_process(post, ["мошен"], [])
        assert result is True

    def test_missing_content(self):
        post = {"title": "мошенники"}
        result = should_process(post, ["мошен"], [])
        assert result is True

    def test_completely_empty_post(self):
        post = {}
        result = should_process(post, ["мошен"], [])
        assert not result
