"""Tests for shared/preprocessing/filters.py — Russian lemmatization and keyword matching."""

import pytest
from shared.preprocessing.filters import (
    contains_keyword, contains_exclusion, should_process,
    _lemmatize, _lemmatize_token,
)


class TestContainsKeyword:
    def test_exact_lemma_match(self):
        assert contains_keyword("мошенники продают квартиры", ["мошенник"])

    def test_morphological_forms(self):
        """Different forms of 'недвижимость' all match."""
        for form in ["недвижимости", "недвижимостью", "недвижимость"]:
            assert contains_keyword(f"рынок {form} растёт", ["недвижимость"])

    def test_partial_stem_substring(self):
        """Partial keyword 'мошен' matches 'мошенничество' via substring."""
        assert contains_keyword("схема мошенничества раскрыта", ["мошен"])

    def test_no_match_returns_false(self):
        assert not contains_keyword("котики захватили интернет", ["мошен", "недвижимост"])

    def test_empty_keywords_returns_false(self):
        assert not contains_keyword("любой текст", [])

    def test_empty_text(self):
        assert not contains_keyword("", ["мошен"])

    def test_case_insensitive(self):
        assert contains_keyword("МОШЕННИКИ в Москве", ["мошен"])

    def test_non_russian_text_no_crash(self):
        """English text doesn't crash pymorphy3."""
        assert not contains_keyword("Hello world this is a test", ["мошен"])

    def test_emoji_text_no_crash(self):
        assert not contains_keyword("😀🎉🔥💯", ["мошен"])

    def test_special_characters(self):
        """Punctuation-heavy text doesn't crash."""
        assert not contains_keyword("!@#$%^&*(){}[]", ["мошен"])

    def test_keyword_in_title_content(self):
        """Keyword found in mixed text."""
        assert contains_keyword("Новая схема мошенников", ["мошен"])


class TestContainsExclusion:
    def test_basic_exclusion(self):
        assert contains_exclusion("это спам и реклама", ["спам"])

    def test_empty_exclusions_returns_false(self):
        assert not contains_exclusion("любой текст", [])

    def test_no_match(self):
        assert not contains_exclusion("серьёзная статья", ["спам", "реклама"])


class TestShouldProcess:
    def test_relevant_post_passes(self, sample_post):
        assert should_process(sample_post, sample_post["keywords"], sample_post["exclusions"])

    def test_irrelevant_post_rejected(self, sample_post_irrelevant):
        assert not should_process(
            sample_post_irrelevant,
            sample_post_irrelevant["keywords"],
            sample_post_irrelevant["exclusions"],
        )

    def test_excluded_post_rejected(self):
        post = {"title": "мошенники спам", "content": "реклама мошенников"}
        assert not should_process(post, ["мошен"], ["спам"])

    def test_no_keywords_accepts_if_no_exclusion(self):
        post = {"title": "любой текст", "content": ""}
        assert should_process(post, [], [])

    def test_no_keywords_rejects_if_exclusion(self):
        post = {"title": "спам рассылка", "content": ""}
        assert not should_process(post, [], ["спам"])

    def test_empty_title_and_content(self):
        post = {"title": "", "content": ""}
        assert not should_process(post, ["мошен"], [])


class TestLemmatization:
    def test_lemmatize_basic(self):
        result = _lemmatize("мошенники продают квартиры")
        assert "мошенник" in result
        assert "продавать" in result

    def test_lemmatize_token(self):
        assert _lemmatize_token("мошенников") == "мошенник"
