"""Tests for shared/embeddings/embedder.py — RuBERT-tiny2 embeddings.

These tests load the real model (~100MB) and are marked slow.
Run with: pytest -m slow
"""

import math
import pytest
from shared.config.settings import vector_config

pytestmark = pytest.mark.slow


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class TestTextToEmbedding:
    def test_dimension(self):
        from shared.embeddings.embedder import text_to_embedding
        emb = text_to_embedding("тестовый текст")
        assert len(emb) == vector_config.embedding_dim  # 312

    def test_empty_text_zero_vector(self):
        from shared.embeddings.embedder import text_to_embedding
        emb = text_to_embedding("")
        assert all(v == 0.0 for v in emb)
        assert len(emb) == vector_config.embedding_dim

    def test_whitespace_only_zero_vector(self):
        from shared.embeddings.embedder import text_to_embedding
        emb = text_to_embedding("   \n\t  ")
        assert all(v == 0.0 for v in emb)

    def test_normalized(self):
        from shared.embeddings.embedder import text_to_embedding
        emb = text_to_embedding("мошенники продают квартиры")
        norm = math.sqrt(sum(x * x for x in emb))
        assert abs(norm - 1.0) < 0.01

    def test_deterministic(self):
        from shared.embeddings.embedder import text_to_embedding
        e1 = text_to_embedding("тест детерминированности")
        e2 = text_to_embedding("тест детерминированности")
        assert e1 == e2

    def test_similar_texts_high_cosine(self):
        from shared.embeddings.embedder import text_to_embedding
        e1 = text_to_embedding("банк мошенники обман")
        e2 = text_to_embedding("мошенничество в банке")
        sim = _cosine(e1, e2)
        assert sim > 0.5, f"Expected > 0.5, got {sim}"

    def test_unrelated_texts_lower_cosine(self):
        from shared.embeddings.embedder import text_to_embedding
        e1 = text_to_embedding("рецепт борща с мясом")
        e2 = text_to_embedding("мошенничество в банке")
        sim = _cosine(e1, e2)
        assert sim < 0.6, f"Expected < 0.6, got {sim}"

    def test_very_long_text(self):
        from shared.embeddings.embedder import text_to_embedding
        long_text = "слово " * 5000  # 5000 words
        emb = text_to_embedding(long_text)
        assert len(emb) == vector_config.embedding_dim


class TestBatchEmbedding:
    def test_batch_returns_correct_count(self):
        from shared.embeddings.embedder import texts_to_embeddings
        texts = ["первый текст", "второй текст", "третий текст"]
        results = texts_to_embeddings(texts)
        assert len(results) == 3
        for emb in results:
            assert len(emb) == vector_config.embedding_dim
