"""Validation: embedding quality — cosine similarity analysis.

Marked slow because it loads the SentenceTransformer model.
"""

import math
import pytest

pytestmark = [pytest.mark.validation, pytest.mark.slow]


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


class TestEmbeddingQuality:
    def test_relevancy_distribution(self, dataset_sample):
        """Relevant posts should have higher mean relevancy than irrelevant."""
        if dataset_sample is None:
            pytest.skip("Dataset not available")

        from shared.embeddings.embedder import text_to_embedding

        keywords = ["мошен", "афер", "обман", "банк"]
        kw_emb = text_to_embedding(" ".join(keywords))

        relevant_scores = []
        irrelevant_scores = []

        count = 0
        for _, row in dataset_sample.iterrows():
            text = str(row.get("Текст", ""))
            if len(text) < 10:
                continue
            relevance = str(row.get("Релевантность", "Нерелевант"))
            emb = text_to_embedding(text[:500])  # truncate for speed
            score = _cosine(emb, kw_emb) * 100

            if relevance != "Нерелевант":
                relevant_scores.append(score)
            else:
                irrelevant_scores.append(score)
            count += 1
            if count >= 50:
                break

        if relevant_scores and irrelevant_scores:
            mean_rel = sum(relevant_scores) / len(relevant_scores)
            mean_irr = sum(irrelevant_scores) / len(irrelevant_scores)
            print(f"\nEmbedding Relevancy Distribution:")
            print(f"  Relevant posts ({len(relevant_scores)}): mean={mean_rel:.1f}")
            print(f"  Irrelevant posts ({len(irrelevant_scores)}): mean={mean_irr:.1f}")
            print(f"  Separation: {mean_rel - mean_irr:.1f}")

    def test_same_topic_high_similarity(self):
        """Posts about same topic should have high cosine similarity."""
        from shared.embeddings.embedder import text_to_embedding

        fraud_1 = text_to_embedding("Мошенники обманули покупателей квартир в Москве")
        fraud_2 = text_to_embedding("Аферисты продавали несуществующую недвижимость")
        cats = text_to_embedding("Котики играют с мячиком во дворе")

        sim_same = _cosine(fraud_1, fraud_2)
        sim_diff = _cosine(fraud_1, cats)

        print(f"\nCosine similarity:")
        print(f"  Same topic (fraud): {sim_same:.3f}")
        print(f"  Different topics:   {sim_diff:.3f}")

        assert sim_same > sim_diff, "Same-topic similarity should be higher"

    def test_edge_cases_no_crash(self):
        """Various edge case texts produce valid embeddings without crashing."""
        from shared.embeddings.embedder import text_to_embedding
        from shared.config.settings import vector_config

        cases = [
            ("single_word", "мошенник"),
            ("emoji_only", "😀🎉🔥"),
            ("english", "Hello world fraud alert"),
            ("mixed", "Alert! Мошенники 🚨 fraud"),
            ("numbers", "12345 67890"),
        ]

        for name, text in cases:
            emb = text_to_embedding(text)
            assert len(emb) == vector_config.embedding_dim, f"Failed for case: {name}"
