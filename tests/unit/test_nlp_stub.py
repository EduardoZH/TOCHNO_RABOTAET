"""Tests for shared/models/rubert_model.py — NLP stub contract validation."""

from shared.models.rubert_model import RuBertModel


class TestRuBertModel:
    def setup_method(self):
        self.model = RuBertModel()

    def test_predict_returns_expected_keys(self):
        result = self.model.predict("тестовый текст")
        assert "sentiment_label" in result
        assert "sentiment_score" in result
        assert "confidence" in result

    def test_predict_label_in_known_set(self):
        for _ in range(20):
            result = self.model.predict("текст")
            assert result["sentiment_label"] in ("negative", "neutral", "positive")

    def test_predict_confidence_range(self):
        for _ in range(20):
            result = self.model.predict("текст")
            assert 0.6 <= result["confidence"] <= 0.99

    def test_predict_score_matches_label(self):
        result = self.model.predict("текст")
        expected_scores = {"negative": 0.15, "neutral": 0.5, "positive": 0.85}
        assert result["sentiment_score"] == expected_scores[result["sentiment_label"]]
