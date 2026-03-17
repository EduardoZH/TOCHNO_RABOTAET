import random


class RuBertModel:
    def __init__(self):
        self.label_map = {
            "negative": 0.15,
            "neutral": 0.5,
            "positive": 0.85,
        }

    def predict(self, text: str):
        tone = random.choice(list(self.label_map.keys()))
        return {
            "sentiment_label": tone,
            "sentiment_score": self.label_map[tone],
            "confidence": round(random.uniform(0.6, 0.99), 2),
        }
