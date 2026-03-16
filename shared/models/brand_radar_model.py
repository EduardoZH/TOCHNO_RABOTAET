import logging
import os

import torch
import torch.nn as nn
import torch.nn.functional as F
from transformers import AutoConfig, AutoTokenizer, BertModel

logger = logging.getLogger(__name__)

SENTIMENT_LABELS = ["negative", "neutral", "positive"]
RELEVANCE_LABELS = ["irrelevant", "relevant"]


class BrandRadarMultiTaskModel(nn.Module):
    def __init__(self, config):
        super().__init__()
        hidden = config.hidden_size  # 312 for rubert-tiny2

        self.bert = BertModel(config)
        self.sentiment_head = nn.Linear(hidden, 3)
        self.relevance_head = nn.Linear(hidden, 2)
        self.risk_head = nn.Sequential(
            nn.Linear(hidden, 128),
            nn.ReLU(),
            nn.Linear(128, 1),
            nn.Sigmoid(),
        )

    def forward(self, input_ids, attention_mask, token_type_ids=None):
        outputs = self.bert(
            input_ids=input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
        )
        pooled = outputs.pooler_output  # [batch, hidden]
        return {
            "sentiment": self.sentiment_head(pooled),
            "relevance": self.relevance_head(pooled),
            "risk": self.risk_head(pooled).squeeze(-1),
        }


class BrandRadarPredictor:
    def __init__(self, model_path: str = "model", device: str = "cpu"):
        self.device = torch.device(
            device if device != "cuda" or torch.cuda.is_available() else "cpu"
        )
        logger.info("Loading BrandRadar model from %s on %s", model_path, self.device)

        config = AutoConfig.from_pretrained(model_path)
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)

        self.model = BrandRadarMultiTaskModel(config)
        weights_path = os.path.join(model_path, "model_weights.pt")
        state_dict = torch.load(weights_path, map_location=self.device, weights_only=True)
        self.model.load_state_dict(state_dict)
        self.model.to(self.device)
        self.model.eval()
        logger.info("BrandRadar model loaded successfully")

    def predict(self, title: str, content: str) -> dict:
        text = f"{title}\n{content}".strip()
        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=256,
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model(**inputs)

        sent_probs = F.softmax(outputs["sentiment"], dim=1)[0]
        rel_probs = F.softmax(outputs["relevance"], dim=1)[0]
        risk_score = outputs["risk"][0].item()

        sent_idx = torch.argmax(sent_probs).item()
        rel_idx = torch.argmax(rel_probs).item()

        return {
            "sentiment_label": SENTIMENT_LABELS[sent_idx],
            "sentiment_score": round(sent_probs[sent_idx].item(), 4),
            "relevance_label": RELEVANCE_LABELS[rel_idx],
            "relevance_score": round(rel_probs[rel_idx].item(), 4),
            "risk_score": round(risk_score, 4),
            "confidence": round(sent_probs[sent_idx].item(), 4),
        }
