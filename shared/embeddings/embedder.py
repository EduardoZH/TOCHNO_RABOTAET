import logging
import threading
from typing import List

import numpy as np
from sentence_transformers import SentenceTransformer

from shared.config.settings import model_config, vector_config

logger = logging.getLogger(__name__)

_model: SentenceTransformer | None = None
_model_lock = threading.Lock()


def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        with _model_lock:
            if _model is None:
                logger.info("Loading embedding model: %s (version %s)",
                            model_config.embedding_model,
                            model_config.embedding_model_version)
                _model = SentenceTransformer(model_config.embedding_model)
    return _model


def text_to_embedding(text: str) -> List[float]:
    if not text or not text.strip():
        return [0.0] * vector_config.embedding_dim
    model = _get_model()
    embedding = model.encode(text, normalize_embeddings=True)
    return embedding.tolist()


def texts_to_embeddings(texts: List[str]) -> List[List[float]]:
    model = _get_model()
    embeddings = model.encode(texts, normalize_embeddings=True, batch_size=32)
    return [e.tolist() for e in embeddings]


# ── Fallback: hash-based bag-of-words (when model unavailable) ──

def _hash_fallback(text: str) -> List[float]:
    """Deterministic hash-based BoW fallback. No model needed, 312-dim."""
    dim = vector_config.embedding_dim
    vector = [0.0] * dim
    if not text or not text.strip():
        return vector
    for token in text.lower().split():
        idx = hash(token) % dim
        vector[idx] += 1.0
    norm = sum(x * x for x in vector) ** 0.5
    if norm > 0:
        vector = [x / norm for x in vector]
    return vector


def text_to_embedding_safe(text: str) -> tuple[List[float], bool]:
    """Returns (embedding, is_fallback). Uses hash fallback if model fails."""
    try:
        return text_to_embedding(text), False
    except Exception:
        logger.warning("Embedding model failed, using hash-based fallback")
        return _hash_fallback(text), True
