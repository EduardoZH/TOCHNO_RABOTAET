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
                logger.info("Loading embedding model: %s", model_config.embedding_model)
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
