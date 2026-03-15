import hashlib
from typing import List

import numpy as np

from shared.config.settings import vector_config


def text_to_embedding(text: str, dim: int = vector_config.embedding_dim) -> List[float]:
    vector = np.zeros(dim, dtype=float)
    tokens = [token for token in text.split() if token]
    if not tokens:
        return vector.tolist()

    for idx, token in enumerate(tokens):
        token_hash = int(hashlib.md5(token.encode()).hexdigest(), 16)
        position = idx % dim
        vector[position] += (token_hash % 1000) / 1000.0

    norm = np.linalg.norm(vector)
    if norm == 0:
        return vector.tolist()
    return (vector / norm).tolist()
