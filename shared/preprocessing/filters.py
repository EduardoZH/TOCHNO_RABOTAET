import logging
from typing import Sequence

import pymorphy3

logger = logging.getLogger(__name__)

_morph = pymorphy3.MorphAnalyzer()


def _lemmatize(text: str) -> str:
    tokens = text.lower().split()
    lemmas = [_morph.parse(token)[0].normal_form for token in tokens]
    return " ".join(lemmas)


def contains_keyword(text: str, keywords: Sequence[str]) -> bool:
    lemmatized = _lemmatize(text)
    return any(_lemmatize(kw) in lemmatized for kw in keywords)


def contains_exclusion(text: str, exclusions: Sequence[str]) -> bool:
    lemmatized = _lemmatize(text)
    return any(_lemmatize(exc) in lemmatized for exc in exclusions)


def should_process(post: dict, keywords: Sequence[str], exclusions: Sequence[str]) -> bool:
    text = " ".join([post.get("title", ""), post.get("content", "")])
    if not keywords:
        return not contains_exclusion(text, exclusions)
    if contains_exclusion(text, exclusions):
        return False
    return contains_keyword(text, keywords)
