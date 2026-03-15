import logging
from typing import Sequence

import pymorphy3

logger = logging.getLogger(__name__)

_morph = pymorphy3.MorphAnalyzer()


def _lemmatize(text: str) -> str:
    tokens = text.lower().split()
    lemmas = [_morph.parse(token)[0].normal_form for token in tokens]
    return " ".join(lemmas)


def _lemmatize_token(token: str) -> str:
    return _morph.parse(token.lower())[0].normal_form


def contains_keyword(text: str, keywords: Sequence[str]) -> bool:
    lemmatized_text = _lemmatize(text)
    for kw in keywords:
        kw_lemma = _lemmatize_token(kw)
        # точное совпадение лемм
        if kw_lemma in lemmatized_text.split():
            return True
        # подстрока в лемматизованном тексте (для частичных ключевых слов типа "мошен")
        if kw.lower() in lemmatized_text or kw_lemma in lemmatized_text:
            return True
        # подстрока в оригинальном тексте
        if kw.lower() in text.lower():
            return True
    return False


def contains_exclusion(text: str, exclusions: Sequence[str]) -> bool:
    if not exclusions:
        return False
    lemmatized_text = _lemmatize(text)
    for exc in exclusions:
        exc_lemma = _lemmatize_token(exc)
        if exc_lemma in lemmatized_text.split():
            return True
        if exc.lower() in text.lower():
            return True
    return False


def should_process(post: dict, keywords: Sequence[str], exclusions: Sequence[str]) -> bool:
    text = " ".join([post.get("title", ""), post.get("content", "")])
    if not keywords:
        return not contains_exclusion(text, exclusions)
    if contains_exclusion(text, exclusions):
        return False
    return contains_keyword(text, keywords)
