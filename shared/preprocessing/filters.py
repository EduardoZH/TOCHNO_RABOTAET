from typing import Sequence


def normalize_text(text: str) -> str:
    return text.lower()


def contains_keyword(text: str, keywords: Sequence[str]) -> bool:
    lowered = normalize_text(text)
    return any(keyword.lower() in lowered for keyword in keywords)


def contains_exclusion(text: str, exclusions: Sequence[str]) -> bool:
    lowered = normalize_text(text)
    return any(exclusion.lower() in lowered for exclusion in exclusions)


def should_process(post: dict, keywords: Sequence[str], exclusions: Sequence[str]) -> bool:
    if not keywords:
        return not contains_exclusion(post.get("content", ""))
    text = "".join([post.get("title", ""), post.get("content", "")])
    if contains_exclusion(text, exclusions):
        return False
    return contains_keyword(text, keywords)
