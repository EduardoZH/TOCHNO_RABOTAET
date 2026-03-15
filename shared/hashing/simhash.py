import hashlib
import re
from typing import Iterable

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)


def compute_simhash(text: str, bits: int = 64) -> int:
    if not text:
        return 0

    normalized = _PUNCT_RE.sub("", text.lower())
    vector = [0] * bits
    for token in normalized.split():
        digest = hashlib.md5(token.encode()).hexdigest()
        token_hash = int(digest, 16)
        for i in range(bits):
            bit_mask = 1 << i
            vector[i] += 1 if token_hash & bit_mask else -1

    fingerprint = 0
    for i, weight in enumerate(vector):
        if weight >= 0:
            fingerprint |= 1 << i
    return fingerprint


def hamming_distance(value: int, other: int) -> int:
    return bin(value ^ other).count("1")


def generate_buckets(fingerprint: int, bucket_count: int = 4, bits: int = 64) -> Iterable[str]:
    slice_width = bits // bucket_count
    for i in range(bucket_count):
        start = i * slice_width
        mask = ((1 << slice_width) - 1) << start
        fragment = (fingerprint & mask) >> start
        yield f"simhash_bucket:{i}:{fragment:0{slice_width}b}"
