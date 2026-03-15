from typing import Tuple

from shared.hashing.simhash import compute_simhash, generate_buckets, hamming_distance


class Deduplicator:
    def __init__(self, redis_store, threshold: int = 3):
        self.redis_store = redis_store
        self.threshold = threshold

    def fingerprint(self, text: str) -> int:
        return compute_simhash(text)

    def is_duplicate(self, text: str) -> Tuple[bool, int]:
        fingerprint = self.fingerprint(text)
        for bucket in generate_buckets(fingerprint):
            candidates = self.redis_store.members(bucket)
            for candidate in candidates:
                candidate_val = int(candidate)
                if hamming_distance(fingerprint, candidate_val) <= self.threshold:
                    return True, candidate_val
        for bucket in generate_buckets(fingerprint):
            self.redis_store.add_bucket(bucket, str(fingerprint))
        return False, fingerprint
