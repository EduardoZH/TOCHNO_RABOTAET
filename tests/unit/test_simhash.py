"""Tests for shared/hashing/simhash.py — SimHash fingerprinting."""

from shared.hashing.simhash import compute_simhash, hamming_distance, generate_buckets


class TestComputeSimhash:
    def test_deterministic(self):
        text = "мошенники продают квартиры"
        assert compute_simhash(text) == compute_simhash(text)

    def test_different_texts_different_hashes(self):
        h1 = compute_simhash("мошенники продают квартиры")
        h2 = compute_simhash("котики захватили интернет")
        assert h1 != h2

    def test_similar_texts_small_hamming(self):
        h1 = compute_simhash("мошенники продают квартиры в Москве")
        h2 = compute_simhash("мошенники продают квартиры в Питере")
        assert hamming_distance(h1, h2) <= 15  # similar texts, moderate distance

    def test_empty_string_returns_zero(self):
        assert compute_simhash("") == 0

    def test_punctuation_ignored(self):
        h1 = compute_simhash("Привет мир!")
        h2 = compute_simhash("Привет мир")
        assert h1 == h2

    def test_returns_positive_integer(self):
        h = compute_simhash("тестовый текст для хеширования")
        assert isinstance(h, int)
        assert h >= 0

    def test_single_word(self):
        h = compute_simhash("мошенник")
        assert isinstance(h, int)
        assert h > 0


class TestHammingDistance:
    def test_identical_is_zero(self):
        assert hamming_distance(123456, 123456) == 0

    def test_known_values(self):
        # 0b0000 vs 0b0001 = 1 bit different
        assert hamming_distance(0, 1) == 1
        # 0b0000 vs 0b1111 = 4 bits different
        assert hamming_distance(0, 15) == 4

    def test_maximum_distance_64bit(self):
        all_ones = (1 << 64) - 1
        assert hamming_distance(0, all_ones) == 64


class TestGenerateBuckets:
    def test_count(self):
        buckets = list(generate_buckets(12345))
        assert len(buckets) == 4

    def test_format(self):
        for bucket in generate_buckets(12345):
            assert bucket.startswith("simhash_bucket:")
            parts = bucket.split(":")
            assert len(parts) == 3

    def test_deterministic(self):
        b1 = list(generate_buckets(99999))
        b2 = list(generate_buckets(99999))
        assert b1 == b2

    def test_custom_bucket_count(self):
        buckets = list(generate_buckets(12345, bucket_count=8, bits=64))
        assert len(buckets) == 8
