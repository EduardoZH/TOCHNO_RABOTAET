import logging

import redis

from shared.config.settings import redis_config

logger = logging.getLogger(__name__)


class RedisStore:
    def __init__(self):
        self.client = redis.from_url(redis_config.url, decode_responses=True)

    def add_bucket(self, bucket: str, value: str) -> None:
        self.client.sadd(bucket, value)
        self.client.expire(bucket, redis_config.dedup_ttl)

    def members(self, bucket: str):
        return self.client.smembers(bucket)
