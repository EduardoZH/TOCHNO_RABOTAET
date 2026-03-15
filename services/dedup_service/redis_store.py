import logging

import redis

from shared.config.settings import redis_config

logger = logging.getLogger(__name__)


class RedisStore:
    def __init__(self):
        self.client = redis.from_url(redis_config.url, decode_responses=True)

    def add_bucket(self, bucket: str, value: str) -> None:
        try:
            self.client.sadd(bucket, value)
            self.client.expire(bucket, redis_config.dedup_ttl)
        except redis.ConnectionError:
            logger.warning("Redis unavailable, skipping bucket add for %s", bucket)

    def members(self, bucket: str):
        try:
            return self.client.smembers(bucket)
        except redis.ConnectionError:
            logger.warning("Redis unavailable, returning empty set for %s", bucket)
            return set()
