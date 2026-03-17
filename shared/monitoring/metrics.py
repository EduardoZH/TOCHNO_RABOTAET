"""Lightweight per-service metrics collector. No external dependencies."""

import json
import logging
import time
import threading
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class PipelineMetrics:
    def __init__(self, service_name: str, log_interval: int = 100):
        self.service_name = service_name
        self.log_interval = log_interval
        self._counters: dict[str, int] = defaultdict(int)
        self._latencies: deque[float] = deque(maxlen=1000)
        self._start_time = time.time()
        self._lock = threading.Lock()

    def inc(self, metric: str, value: int = 1) -> None:
        with self._lock:
            self._counters[metric] += value
            if (metric == "messages_processed"
                    and self._counters[metric] % self.log_interval == 0):
                self.log_stats()

    def observe_latency(self, seconds: float) -> None:
        with self._lock:
            self._latencies.append(seconds)

    def get_stats(self) -> dict:
        with self._lock:
            stats = {
                "service": self.service_name,
                "uptime_s": round(time.time() - self._start_time, 1),
                **dict(self._counters),
            }
            if self._latencies:
                sorted_lat = sorted(self._latencies)
                stats["avg_latency_ms"] = round(
                    sum(sorted_lat) / len(sorted_lat) * 1000, 1)
                p95_idx = int(len(sorted_lat) * 0.95)
                stats["p95_latency_ms"] = round(sorted_lat[p95_idx] * 1000, 1)
            return stats

    def log_stats(self) -> None:
        stats = self.get_stats()
        logger.info("METRICS %s", json.dumps(stats, ensure_ascii=False))
