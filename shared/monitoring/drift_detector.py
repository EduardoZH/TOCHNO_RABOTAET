"""Detects input data distribution changes (relevancy score drift)."""

import logging
import math
from collections import deque

logger = logging.getLogger(__name__)


class DriftDetector:
    def __init__(self, window_size: int = 500):
        self._scores: deque[float] = deque(maxlen=window_size)
        self._baseline_mean: float | None = None
        self._baseline_std: float | None = None

    def record(self, score: float) -> None:
        self._scores.append(score)

    def set_baseline(self) -> None:
        if len(self._scores) < 10:
            return
        self._baseline_mean = sum(self._scores) / len(self._scores)
        variance = sum((x - self._baseline_mean) ** 2
                       for x in self._scores) / len(self._scores)
        self._baseline_std = math.sqrt(variance) if variance > 0 else 1.0
        logger.info("Drift baseline set: mean=%.2f std=%.2f",
                     self._baseline_mean, self._baseline_std)

    def check_drift(self) -> dict:
        if self._baseline_mean is None or len(self._scores) < 10:
            return {"drift_detected": False, "reason": "no baseline"}
        current_mean = sum(self._scores) / len(self._scores)
        shift = abs(current_mean - self._baseline_mean)
        threshold = 2.0 * self._baseline_std
        drifted = shift > threshold
        if drifted:
            logger.warning("DRIFT DETECTED: baseline_mean=%.2f current_mean=%.2f shift=%.2f threshold=%.2f",
                           self._baseline_mean, current_mean, shift, threshold)
        return {
            "drift_detected": drifted,
            "baseline_mean": round(self._baseline_mean, 3),
            "current_mean": round(current_mean, 3),
            "shift": round(shift, 3),
            "threshold": round(threshold, 3),
        }
