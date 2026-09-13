import random
import time
import redis


def backoff_delays(attempts: int = 5, base: float = 1.0, cap: float = 30.0):
    """Exponential backoff with full jitter."""
    for i in range(attempts):
        yield min(cap, base * (2 ** i)) * random.random()


def with_retry(fn, attempts: int = 5, retry_on=(Exception,)):
    last = None
    for delay in backoff_delays(attempts):
        try:
            return fn()
        except retry_on as e:
            last = e
            time.sleep(delay)
    raise last


class CircuitBreaker:
    """State lives in Redis under cb:, so all workers share one view per source."""

    def __init__(self, redis_url: str, source_id: str,
                 threshold: int = 5, open_seconds: int = 60):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)
        self.key = f"cb:source:{source_id}"
        self.threshold = threshold
        self.open_seconds = open_seconds

    def is_open(self) -> bool:
        return self.r.get(f"{self.key}:open") == "1"

    def record_failure(self):
        fails = self.r.incr(self.key)
        self.r.expire(self.key, self.open_seconds * 2)
        if fails >= self.threshold:
            self.r.setex(f"{self.key}:open", self.open_seconds, "1")

    def record_success(self):
        self.r.delete(self.key, f"{self.key}:open")

    def call(self, fn):
        if self.is_open():
            raise RuntimeError(f"circuit open for {self.key}")
        try:
            result = fn()
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result