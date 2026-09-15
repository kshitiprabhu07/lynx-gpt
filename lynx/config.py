import os


class SecretStr:
    """Wraps a secret so it can't be printed by accident."""
    def __init__(self, value: str):
        self._value = value

    def get(self) -> str:
        return self._value

    def __repr__(self):
        return "SecretStr('***')"

    __str__ = __repr__


# DB 0 — platform namespaces: rl:, cb:, q:, and (Phase 2) sess:
# noeviction policy. Rate counters and sessions must not be evicted.
REDIS_URL = os.environ.get("REDIS_URL_LOCAL", "redis://localhost:6379/0")

# DB 1 — RedisVL semantic cache. Vector index; eviction would
# desync it from the underlying hashes, so it expires by TTL only.
REDIS_CACHE_URL = os.environ.get("REDIS_CACHE_URL_LOCAL", "redis://localhost:6379/1")

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN_LOCAL",
    "postgresql://lynx:postgres@localhost:5432/lynxgpt",
)

RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "60"))