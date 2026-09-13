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


REDIS_URL = os.environ.get("REDIS_URL_LOCAL", "redis://localhost:6379/0")
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "60"))

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN_LOCAL",
    "postgresql://lynx:postgres@localhost:5432/lynxgpt",
)