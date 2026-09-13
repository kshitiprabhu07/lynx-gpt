import time
import redis

SLIDING_WINDOW_LUA = """
local key        = KEYS[1]
local now_ms     = tonumber(ARGV[1])
local window_ms  = tonumber(ARGV[2])
local limit      = tonumber(ARGV[3])
local member     = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now_ms - window_ms)
local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now_ms, member)
    redis.call('PEXPIRE', key, window_ms)
    return {1, limit - count - 1}
else
    redis.call('PEXPIRE', key, window_ms)
    return {0, 0}
end
"""


class RateLimiter:
    """
    fail_open=False -> API limiter. Redis down means DENY.
    fail_open=True  -> caches only. NEVER for a limiter.
    """

    def __init__(self, redis_url: str, namespace: str = "rl:api",
                 limit: int = 60, window_ms: int = 60_000,
                 fail_open: bool = False):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True,
                                      socket_timeout=0.25)
        self.script = self.r.register_script(SLIDING_WINDOW_LUA)
        self.namespace = namespace
        self.limit = limit
        self.window_ms = window_ms
        self.fail_open = fail_open
        self.degraded = False

    def check(self, identity: str):
        key = f"{self.namespace}:{identity}"
        now_ms = int(time.time() * 1000)
        member = f"{now_ms}-{time.perf_counter_ns()}"
        try:
            allowed, remaining = self.script(
                keys=[key], args=[now_ms, self.window_ms, self.limit, member]
            )
            self.degraded = False
            return bool(allowed), int(remaining)
        except redis.RedisError:
            self.degraded = True
            if self.fail_open:
                return True, 0
            # FAIL CLOSED: a Redis outage must not become an unlimited-LLM bill.
            return False, 0