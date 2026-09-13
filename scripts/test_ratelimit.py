import time
from lynx.config import REDIS_URL
from lynx.ratelimit.limiter import RateLimiter

limiter = RateLimiter(REDIS_URL, namespace="rl:demo", limit=5, window_ms=1000)
limiter.r.delete("rl:demo:test-client")

print("--- 5 requests allowed per 1s window ---")
for i in range(7):
    allowed, remaining = limiter.check("test-client")
    print(f"request {i+1}: allowed={allowed} remaining={remaining}")

print("\n--- wait 1.1s for the window to slide ---")
time.sleep(1.1)
print("after wait:", limiter.check("test-client"))