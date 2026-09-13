from lynx.config import REDIS_URL
from lynx.reliability.controls import CircuitBreaker, backoff_delays

print("--- backoff delays (jittered, so each run differs) ---")
print([round(d, 2) for d in backoff_delays(attempts=5)])

cb = CircuitBreaker(REDIS_URL, source_id="nitt_notices", threshold=3, open_seconds=5)
cb.record_success()   # reset any leftover state


def always_fails():
    raise RuntimeError("source timeout")


print("\n--- circuit breaker: threshold 3 ---")
for i in range(5):
    try:
        cb.call(always_fails)
    except Exception as e:
        print(f"attempt {i+1}: {e}")

print("\ncircuit open?", cb.is_open())