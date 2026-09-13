from fastapi import FastAPI

from lynx.config import REDIS_URL
from lynx.queue.streams import WorkQueue
from lynx.ratelimit.limiter import RateLimiter
from lynx.ratelimit.middleware import install_rate_limit

app = FastAPI(title="Lynx GPT — platform")

limiter = RateLimiter(REDIS_URL, namespace="rl:api", limit=10,
                      window_ms=60_000, fail_open=False)
install_rate_limit(app, limiter)

queue = WorkQueue(REDIS_URL, group="api", consumer="api-1")


@app.get("/health")
def health():
    return {"status": "ok", "rate_limiter_degraded": limiter.degraded}


@app.get("/api/search")
def search(q: str = ""):
    """Placeholder for Pranav's RAG endpoint — exists so the limiter has a target."""
    return {"query": q, "results": [], "note": "stub"}


@app.get("/api/queue")
def queue_depth():
    return queue.depth()