from fastapi import FastAPI

from lynx.config import REDIS_URL
from lynx.observability.metrics import (
    install_metrics, dlq_depth, queue_oldest_age,
)
from lynx.queue.streams import WorkQueue
from lynx.ratelimit.limiter import RateLimiter
from lynx.ratelimit.middleware import install_rate_limit

app = FastAPI(title="Lynx GPT — platform")

limiter = RateLimiter(REDIS_URL, namespace="rl:api", limit=10,
                      window_ms=60_000, fail_open=False)
install_rate_limit(app, limiter)
install_metrics(app)

queue = WorkQueue(REDIS_URL, group="api", consumer="api-1")


@app.get("/health")
def health():
    return {"status": "ok", "rate_limiter_degraded": limiter.degraded}


@app.get("/api/search")
def search(q: str = ""):
    """Placeholder for Pranav's RAG endpoint — gives the limiter a target."""
    return {"query": q, "results": [], "note": "stub"}


@app.get("/api/queue")
def queue_depth():
    d = queue.depth()
    dlq_depth.set(d["dead_letter_depth"])
    queue_oldest_age.set(d["oldest_pending_age_s"])
    return d