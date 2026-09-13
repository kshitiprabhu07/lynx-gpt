# Project Lynx GPT — Platform Library

Shared infrastructure for ingestion, RAG, and notification services.
Owner: Kshiti

## Setup

```bash
git clone git@github.com:kshitiprabhu07/lynx-gpt.git
cd lynx-gpt
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
cp .env.example .env        # fill in real values
docker compose up -d
```

Postgres, Redis, MinIO and Jaeger come up on the `lynx` network.

## What's in here

| Module | What it gives you |
|---|---|
| `lynx.queue.streams` | Work queue — at-least-once, retry, dead-letter |
| `lynx.ratelimit` | Sliding-window rate limiter (fail-closed) + FastAPI middleware |
| `lynx.security.ssrf` | SSRF guard and content validation for the fetcher |
| `lynx.reliability` | Retry with jittered backoff, shared circuit breaker |
| `lynx.observability` | Prometheus metrics, OpenTelemetry tracing |
| `admin/` | Admin console — source health, quarantine, replay |

## Contracts — read before writing code

- **`docs/redis-namespaces.md`** — we share one Redis. Prefixes are assigned. Don't write unprefixed keys.
- **Queue delivery is at-least-once.** Consumers must be idempotent on `(event_id, target_version)`.
- **Fetcher must call `assert_url_is_safe()`** before every outbound request.

## Running things

```bash
uvicorn lynx.api:app --reload --port 8000     # API
uvicorn admin.app:app --reload --port 8001    # Admin console at /admin
```

Jaeger UI: http://localhost:16686
