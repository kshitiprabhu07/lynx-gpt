# Lynx GPT admin console: error detection

The console now answers one question: what in Lynx GPT is broken right now?
It checks seven things and turns each problem into a critical or warning alert.

| Area | What counts as an error |
|---|---|
| Sources | failing (3+ failures in a row or never succeeded), stale (no success for 3x cadence), silent (succeeded but found 0 items when it usually finds 3+), degraded (any failure in 24 h), never run |
| API routes | 5xx rate ≥ 2% / 10%, p95 latency ≥ 2 s / 5 s over the last hour (routes with < 20 requests are ignored) |
| App errors | any critical-severity error, the same error 20+ times in an hour, an error first seen in the last hour |
| Services | no heartbeat for 2 min (late) or 10 min (dead); an expected service that never reported |
| Queue | Redis unreachable, oldest pending message ≥ 5 / 30 min, anything in dead-letter |
| Index | 50+ documents waiting, oldest waiting ≥ 30 / 120 min, documents that failed to index |
| Quarantine | a replay sent more than 15 min ago that the worker never confirmed |

Thresholds live in `admin/health.py` (`THRESHOLDS`, `EXPECTED_SERVICES`).

## Files

```
admin/app.py                  console: auth, queries, pages, actions
admin/health.py               the rules (pure functions, unit-tested)
admin/templates/dashboard.html
admin/sql/observability.sql   new tables/columns, safe on the real DB
lynx/observability/report.py  library every service uses to report errors
admin/testdata/base_schema.sql   stand-in tables for an empty test DB only
admin/testdata/seed.sql          dummy data (test DBs only, it truncates)
admin/testdata/queue_fixture.json  fake Redis queue stats
tests/test_health.py          unit tests, no DB needed
tests/test_admin_e2e.py       seeds a test DB, checks every alert and action
```

## Test with the dummy data

```bash
pip install fastapi jinja2 "psycopg[binary]" httpx pytest python-multipart uvicorn
createdb lynx_test                               # name must contain "test"
export LYNX_TEST_DSN="postgresql://localhost/lynx_test"

# 1. rules only
pytest tests/test_health.py

# 2. full stack against Postgres
pytest tests/test_admin_e2e.py

# 3. look at it in the browser
psql "$LYNX_TEST_DSN" -f admin/testdata/base_schema.sql
psql "$LYNX_TEST_DSN" -f admin/sql/observability.sql
psql "$LYNX_TEST_DSN" -f admin/testdata/seed.sql
LYNX_ADMIN_DSN="$LYNX_TEST_DSN" LYNX_ADMIN_PASSWORD=dev \
LYNX_ADMIN_QUEUE_FIXTURE=admin/testdata/queue_fixture.json \
  uvicorn admin.app:app --port 8100
# open http://localhost:8100/admin  (user admin, password dev)
```

Re-run `seed.sql` whenever you want a fresh state; timestamps are relative
to `now()`. Leave out `LYNX_ADMIN_QUEUE_FIXTURE` to use real Redis.

### What the seed should produce: 7 critical, 14 warnings

Critical
- `nitt-academics` stale: no success for 300 min, cadence 60
- `careers-freshworks` failing: 4 × 403 in a row
- `rag-api POST /rag/query`: 12.5% 5xx, and p95 10 s (two alerts)
- `rag-api ReadTimeout`: 27 in the last hour
- `notifier SMTPAuthenticationError`: critical severity
- `scheduler`: never sent a heartbeat

Warning
- `careers-zoho` degraded, `careers-tcs` silent (0 items, usually ~11), `careers-infosys` never run
- `jobs-api GET /jobs/{job_id}` 4% 5xx, `GET /jobs/search` p95 ~3.5 s
- `indexer ValueError` new error
- `ingest-worker/worker-2` dead (worker-1 is fine, so warning), `jobs-api/jobs-1` late
- queue: oldest pending 15 min, 3 dead-letter messages
- index: 75 waiting, oldest 95 min, 4 failed
- quarantine item 3 replay never confirmed

Should stay quiet: `nitt-notices` (healthy), `nitt-hostel` (paused),
`GET /jobs` (1% errors), `POST /jobs/{job_id}/apply` (only 3 requests),
the acknowledged `ConnectionResetError`, the resolved quarantine item.

## Wiring it into the real services

1. Run `admin/sql/observability.sql` on the real database.
2. RAG API and Jobs API: `report.install(app, service="rag-api")` (or `"jobs-api"`).
3. Workers, indexer, notifier, scheduler: wrap units of work in
   `with report.capture("indexer", component="embed_batch"): ...` and call
   `report.heartbeat("<service>")` every 30 s from each process.
4. Ingestion adapters: write the failure reason to `fetch_run.error`.
5. Replay worker: on success, run
   `UPDATE quarantine SET resolved = true WHERE id = %(quarantine_id)s`
   (the event now carries `quarantine_id`).
6. Check `WorkQueue.depth()`: if it reports pending messages for its own
   consumer group, set `LYNX_ADMIN_QUEUE_GROUP` to the workers' group, or the
   pending count will always read 0.
7. If the indexed-documents table is not called `document`, edit `INDEX_SQL`
   in `admin/app.py`.
8. Point an uptime monitor at `/admin/api/health` with basic auth; it returns
   HTTP 503 while anything is critical.

## Settings

| Variable | Purpose |
|---|---|
| `LYNX_ADMIN_PASSWORD` | required; the console refuses to serve without it |
| `LYNX_ADMIN_USER` | default `admin` |
| `LYNX_ADMIN_DSN` | override `POSTGRES_DSN` (used for the test DB) |
| `LYNX_ADMIN_QUEUE_FIXTURE` | read queue stats from a JSON file instead of Redis |
| `LYNX_ADMIN_QUEUE_GROUP` | consumer group whose backlog to report |
