"""Lynx GPT admin console: one page that shows what is broken right now.

Run:  LYNX_ADMIN_PASSWORD=... uvicorn admin.app:app --port 8100
Data: Postgres (sources, fetch runs, errors, API requests, heartbeats,
      documents, quarantine) and Redis (work-queue depth).
"""
from __future__ import annotations

import json
import os
import secrets
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from psycopg.rows import dict_row

from admin import health
from lynx.config import POSTGRES_DSN, REDIS_URL

DSN = os.environ.get("LYNX_ADMIN_DSN", POSTGRES_DSN)
ADMIN_USER = os.environ.get("LYNX_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("LYNX_ADMIN_PASSWORD")       # required, no default
QUEUE_FIXTURE = os.environ.get("LYNX_ADMIN_QUEUE_FIXTURE")   # JSON file, for testing
# The consumer group whose backlog matters is the workers', not the console's.
QUEUE_GROUP = os.environ.get("LYNX_ADMIN_QUEUE_GROUP", "admin")


# --------------------------------------------------------------------------
# Security: HTTP basic auth on every route, same-origin check on every POST
# --------------------------------------------------------------------------
_basic = HTTPBasic()


def require_admin(creds: HTTPBasicCredentials = Depends(_basic)) -> str:
    if not ADMIN_PASSWORD:
        raise HTTPException(503, "Set LYNX_ADMIN_PASSWORD to enable the admin console")
    user_ok = secrets.compare_digest(creds.username.encode(), ADMIN_USER.encode())
    pass_ok = secrets.compare_digest(creds.password.encode(), ADMIN_PASSWORD.encode())
    if not (user_ok and pass_ok):
        raise HTTPException(401, "Wrong username or password",
                            headers={"WWW-Authenticate": "Basic"})
    return creds.username


def same_origin(request: Request) -> None:
    """Browsers resend basic-auth credentials on cross-site form posts, so a
    malicious page could press Pause for you. Reject POSTs from other origins.
    Behind a reverse proxy, make sure it forwards the original Host header."""
    origin = request.headers.get("origin") or request.headers.get("referer")
    if not origin or urlparse(origin).netloc != request.headers.get("host"):
        raise HTTPException(403, "Cross-site request blocked")


app = FastAPI(title="Lynx GPT — Admin Console", dependencies=[Depends(require_admin)])
templates = Jinja2Templates(directory="admin/templates")


def _ago(ts: datetime | None) -> str:
    if ts is None:
        return "—"
    secs = (datetime.now(timezone.utc) - ts).total_seconds()
    if secs < 90:
        return f"{secs:.0f}s ago"
    if secs < 5400:
        return f"{secs / 60:.0f} min ago"
    if secs < 172800:
        return f"{secs / 3600:.1f} h ago"
    return f"{secs / 86400:.0f} d ago"


def _duration(seconds) -> str:
    s = int(float(seconds or 0))
    return f"{s}s" if s < 120 else f"{s // 60} min" if s < 7200 else f"{s / 3600:.1f} h"


templates.env.filters["ago"] = _ago
templates.env.filters["duration"] = _duration


# --------------------------------------------------------------------------
# Queries
# --------------------------------------------------------------------------
SOURCES_SQL = """
SELECT s.id, s.kind, s.state, s.cadence_min, s.owner,
       lr.last_run,
       ls.started_at                       AS last_success,
       ls.items_found                      AS last_items,
       COALESCE(base.avg_items, 0)         AS baseline_items,
       f24.failures_24h,
       fs.failures_since_success,
       le.error                            AS last_error,
       EXTRACT(EPOCH FROM now() - ls.started_at) / 60 AS stale_minutes
FROM source s
LEFT JOIN LATERAL (SELECT MAX(started_at) AS last_run
                   FROM fetch_run WHERE source_id = s.id) lr ON true
LEFT JOIN LATERAL (SELECT started_at, items_found FROM fetch_run
                   WHERE source_id = s.id AND status = 'success'
                   ORDER BY started_at DESC LIMIT 1) ls ON true
LEFT JOIN LATERAL (SELECT AVG(items_found) AS avg_items FROM (
                     SELECT items_found FROM fetch_run
                     WHERE source_id = s.id AND status = 'success'
                     ORDER BY started_at DESC OFFSET 1 LIMIT 20) prev) base ON true
LEFT JOIN LATERAL (SELECT COUNT(*) AS failures_24h FROM fetch_run
                   WHERE source_id = s.id AND status = 'failed'
                     AND started_at > now() - interval '24 hours') f24 ON true
LEFT JOIN LATERAL (SELECT COUNT(*) AS failures_since_success FROM fetch_run
                   WHERE source_id = s.id AND status = 'failed'
                     AND started_at > COALESCE(ls.started_at, '-infinity')) fs ON true
LEFT JOIN LATERAL (SELECT error FROM fetch_run
                   WHERE source_id = s.id AND status = 'failed'
                   ORDER BY started_at DESC LIMIT 1) le ON true
ORDER BY s.id;
"""

ROUTES_SQL = """
SELECT service, method, route,
       COUNT(*)                                          AS requests,
       COUNT(*) FILTER (WHERE status >= 500)             AS errors_5xx,
       COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499) AS errors_4xx,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_ms
FROM api_request
WHERE ts > now() - interval '1 hour'
GROUP BY service, method, route
ORDER BY errors_5xx DESC, requests DESC;
"""

# Errors from the last 24 h grouped by fingerprint. An acknowledged group
# stays hidden until it happens again after the acknowledgement.
ERRORS_SQL = """
SELECT e.fingerprint,
       (array_agg(e.service    ORDER BY e.occurred_at DESC))[1] AS service,
       (array_agg(e.component  ORDER BY e.occurred_at DESC))[1] AS component,
       (array_agg(e.error_type ORDER BY e.occurred_at DESC))[1] AS error_type,
       (array_agg(e.message    ORDER BY e.occurred_at DESC))[1] AS last_message,
       (array_agg(e.request_id ORDER BY e.occurred_at DESC))[1] AS last_request_id,
       bool_or(e.severity = 'critical')                          AS any_critical,
       COUNT(*)                                                  AS count_24h,
       COUNT(*) FILTER (WHERE e.occurred_at > now() - interval '1 hour') AS count_1h,
       MAX(e.occurred_at)                                        AS last_seen,
       (SELECT MIN(occurred_at) FROM app_error a
         WHERE a.fingerprint = e.fingerprint)                    AS first_seen
FROM app_error e
WHERE e.occurred_at > now() - interval '24 hours'
GROUP BY e.fingerprint
HAVING MAX(e.occurred_at) > COALESCE(
         (SELECT acked_at FROM error_ack k WHERE k.fingerprint = e.fingerprint),
         '-infinity')
ORDER BY any_critical DESC, count_1h DESC, last_seen DESC
LIMIT 100;
"""

HEARTBEATS_SQL = """
SELECT service, instance, version, last_seen,
       EXTRACT(EPOCH FROM now() - last_seen) AS age_s
FROM service_heartbeat ORDER BY service, instance;
"""

# Adjust to the data layer's real table if it is not called "document".
INDEX_SQL = """
SELECT COUNT(*) FILTER (WHERE indexed_at IS NULL AND index_error IS NULL)     AS backlog,
       COUNT(*) FILTER (WHERE indexed_at IS NULL AND index_error IS NOT NULL) AS failed,
       EXTRACT(EPOCH FROM now() - MIN(ingested_at)
               FILTER (WHERE indexed_at IS NULL AND index_error IS NULL)) / 60 AS oldest_unindexed_min,
       COUNT(*) FILTER (WHERE indexed_at > now() - interval '1 hour')         AS indexed_1h
FROM document;
"""

QUARANTINE_SQL = """
SELECT q.id, q.source_id, q.reason, q.created_at, q.replayed_at, q.replay_count
FROM quarantine q
WHERE NOT q.resolved
ORDER BY q.created_at DESC LIMIT 50;
"""


def q(sql: str, params=()) -> list[dict]:
    with psycopg.connect(DSN, row_factory=dict_row, connect_timeout=5) as conn:
        return conn.execute(sql, params).fetchall()


def execute(sql: str, params=()) -> int:
    with psycopg.connect(DSN, connect_timeout=5) as conn:
        return conn.execute(sql, params).rowcount


def _safe(name: str, sql: str, problems: list[str]):
    """One broken query (missing table, DB down) must not blank the page;
    it becomes an alert instead."""
    try:
        return q(sql)
    except psycopg.Error as exc:
        problems.append(f"{name}: {type(exc).__name__}: {str(exc).strip().splitlines()[0]}")
        return None


# --------------------------------------------------------------------------
# Queue
# --------------------------------------------------------------------------
_queue = None


def _get_queue():
    global _queue
    if _queue is None:
        from lynx.queue.streams import WorkQueue
        _queue = WorkQueue(REDIS_URL, group=QUEUE_GROUP, consumer="console")
    return _queue


def queue_depth() -> dict:
    if QUEUE_FIXTURE:
        with open(QUEUE_FIXTURE) as f:
            return json.load(f)
    global _queue
    try:
        return _get_queue().depth()
    except Exception as exc:          # Redis down must show up, not crash the page
        _queue = None
        return {"error": f"{type(exc).__name__}: {exc}"}


# --------------------------------------------------------------------------
# Snapshot
# --------------------------------------------------------------------------
def collect() -> dict:
    problems: list[str] = []
    index_rows = _safe("index backlog", INDEX_SQL, problems)
    snap = {
        "now": datetime.now(timezone.utc),
        "sources": _safe("sources", SOURCES_SQL, problems),
        "routes": _safe("api requests", ROUTES_SQL, problems),
        "errors": _safe("application errors", ERRORS_SQL, problems),
        "heartbeats": _safe("heartbeats", HEARTBEATS_SQL, problems),
        "index": index_rows[0] if index_rows else None,
        "quarantined": _safe("quarantine", QUARANTINE_SQL, problems),
        "queue": queue_depth(),
        "problems": problems,
        "t": health.THRESHOLDS,
    }
    snap["alerts"] = health.evaluate(snap)
    snap["status"] = health.overall_status(snap["alerts"])
    return snap


# --------------------------------------------------------------------------
# Pages
# --------------------------------------------------------------------------
@app.get("/admin")
def dashboard(request: Request):
    snap = collect()
    return templates.TemplateResponse(request, "dashboard.html", snap)


@app.get("/admin/api/health")
def health_json():
    """For an uptime monitor or cron: HTTP 503 whenever something is critical."""
    snap = collect()
    body = {
        "status": snap["status"],
        "generated_at": snap["now"].isoformat(),
        "critical": sum(a.severity == "critical" for a in snap["alerts"]),
        "warning": sum(a.severity == "warning" for a in snap["alerts"]),
        "alerts": [a.to_dict() for a in snap["alerts"]],
    }
    return JSONResponse(body, status_code=503 if snap["status"] == "critical" else 200)


# --------------------------------------------------------------------------
# Actions (all POST, all same-origin)
# --------------------------------------------------------------------------
def _back() -> RedirectResponse:
    return RedirectResponse("/admin", status_code=303)


@app.post("/admin/source/{source_id}/pause", dependencies=[Depends(same_origin)])
def pause(source_id: str):
    if not execute("UPDATE source SET state = 'paused' WHERE id = %s", (source_id,)):
        raise HTTPException(404, f"No source {source_id!r}")
    return _back()


@app.post("/admin/source/{source_id}/resume", dependencies=[Depends(same_origin)])
def resume(source_id: str):
    if not execute("UPDATE source SET state = 'active' WHERE id = %s", (source_id,)):
        raise HTTPException(404, f"No source {source_id!r}")
    return _back()


@app.post("/admin/errors/{fingerprint}/ack", dependencies=[Depends(same_origin)])
def ack_error(fingerprint: str, user: str = Depends(require_admin)):
    execute("""INSERT INTO error_ack (fingerprint, acked_at, acked_by)
               VALUES (%s, now(), %s)
               ON CONFLICT (fingerprint) DO UPDATE
               SET acked_at = now(), acked_by = EXCLUDED.acked_by""",
            (fingerprint, user))
    return _back()


@app.post("/admin/quarantine/{item_id}/replay", dependencies=[Depends(same_origin)])
def replay(item_id: int):
    """Replay from the raw snapshot — no re-fetch. This is why snapshots exist.

    The item stays unresolved until the replay worker succeeds and runs
    `UPDATE quarantine SET resolved = true WHERE id = <quarantine_id>`.
    If it never does, the dashboard flags the replay as stuck."""
    rows = q("SELECT source_id, payload_ref, replay_count FROM quarantine "
             "WHERE id = %s AND NOT resolved", (item_id,))
    if not rows:
        raise HTTPException(404, f"No unresolved quarantine item {item_id}")
    attempt = rows[0]["replay_count"] + 1
    try:
        _get_queue().publish("snapshot.replay", {
            # attempt number keeps a second replay from being dropped as a duplicate
            "event_id": f"replay-{item_id}-{attempt}",
            "quarantine_id": item_id,
            "source_id": rows[0]["source_id"],
            "snapshot_ref": rows[0]["payload_ref"],
        })
    except Exception as exc:
        raise HTTPException(503, f"Could not publish replay: {type(exc).__name__}: {exc}")
    execute("UPDATE quarantine SET replayed_at = now(), replay_count = %s WHERE id = %s",
            (attempt, item_id))
    return _back()
