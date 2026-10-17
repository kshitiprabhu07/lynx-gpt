"""Lynx GPT admin console: one page that shows what is broken right now.

Run:  LYNX_ADMIN_PASSWORD=... uvicorn admin.app:app --port 8100
Data: Postgres (sources, fetch runs, errors, API requests, heartbeats,
      documents, quarantine, and Postgres's own statistics views) and
      Redis (work-queue depth).
"""
from __future__ import annotations

import json
import os
import secrets
import time
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
# A page load must not hang on a sick database: every console query is cut off here.
STATEMENT_TIMEOUT_MS = int(os.environ.get("LYNX_ADMIN_STATEMENT_TIMEOUT_MS", "5000"))


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


def _bytes(n) -> str:
    n = float(n or 0)
    for unit in ("B", "kB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


templates.env.filters["ago"] = _ago
templates.env.filters["duration"] = _duration
templates.env.filters["bytes"] = _bytes


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


# --- Database health: read from Postgres's own statistics views --------------
# Seeing other roles' queries in pg_stat_activity needs pg_read_all_stats:
#   GRANT pg_monitor TO <console role>;
# Without it those sessions are counted but cannot be judged.
DB_SQL = """
SELECT current_setting('server_version')                 AS version,
       pg_postmaster_start_time()                        AS started_at,
       pg_is_in_recovery()                               AS in_recovery,
       pg_database_size(current_database())              AS size_bytes,
       current_setting('max_connections')::int           AS max_connections,
       current_setting('superuser_reserved_connections')::int
         + COALESCE(NULLIF(current_setting('reserved_connections', true), ''), '0')::int
                                                         AS reserved_connections,
       (SELECT count(*) FROM pg_stat_activity
         WHERE backend_type = 'client backend')          AS connections,
       (SELECT count(*) FROM pg_stat_activity
         WHERE backend_type = 'client backend' AND state = 'active') AS active,
       (SELECT age(datfrozenxid) FROM pg_database
         WHERE datname = current_database())             AS xid_age,
       (SELECT blks_hit::float / NULLIF(blks_hit + blks_read, 0)
          FROM pg_stat_database
         WHERE datname = current_database())             AS cache_hit_ratio,
       current_user                                      AS console_role,
       (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
         OR pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') AS sees_all_sessions;
"""

# Sessions that are doing something (or holding a transaction open), worst
# first. Short healthy queries are filtered out by the rules in health.py.
DB_SESSIONS_SQL = """
SELECT pid, usename AS "user", application_name AS app, client_addr::text AS client,
       state, wait_event_type, wait_event,
       EXTRACT(EPOCH FROM now() - query_start) AS query_age_s,
       EXTRACT(EPOCH FROM now() - xact_start)  AS xact_age_s,
       pg_blocking_pids(pid)                   AS blocked_by,
       left(query, 500)                        AS query
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND pid <> pg_backend_pid()
  AND state <> 'idle'
ORDER BY cardinality(pg_blocking_pids(pid)) > 0 DESC, xact_start NULLS LAST
LIMIT 50;
"""

DB_TABLES_SQL = """
SELECT s.relname AS "table", s.schemaname AS schema,
       pg_total_relation_size(s.relid)               AS size_bytes,
       s.n_live_tup, s.n_dead_tup,
       GREATEST(s.last_autovacuum, s.last_vacuum)    AS last_vacuum
FROM pg_stat_user_tables s
ORDER BY pg_total_relation_size(s.relid) DESC
LIMIT 200;
"""

RETENTION_SQL = """
SELECT EXTRACT(EPOCH FROM now() - MIN(ts)) / 86400 AS api_request_oldest_days
FROM api_request;
"""


def _connect(**kwargs) -> psycopg.Connection:
    # application_name makes the console's own sessions easy to spot in pg_stat_activity.
    return psycopg.connect(DSN, connect_timeout=5,
                           application_name="lynx-admin-console", **kwargs)


def _first_line(exc: Exception) -> str:
    return f"{type(exc).__name__}: {(str(exc).strip().splitlines() or [''])[0]}"


def q(sql: str, params=()) -> list[dict]:
    with _connect(row_factory=dict_row) as conn:
        return conn.execute(sql, params).fetchall()


def execute(sql: str, params=()) -> int:
    with _connect() as conn:
        return conn.execute(sql, params).rowcount


def _safe(conn, name: str, sql: str, problems: list[str]):
    """One broken query (missing table, timeout) must not blank the page;
    it becomes an alert instead. The connection is in autocommit mode, so a
    failed query does not poison the ones after it."""
    try:
        return conn.execute(sql).fetchall()
    except psycopg.Error as exc:
        problems.append(f"{name}: {_first_line(exc)}")
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
def _open_db() -> tuple[psycopg.Connection | None, dict]:
    """Connect once per page load. The timings double as the latency probe."""
    t0 = time.perf_counter()
    try:
        conn = _connect(row_factory=dict_row, autocommit=True)
    except psycopg.Error as exc:
        return None, {"error": _first_line(exc)}
    t1 = time.perf_counter()
    try:
        conn.execute("SELECT 1")
        t2 = time.perf_counter()
        conn.execute("SELECT set_config('statement_timeout', %s, false)",
                     (str(STATEMENT_TIMEOUT_MS),))
    except psycopg.Error as exc:
        conn.close()
        return None, {"error": _first_line(exc)}
    return conn, {"connect_ms": (t1 - t0) * 1000, "rtt_ms": (t2 - t1) * 1000}


def collect() -> dict:
    problems: list[str] = []
    snap = {
        "now": datetime.now(timezone.utc),
        "sources": None, "routes": None, "errors": None, "heartbeats": None,
        "index": None, "quarantined": None,
        "db_sessions": None, "db_tables": None, "retention": None,
        "queue": queue_depth(),
        "problems": problems,
        "t": health.THRESHOLDS,
    }
    conn, snap["db"] = _open_db()
    if conn is not None:
        # If the database is down, it gets one "unreachable" alert from
        # health.db_alerts instead of a "query failed" alert per table.
        with conn:
            server = _safe(conn, "database stats", DB_SQL, problems)
            if server:
                snap["db"].update(server[0])
            snap["db_sessions"] = _safe(conn, "database sessions", DB_SESSIONS_SQL, problems)
            snap["db_tables"] = _safe(conn, "database tables", DB_TABLES_SQL, problems)
            retention = _safe(conn, "api_request retention", RETENTION_SQL, problems)
            snap["retention"] = retention[0] if retention else None

            snap["sources"] = _safe(conn, "sources", SOURCES_SQL, problems)
            snap["routes"] = _safe(conn, "api requests", ROUTES_SQL, problems)
            snap["errors"] = _safe(conn, "application errors", ERRORS_SQL, problems)
            snap["heartbeats"] = _safe(conn, "heartbeats", HEARTBEATS_SQL, problems)
            index_rows = _safe(conn, "index backlog", INDEX_SQL, problems)
            snap["index"] = index_rows[0] if index_rows else None
            snap["quarantined"] = _safe(conn, "quarantine", QUARANTINE_SQL, problems)
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


def _db_summary(db: dict) -> dict:
    if "error" in db:
        return {"reachable": False, "error": db["error"]}
    return {
        "reachable": True,
        "round_trip_ms": round(db["connect_ms"] + db["rtt_ms"], 1),
        "connections": db.get("connections"),
        "usable_connections": db.get("usable_connections"),
        "in_recovery": db.get("in_recovery"),
    }


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
        "database": _db_summary(snap["db"]),
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
