"""Error-detection rules for the Lynx GPT admin console.

Everything here is a pure function over plain dicts (rows from Postgres,
the queue stats from Redis), so the rules can be unit-tested without a
database. app.py collects the data; this module decides what is wrong.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

THRESHOLDS = {
    # ingestion sources
    "stale_factor": 3,                 # no success for 3x cadence -> stale
    "consecutive_failures_crit": 3,    # 3 failures in a row -> failing
    "silent_min_baseline": 3.0,        # usually finds >=3 items, now finds 0
    # RAG / Jobs APIs (last hour)
    "api_min_requests": 20,            # ignore routes with too little traffic
    "api_5xx_warn": 0.02,
    "api_5xx_crit": 0.10,
    "api_p95_warn_ms": 2000,
    "api_p95_crit_ms": 5000,
    # application errors
    "error_burst_crit": 20,            # same error >= 20 times in an hour
    "new_error_window_min": 60,        # first seen within the last hour
    # processes
    "heartbeat_warn_s": 120,
    "heartbeat_crit_s": 600,
    # work queue
    "queue_oldest_pending_warn_s": 300,
    "queue_oldest_pending_crit_s": 1800,
    "dead_letter_warn": 1,
    # indexing
    "index_backlog_warn": 50,
    "index_oldest_warn_min": 30,
    "index_oldest_crit_min": 120,
    # quarantine replay
    "replay_stuck_min": 15,
    # database (Postgres)
    "db_latency_warn_ms": 250,         # connect + SELECT 1, as the console sees it
    "db_latency_crit_ms": 1000,
    "db_connections_warn": 0.80,       # share of usable connection slots in use
    "db_connections_crit": 0.95,
    "db_long_query_warn_s": 60,        # one statement running this long
    "db_long_query_crit_s": 300,
    "db_idle_in_tx_warn_s": 300,       # transaction left open: holds locks, blocks vacuum
    "db_idle_in_tx_crit_s": 1800,
    "db_blocked_warn_s": 30,           # waiting on another session's lock
    "db_blocked_crit_s": 300,
    "db_xid_age_warn": 500_000_000,    # wraparound; Postgres stops writes near 2.1 billion
    "db_xid_age_crit": 1_000_000_000,
    "db_dead_rows_min": 10_000,        # vacuum not keeping up: this many dead rows ...
    "db_dead_ratio_warn": 0.20,        # ... and this share of the table
    "api_request_retention_days": 7,   # README: keep about a week of api_request
}

# Processes that must always be alive. A service missing from
# service_heartbeat entirely is reported as down.
EXPECTED_SERVICES = ("ingest-worker", "indexer", "rag-api", "jobs-api",
                     "notifier", "scheduler")

_RANK = {"critical": 0, "warning": 1}


@dataclass(frozen=True)
class Alert:
    severity: str   # "critical" | "warning"
    area: str       # sources | api | errors | services | queue | index | quarantine | database | console
    subject: str
    message: str

    def to_dict(self) -> dict:
        return asdict(self)


def _num(value, default=0.0) -> float:
    """Postgres numerics arrive as Decimal; NULL as None."""
    return default if value is None else float(value)


# --------------------------------------------------------------------------
# Ingestion sources
# --------------------------------------------------------------------------
def classify_source(s: dict, t: dict = THRESHOLDS) -> tuple[str, str | None]:
    """Return (health label, alert severity or None). Order matters."""
    if s["state"] == "paused":
        return "paused", None
    if s.get("last_run") is None:
        return "never run", "warning"
    if s.get("last_success") is None:
        return "failing", "critical"
    if s["failures_since_success"] >= t["consecutive_failures_crit"]:
        return "failing", "critical"
    if _num(s["stale_minutes"]) > s["cadence_min"] * t["stale_factor"]:
        return "stale", "critical"
    if s.get("last_items") == 0 and _num(s.get("baseline_items")) >= t["silent_min_baseline"]:
        # The fetch "worked" but found nothing: usually the site layout changed
        # and the selectors no longer match. This is the error scrapers hide best.
        return "silent", "warning"
    if s["failures_24h"] > 0:
        return "degraded", "warning"
    return "ok", None


def _source_message(label: str, s: dict) -> str:
    last_error = s.get("last_error") or "no error message recorded"
    if label == "never run":
        return "registered but has never been fetched; check the scheduler"
    if label == "failing" and s.get("last_success") is None:
        return f"has never succeeded; last error: {last_error}"
    if label == "failing":
        return f"{s['failures_since_success']} failures in a row; last error: {last_error}"
    if label == "stale":
        return (f"no successful fetch for {_num(s['stale_minutes']):.0f} min "
                f"(cadence {s['cadence_min']} min)")
    if label == "silent":
        return (f"last successful run found 0 items (usually about "
                f"{_num(s['baseline_items']):.0f}); the page layout may have changed")
    return f"{s['failures_24h']} failed run(s) in 24h; last error: {last_error}"


def source_alerts(sources: list[dict], t: dict = THRESHOLDS) -> list[Alert]:
    """Annotates each row with 'health' and returns alerts."""
    alerts = []
    for s in sources:
        label, severity = classify_source(s, t)
        s["health"] = label
        if severity:
            alerts.append(Alert(severity, "sources", s["id"], _source_message(label, s)))
    return alerts


# --------------------------------------------------------------------------
# RAG / Jobs API routes
# --------------------------------------------------------------------------
def api_alerts(routes: list[dict], t: dict = THRESHOLDS) -> list[Alert]:
    alerts = []
    for r in routes:
        n = r["requests"]
        r["error_rate"] = r["errors_5xx"] / n if n else 0.0
        if n < t["api_min_requests"]:
            continue
        subject = f"{r['service']} {r['method']} {r['route']}"
        rate = r["error_rate"]
        if rate >= t["api_5xx_warn"]:
            sev = "critical" if rate >= t["api_5xx_crit"] else "warning"
            alerts.append(Alert(sev, "api", subject,
                                f"{rate:.1%} of {n} requests returned 5xx in the last hour"))
        p95 = _num(r.get("p95_ms"))
        if p95 >= t["api_p95_warn_ms"]:
            sev = "critical" if p95 >= t["api_p95_crit_ms"] else "warning"
            alerts.append(Alert(sev, "api", subject,
                                f"p95 latency {p95:.0f} ms in the last hour"))
    return alerts


# --------------------------------------------------------------------------
# Application errors (grouped by fingerprint)
# --------------------------------------------------------------------------
def error_alerts(groups: list[dict], now: datetime, t: dict = THRESHOLDS) -> list[Alert]:
    alerts = []
    new_since = now - timedelta(minutes=t["new_error_window_min"])
    for g in groups:
        g["is_new"] = g["first_seen"] >= new_since
        subject = f"{g['service']} {g['error_type']}"
        if g["any_critical"]:
            alerts.append(Alert("critical", "errors", subject,
                                f"critical error: {g['last_message']}"))
        elif g["count_1h"] >= t["error_burst_crit"]:
            alerts.append(Alert("critical", "errors", subject,
                                f"{g['count_1h']} occurrences in the last hour: {g['last_message']}"))
        elif g["is_new"]:
            mins = (now - g["first_seen"]).total_seconds() / 60
            alerts.append(Alert("warning", "errors", subject,
                                f"new error, first seen {mins:.0f} min ago: {g['last_message']}"))
    return alerts


# --------------------------------------------------------------------------
# Process heartbeats
# --------------------------------------------------------------------------
def heartbeat_alerts(beats: list[dict], expected=EXPECTED_SERVICES,
                     t: dict = THRESHOLDS) -> list[Alert]:
    if beats is None:   # query failed; already reported as a console problem
        return []
    alerts = []
    by_service: dict[str, list[dict]] = {}
    for b in beats:
        age = _num(b["age_s"])
        b["status"] = ("dead" if age > t["heartbeat_crit_s"]
                       else "late" if age > t["heartbeat_warn_s"] else "ok")
        by_service.setdefault(b["service"], []).append(b)

    for service in expected:
        if service not in by_service:
            alerts.append(Alert("critical", "services", service,
                                "no heartbeat has ever been recorded; the process is not running"))

    for service, instances in by_service.items():
        all_dead = all(b["status"] == "dead" for b in instances)
        for b in instances:
            if b["status"] == "ok":
                continue
            subject = f"{service}/{b['instance']}"
            age = _num(b["age_s"])
            if b["status"] == "dead":
                # One dead replica is a warning while others still serve.
                sev = "critical" if all_dead else "warning"
                alerts.append(Alert(sev, "services", subject,
                                    f"silent for {age / 60:.0f} min"))
            else:
                alerts.append(Alert("warning", "services", subject,
                                    f"last heartbeat {age:.0f} s ago"))
    return alerts


# --------------------------------------------------------------------------
# Work queue (Redis streams)
# --------------------------------------------------------------------------
def queue_alerts(q: dict | None, t: dict = THRESHOLDS) -> list[Alert]:
    if not q or "error" in q:
        reason = (q or {}).get("error", "no response")
        return [Alert("critical", "queue", "redis", f"queue stats unavailable: {reason}")]
    alerts = []
    oldest = _num(q.get("oldest_pending_age_s"))
    if oldest >= t["queue_oldest_pending_warn_s"]:
        sev = "critical" if oldest >= t["queue_oldest_pending_crit_s"] else "warning"
        alerts.append(Alert(sev, "queue", "pending",
                            f"oldest unacknowledged message is {oldest / 60:.0f} min old; "
                            f"a consumer may be stuck"))
    dead = int(q.get("dead_letter_depth") or 0)
    if dead >= t["dead_letter_warn"]:
        alerts.append(Alert("warning", "queue", "dead-letter",
                            f"{dead} message(s) in the dead-letter stream"))
    return alerts


# --------------------------------------------------------------------------
# Indexing backlog
# --------------------------------------------------------------------------
def index_alerts(ix: dict | None, t: dict = THRESHOLDS) -> list[Alert]:
    if not ix:
        return []
    alerts = []
    backlog = int(ix["backlog"] or 0)
    oldest = _num(ix.get("oldest_unindexed_min"))
    failed = int(ix["failed"] or 0)
    if backlog >= t["index_backlog_warn"]:
        alerts.append(Alert("warning", "index", "backlog",
                            f"{backlog} documents waiting to be indexed"))
    if oldest >= t["index_oldest_warn_min"]:
        sev = "critical" if oldest >= t["index_oldest_crit_min"] else "warning"
        alerts.append(Alert(sev, "index", "lag",
                            f"oldest unindexed document has waited {oldest:.0f} min; "
                            f"it is not searchable yet"))
    if failed:
        alerts.append(Alert("warning", "index", "failures",
                            f"{failed} document(s) failed to index"))
    return alerts


# --------------------------------------------------------------------------
# Quarantine
# --------------------------------------------------------------------------
def quarantine_alerts(items: list[dict], now: datetime, t: dict = THRESHOLDS) -> list[Alert]:
    alerts = []
    limit = timedelta(minutes=t["replay_stuck_min"])
    for item in items:
        sent = item.get("replayed_at")
        item["replay_stuck"] = bool(sent and now - sent > limit)
        if item["replay_stuck"]:
            mins = (now - sent).total_seconds() / 60
            alerts.append(Alert("warning", "quarantine", f"item {item['id']} ({item['source_id']})",
                                f"replay sent {mins:.0f} min ago but never confirmed"))
    return alerts


# --------------------------------------------------------------------------
# Database (Postgres)
# --------------------------------------------------------------------------
def _mins(seconds: float) -> str:
    return f"{seconds:.0f} s" if seconds < 120 else f"{seconds / 60:.0f} min"


def _short_sql(sql: str | None, n: int = 70) -> str:
    one_line = " ".join((sql or "").split())
    return one_line if len(one_line) <= n else one_line[:n - 1] + "…"


def _grade(value: float, warn: float, crit: float) -> str | None:
    return "critical" if value >= crit else "warning" if value >= warn else None


def db_alerts(db: dict | None, t: dict = THRESHOLDS) -> list[Alert]:
    """Server-level checks. When the database is unreachable this is the only
    database alert; app.collect() skips the other queries instead of raising
    one 'query failed' alert per table."""
    if not db:
        return []
    if "error" in db:
        return [Alert("critical", "database", "postgres", f"unreachable: {db['error']}")]
    alerts = []

    latency = _num(db.get("connect_ms")) + _num(db.get("rtt_ms"))
    sev = _grade(latency, t["db_latency_warn_ms"], t["db_latency_crit_ms"])
    if sev:
        alerts.append(Alert(sev, "database", "latency",
                            f"connecting and running SELECT 1 took {latency:.0f} ms"))

    if db.get("max_connections") is not None:    # absent if the stats query failed
        usable = int(db["max_connections"]) - int(db.get("reserved_connections") or 0)
        used = int(db["connections"])
        db["usable_connections"] = usable
        db["connections_used"] = used / usable if usable > 0 else 1.0
        sev = _grade(db["connections_used"], t["db_connections_warn"], t["db_connections_crit"])
        if sev:
            alerts.append(Alert(sev, "database", "connections",
                                f"{used} of {usable} connection slots in use; new "
                                f"connections will be refused when they run out"))

    xid_age = int(db.get("xid_age") or 0)
    sev = _grade(xid_age, t["db_xid_age_warn"], t["db_xid_age_crit"])
    if sev:
        alerts.append(Alert(sev, "database", "wraparound",
                            f"transaction ID age is {xid_age / 1e6:.0f} million; Postgres "
                            f"stops accepting writes near 2,100 million. Check that "
                            f"autovacuum can finish (long transactions block it)"))

    if db.get("in_recovery"):
        alerts.append(Alert("warning", "database", "read-only",
                            "connected to a read-only replica; writes, including console "
                            "actions, will fail"))
    return alerts


def classify_session(s: dict, t: dict = THRESHOLDS) -> tuple[str, str | None]:
    """Return (label, severity or None) for one row of pg_stat_activity."""
    if s.get("blocked_by"):
        return "blocked", _grade(_num(s.get("query_age_s")),
                                 t["db_blocked_warn_s"], t["db_blocked_crit_s"])
    state = s.get("state") or ""
    if state == "active":
        return "long query", _grade(_num(s.get("query_age_s")),
                                    t["db_long_query_warn_s"], t["db_long_query_crit_s"])
    if state.startswith("idle in transaction"):
        return "idle in transaction", _grade(_num(s.get("xact_age_s")),
                                             t["db_idle_in_tx_warn_s"], t["db_idle_in_tx_crit_s"])
    return state or "unknown", None


def session_alerts(sessions: list[dict], t: dict = THRESHOLDS) -> list[Alert]:
    """Annotates each row with 'label' and 'severity' and returns alerts."""
    alerts = []
    for s in sessions:
        label, sev = classify_session(s, t)
        s["label"], s["severity"] = label, sev
        if not sev:
            continue
        who = s.get("app") or s.get("user") or "unknown client"
        subject = f"pid {s['pid']} ({who})"
        query = _short_sql(s.get("query"))
        if label == "blocked":
            blockers = ", ".join(str(p) for p in s["blocked_by"])
            msg = (f"waiting {_mins(_num(s['query_age_s']))} for a lock held by pid "
                   f"{blockers}: {query}")
        elif label == "long query":
            msg = f"query running for {_mins(_num(s['query_age_s']))}: {query}"
        else:
            msg = (f"transaction open for {_mins(_num(s['xact_age_s']))} and idle; it holds "
                   f"its locks and stops vacuum. Last statement: {query}")
        alerts.append(Alert(sev, "database", subject, msg))
    return alerts


def table_alerts(tables: list[dict], t: dict = THRESHOLDS) -> list[Alert]:
    """Annotates each row with 'dead_ratio' and 'bloated' and returns alerts."""
    alerts = []
    for tb in tables:
        live, dead = int(tb.get("n_live_tup") or 0), int(tb.get("n_dead_tup") or 0)
        tb["dead_ratio"] = dead / (live + dead) if live + dead else 0.0
        tb["bloated"] = dead >= t["db_dead_rows_min"] and tb["dead_ratio"] >= t["db_dead_ratio_warn"]
        if tb["bloated"]:
            alerts.append(Alert("warning", "database", f"table {tb['table']}",
                                f"{dead:,} dead rows ({tb['dead_ratio']:.0%} of the table); "
                                f"autovacuum is not keeping up"))
    return alerts


def retention_alerts(ret: dict | None, t: dict = THRESHOLDS) -> list[Alert]:
    if not ret or ret.get("api_request_oldest_days") is None:
        return []
    oldest = _num(ret["api_request_oldest_days"])
    keep = t["api_request_retention_days"]
    if oldest > keep + 1:      # one day of grace for a daily cleanup job
        return [Alert("warning", "database", "table api_request",
                      f"oldest row is {oldest:.0f} days old (keep {keep}); the cleanup "
                      f"job is not running and the table keeps growing")]
    return []


# --------------------------------------------------------------------------
def evaluate(snap: dict, t: dict = THRESHOLDS) -> list[Alert]:
    """Run every rule over a snapshot built by admin.app.collect()."""
    now = snap["now"]
    alerts = [Alert("critical", "console", "query failed", p) for p in snap.get("problems", [])]
    alerts += source_alerts(snap.get("sources") or [], t)
    alerts += api_alerts(snap.get("routes") or [], t)
    alerts += error_alerts(snap.get("errors") or [], now, t)
    alerts += heartbeat_alerts(snap.get("heartbeats"), snap.get("expected", EXPECTED_SERVICES), t)
    alerts += queue_alerts(snap.get("queue"), t)
    alerts += index_alerts(snap.get("index"), t)
    alerts += quarantine_alerts(snap.get("quarantined") or [], now, t)
    alerts += db_alerts(snap.get("db"), t)
    alerts += session_alerts(snap.get("db_sessions") or [], t)
    alerts += table_alerts(snap.get("db_tables") or [], t)
    alerts += retention_alerts(snap.get("retention"), t)
    return sorted(alerts, key=lambda a: (_RANK[a.severity], a.area, a.subject))


def overall_status(alerts: list[Alert]) -> str:
    if any(a.severity == "critical" for a in alerts):
        return "critical"
    return "warning" if alerts else "ok"
