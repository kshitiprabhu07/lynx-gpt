"""Unit tests for the database rules in admin/health.py. No database needed."""
from datetime import datetime, timezone

from admin import health
from admin.health import (classify_session, db_alerts, retention_alerts, session_alerts,
                          table_alerts)

HEALTHY = {"connect_ms": 4.0, "rtt_ms": 0.5, "max_connections": 100, "reserved_connections": 3,
           "connections": 12, "active": 2, "xid_age": 2_000_000, "in_recovery": False}


def sevs(alerts):
    return [(a.severity, a.subject) for a in alerts]


def test_healthy_server_raises_nothing():
    db = dict(HEALTHY)
    assert db_alerts(db) == []
    assert db["usable_connections"] == 97


def test_unreachable_is_one_critical_alert():
    assert sevs(db_alerts({"error": "OperationalError: connection refused"})) == \
        [("critical", "postgres")]


def test_latency():
    assert sevs(db_alerts({**HEALTHY, "connect_ms": 300})) == [("warning", "latency")]
    assert sevs(db_alerts({**HEALTHY, "connect_ms": 900, "rtt_ms": 150})) == [("critical", "latency")]


def test_connection_slots_exclude_reserved():
    assert sevs(db_alerts({**HEALTHY, "connections": 78})) == [("warning", "connections")]  # 78/97
    assert sevs(db_alerts({**HEALTHY, "connections": 93})) == [("critical", "connections")]


def test_missing_server_stats_still_checks_latency():
    assert db_alerts({"connect_ms": 3, "rtt_ms": 1}) == []


def test_wraparound_and_replica():
    assert sevs(db_alerts({**HEALTHY, "xid_age": 600_000_000})) == [("warning", "wraparound")]
    assert sevs(db_alerts({**HEALTHY, "xid_age": 1_200_000_000})) == [("critical", "wraparound")]
    assert sevs(db_alerts({**HEALTHY, "in_recovery": True})) == [("warning", "read-only")]


def session(**kw):
    base = {"pid": 4242, "app": "rag-api", "user": "lynx", "state": "active",
            "query_age_s": 1, "xact_age_s": 1, "blocked_by": [], "query": "SELECT 1"}
    return {**base, **kw}


def test_session_classification():
    assert classify_session(session(query_age_s=5)) == ("long query", None)
    assert classify_session(session(query_age_s=90)) == ("long query", "warning")
    assert classify_session(session(query_age_s=400)) == ("long query", "critical")
    assert classify_session(session(state="idle in transaction", xact_age_s=400)) == \
        ("idle in transaction", "warning")
    assert classify_session(session(state="idle in transaction (aborted)", xact_age_s=2000)) == \
        ("idle in transaction", "critical")
    # blocked wins over "long query": the lock is the problem, not the query
    assert classify_session(session(query_age_s=45, blocked_by=[17])) == ("blocked", "warning")
    assert classify_session(session(state=None)) == ("unknown", None)


def test_session_alert_messages():
    rows = [session(pid=1, query_age_s=40, blocked_by=[7, 8], query="UPDATE   source\n SET x=1"),
            session(pid=2, app="", user="indexer", state="idle in transaction", xact_age_s=600),
            session(pid=3, query_age_s=10)]
    alerts = session_alerts(rows)
    assert [a.subject for a in alerts] == ["pid 1 (rag-api)", "pid 2 (indexer)"]
    assert "lock held by pid 7, 8: UPDATE source SET x=1" in alerts[0].message
    assert "transaction open for 10 min" in alerts[1].message
    assert rows[2]["severity"] is None                      # annotated for the template


def test_dead_rows_need_both_count_and_share():
    tables = [{"table": "api_request", "n_live_tup": 40_000, "n_dead_tup": 15_000},  # 27% -> warn
              {"table": "big", "n_live_tup": 10_000_000, "n_dead_tup": 50_000},      # 0.5%
              {"table": "tiny", "n_live_tup": 10, "n_dead_tup": 900},                # too few
              {"table": "empty", "n_live_tup": 0, "n_dead_tup": 0}]
    assert sevs(table_alerts(tables)) == [("warning", "table api_request")]
    assert tables[3]["dead_ratio"] == 0.0


def test_retention_has_a_day_of_grace():
    assert retention_alerts({"api_request_oldest_days": 7.9}) == []
    assert sevs(retention_alerts({"api_request_oldest_days": 8.5})) == \
        [("warning", "table api_request")]
    assert retention_alerts({"api_request_oldest_days": None}) == []   # empty table
    assert retention_alerts(None) == []


def test_evaluate_includes_database_area():
    snap = {"now": datetime.now(timezone.utc),
            "queue": {"stream_length": 0}, "heartbeats": None,
            "db": {"error": "down"}}
    alerts = health.evaluate(snap)
    assert ("critical", "database", "postgres") in [(a.severity, a.area, a.subject) for a in alerts]
