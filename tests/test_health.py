"""Unit tests for admin/health.py. No database or Redis needed:  pytest tests/test_health.py"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from admin import health

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def source(**over):
    row = dict(id="src", kind="jobs", state="active", cadence_min=60, owner="x",
               last_run=NOW - timedelta(minutes=10), last_success=NOW - timedelta(minutes=10),
               last_items=8, baseline_items=Decimal("9.5"), failures_24h=0,
               failures_since_success=0, last_error=None, stale_minutes=Decimal("10"))
    row.update(over)
    return row


@pytest.mark.parametrize("over, label, severity", [
    ({}, "ok", None),
    ({"state": "paused", "stale_minutes": Decimal("5000")}, "paused", None),
    ({"last_run": None, "last_success": None, "stale_minutes": None}, "never run", "warning"),
    ({"last_success": None, "stale_minutes": None, "failures_since_success": 2}, "failing", "critical"),
    ({"failures_since_success": 3}, "failing", "critical"),
    ({"stale_minutes": Decimal("181")}, "stale", "critical"),
    ({"stale_minutes": Decimal("179")}, "ok", None),
    ({"last_items": 0}, "silent", "warning"),
    ({"last_items": 0, "baseline_items": Decimal("1")}, "ok", None),  # source is usually quiet
    ({"failures_24h": 1}, "degraded", "warning"),
])
def test_classify_source(over, label, severity):
    assert health.classify_source(source(**over)) == (label, severity)


def test_failing_wins_over_stale():
    s = source(failures_since_success=4, stale_minutes=Decimal("400"))
    assert health.classify_source(s) == ("failing", "critical")


def route(requests, errors_5xx=0, p95=100):
    return dict(service="rag-api", method="POST", route="/rag/query", requests=requests,
                errors_5xx=errors_5xx, errors_4xx=0, p95_ms=p95)


@pytest.mark.parametrize("r, expected", [
    (route(100, 1), []),                                   # 1%
    (route(100, 2), ["warning"]),                          # 2%
    (route(100, 10), ["critical"]),                        # 10%
    (route(10, 9), []),                                    # too little traffic
    (route(100, 0, p95=2500), ["warning"]),
    (route(100, 0, p95=6000), ["critical"]),
    (route(100, 20, p95=9000), ["critical", "critical"]),
])
def test_api_alerts(r, expected):
    assert [a.severity for a in health.api_alerts([r])] == expected


def group(**over):
    g = dict(fingerprint="fp", service="indexer", component="c", error_type="ValueError",
             last_message="boom", any_critical=False, count_1h=1, count_24h=5,
             first_seen=NOW - timedelta(days=3))
    g.update(over)
    return g


def test_error_alerts():
    assert health.error_alerts([group()], NOW) == []
    assert health.error_alerts([group(count_1h=20)], NOW)[0].severity == "critical"
    assert health.error_alerts([group(any_critical=True)], NOW)[0].severity == "critical"
    new = health.error_alerts([group(first_seen=NOW - timedelta(minutes=30))], NOW)
    assert new[0].severity == "warning" and "new error" in new[0].message


def beat(service, instance, age):
    return dict(service=service, instance=instance, version="v", last_seen=NOW,
                age_s=Decimal(age))


def test_heartbeats():
    beats = [beat("rag-api", "r1", 10), beat("ingest-worker", "w1", 10),
             beat("ingest-worker", "w2", 900), beat("jobs-api", "j1", 200),
             beat("indexer", "i1", 700)]
    alerts = {a.subject: a.severity for a in health.heartbeat_alerts(
        beats, expected=("rag-api", "ingest-worker", "jobs-api", "indexer", "scheduler"))}
    assert alerts == {
        "scheduler": "critical",              # never reported
        "ingest-worker/w2": "warning",        # dead, but w1 still running
        "jobs-api/j1": "warning",             # late
        "indexer/i1": "critical",             # only instance is dead
    }


def test_heartbeat_query_failure_does_not_report_everything_down():
    assert health.heartbeat_alerts(None) == []


def test_queue_alerts():
    assert health.queue_alerts(None)[0].severity == "critical"
    assert health.queue_alerts({"error": "ConnectionError"})[0].severity == "critical"
    ok = {"stream_length": 5, "pending": 0, "oldest_pending_age_s": 0, "dead_letter_depth": 0}
    assert health.queue_alerts(ok) == []
    stuck = dict(ok, oldest_pending_age_s=2000, dead_letter_depth=2)
    assert sorted(a.severity for a in health.queue_alerts(stuck)) == ["critical", "warning"]


def test_index_alerts():
    assert health.index_alerts({"backlog": 3, "failed": 0, "oldest_unindexed_min": 2}) == []
    sev = [a.severity for a in health.index_alerts(
        {"backlog": 60, "failed": 1, "oldest_unindexed_min": Decimal("130")})]
    assert sorted(sev) == ["critical", "warning", "warning"]


def test_replay_stuck():
    items = [dict(id=1, source_id="s", replayed_at=NOW - timedelta(minutes=40)),
             dict(id=2, source_id="s", replayed_at=NOW - timedelta(minutes=5)),
             dict(id=3, source_id="s", replayed_at=None)]
    alerts = health.quarantine_alerts(items, NOW)
    assert [a.subject for a in alerts] == ["item 1 (s)"]
    assert [i["replay_stuck"] for i in items] == [True, False, False]


def test_evaluate_orders_critical_first_and_sets_status():
    snap = {"now": NOW, "sources": [source(failures_24h=1), source(id="b", stale_minutes=999)],
            "routes": [], "errors": [], "heartbeats": [], "expected": (),
            "queue": {"oldest_pending_age_s": 0, "dead_letter_depth": 0},
            "index": None, "quarantined": [], "problems": ["sources: UndefinedTable"]}
    alerts = health.evaluate(snap)
    assert [a.severity for a in alerts] == ["critical", "critical", "warning"]
    assert health.overall_status(alerts) == "critical"
    assert health.overall_status([]) == "ok"
