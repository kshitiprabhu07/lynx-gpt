"""Database-status checks against a real Postgres.

Opens real sessions that hold locks, wait on them, and run long queries, then
checks the console reports each one. Thresholds are lowered to ~1 s so the
test runs in seconds.

    export LYNX_TEST_DSN="postgresql://localhost/lynx_test"   # name must contain "test"
    pytest tests/test_db_e2e.py
"""
from __future__ import annotations

import os
import pathlib
import threading
import time

import psycopg
import pytest

os.environ.setdefault("LYNX_ADMIN_PASSWORD", "dev")
ROOT = pathlib.Path(__file__).resolve().parents[1]
os.environ.setdefault("LYNX_ADMIN_QUEUE_FIXTURE", str(ROOT / "admin/testdata/queue_fixture.json"))

from fastapi.testclient import TestClient  # noqa: E402

from admin import app as console, health  # noqa: E402

DSN = os.environ.get("LYNX_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set LYNX_TEST_DSN to a test database")

# The database's own alerts are what this file tests; the seed's 7 critical
# and 14 warnings belong to the other areas.
SEED_CRITICAL, SEED_WARNING = 7, 14


def _run_sql_file(conn, path):
    sql = (ROOT / path).read_text()
    sql = "\n".join(l for l in sql.splitlines() if not l.startswith("\\"))  # drop psql meta-commands
    conn.execute(sql)


@pytest.fixture(scope="module", autouse=True)
def seeded():
    with psycopg.connect(DSN, autocommit=True) as conn:
        assert "test" in conn.info.dbname
        for f in ("admin/testdata/base_schema.sql", "admin/sql/observability.sql",
                  "admin/testdata/seed.sql"):
            _run_sql_file(conn, f)


@pytest.fixture(autouse=True)
def console_on_test_db(monkeypatch):
    monkeypatch.setattr(console, "DSN", DSN)
    monkeypatch.setattr(console, "QUEUE_FIXTURE", os.environ["LYNX_ADMIN_QUEUE_FIXTURE"])


@pytest.fixture
def fast_thresholds(monkeypatch):
    for key in ("db_long_query_warn_s", "db_idle_in_tx_warn_s", "db_blocked_warn_s"):
        monkeypatch.setitem(health.THRESHOLDS, key, 1)


def db_alerts(snap):
    return [a for a in snap["alerts"] if a.area == "database"]


def test_seeded_db_is_healthy():
    snap = console.collect()
    assert db_alerts(snap) == []
    assert not snap["problems"]
    sev = [a.severity for a in snap["alerts"]]
    assert (sev.count("critical"), sev.count("warning")) == (SEED_CRITICAL, SEED_WARNING)
    db = snap["db"]
    assert db["usable_connections"] > 0 and db["connections"] >= 1
    assert db["in_recovery"] is False


def test_lock_holder_blocked_and_long_queries(fast_thresholds):
    holder = psycopg.connect(DSN, application_name="test-holder")
    holder.execute("SELECT * FROM source WHERE id = 'nitt-notices' FOR UPDATE")  # tx left open
    holder_pid = holder.info.backend_pid

    waiter = psycopg.connect(DSN, application_name="test-waiter")
    sleeper = psycopg.connect(DSN, application_name="test-sleeper", autocommit=True)
    pids = {"waiter": waiter.info.backend_pid, "sleeper": sleeper.info.backend_pid}

    def wait_for_lock():
        try:
            waiter.execute("UPDATE source SET owner = owner WHERE id = 'nitt-notices'")
        finally:
            waiter.rollback()

    def sleep_long():
        try:
            sleeper.execute("SELECT pg_sleep(30)")
        except psycopg.errors.QueryCanceled:
            pass

    threads = [threading.Thread(target=wait_for_lock), threading.Thread(target=sleep_long)]
    for th in threads:
        th.start()
    try:
        time.sleep(2.0)
        snap = console.collect()
        by_subject = {a.subject: a for a in db_alerts(snap)}

        blocked = by_subject[f"pid {pids['waiter']} (test-waiter)"]
        assert blocked.severity == "warning"
        assert f"lock held by pid {holder_pid}" in blocked.message

        long_q = by_subject[f"pid {pids['sleeper']} (test-sleeper)"]
        assert "query running for" in long_q.message and "pg_sleep" in long_q.message

        idle = by_subject[f"pid {holder_pid} (test-holder)"]
        assert "transaction open for" in idle.message

        # the console never reports itself
        assert all(s["app"] != "lynx-admin-console" for s in snap["db_sessions"])

        html = TestClient(console.app).get("/admin", auth=("admin", "dev")).text
        assert "Sessions needing attention" in html and "test-waiter" in html
        assert f"pid {holder_pid}" in html            # the "Waiting on" column
    finally:
        holder.rollback()
        holder.close()
        with psycopg.connect(DSN, autocommit=True) as c:
            c.execute("SELECT pg_cancel_backend(%s)", (pids["sleeper"],))
        for th in threads:
            th.join(10)
        waiter.close()
        sleeper.close()

    assert db_alerts(console.collect()) == []


def test_statement_timeout_keeps_the_page_alive(monkeypatch):
    monkeypatch.setattr(console, "STATEMENT_TIMEOUT_MS", 500)
    locker = psycopg.connect(DSN)
    locker.execute("LOCK TABLE quarantine IN ACCESS EXCLUSIVE MODE")
    try:
        start = time.perf_counter()
        snap = console.collect()
        elapsed = time.perf_counter() - start
    finally:
        locker.rollback()
        locker.close()
    assert elapsed < 4
    assert any(p.startswith("quarantine: QueryCanceled") for p in snap["problems"])
    assert snap["sources"] and snap["routes"]            # the rest still loaded


def test_database_unreachable(monkeypatch):
    monkeypatch.setattr(console, "DSN", "postgresql://127.0.0.1:1/lynx_test")
    snap = console.collect()
    assert [(a.severity, a.subject) for a in db_alerts(snap)] == [("critical", "postgres")]
    assert not any(a.area == "console" for a in snap["alerts"])   # no per-table noise
    client = TestClient(console.app)
    page = client.get("/admin", auth=("admin", "dev"))
    assert page.status_code == 200 and "Unreachable." in page.text
    assert "<h2>Sources</h2>" not in page.text
    body = client.get("/admin/api/health", auth=("admin", "dev"))
    assert body.status_code == 503 and body.json()["database"]["reachable"] is False


def test_api_request_retention():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("""INSERT INTO api_request (ts, service, route, method, status, latency_ms)
                     VALUES (now() - interval '10 days', 'jobs-api', '/jobs', 'GET', 200, 50)""")
    try:
        alerts = db_alerts(console.collect())
        assert [a.subject for a in alerts] == ["table api_request"]
        assert "10 days old" in alerts[0].message
    finally:
        with psycopg.connect(DSN, autocommit=True) as c:
            c.execute("DELETE FROM api_request WHERE ts < now() - interval '2 days'")


def test_dead_rows_flagged():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS bloat_probe")
        c.execute("CREATE TABLE bloat_probe (id int) WITH (autovacuum_enabled = false)")
        c.execute("INSERT INTO bloat_probe SELECT generate_series(1, 20000)")
        c.execute("DELETE FROM bloat_probe WHERE id > 5000")
        c.execute("SELECT pg_stat_force_next_flush()")
    try:
        deadline = time.time() + 5          # statistics reach the views asynchronously
        while time.time() < deadline:
            subjects = [a.subject for a in db_alerts(console.collect())]
            if "table bloat_probe" in subjects:
                break
            time.sleep(0.3)
        assert "table bloat_probe" in subjects
    finally:
        with psycopg.connect(DSN, autocommit=True) as c:
            c.execute("DROP TABLE bloat_probe")
