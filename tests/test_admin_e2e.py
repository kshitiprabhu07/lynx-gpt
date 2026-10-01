"""End-to-end test against a real Postgres loaded with admin/testdata/seed.sql.

    createdb lynx_test
    LYNX_TEST_DSN="postgresql://localhost/lynx_test" pytest tests/test_admin_e2e.py

Skipped when LYNX_TEST_DSN is not set. The database name must contain
"test", because the seed truncates tables.
"""
import os
from pathlib import Path

import psycopg
import pytest

DSN = os.environ.get("LYNX_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set LYNX_TEST_DSN to run")

ROOT = Path(__file__).resolve().parents[1]
AUTH = ("admin", "test-password")
SAME_ORIGIN = {"origin": "http://testserver"}

# Every alert the seed is built to trigger: (severity, area, subject).
EXPECTED = {
    ("critical", "api", "rag-api POST /rag/query"),          # 5xx rate
    ("critical", "errors", "notifier SMTPAuthenticationError"),
    ("critical", "errors", "rag-api ReadTimeout"),
    ("critical", "services", "scheduler"),
    ("critical", "sources", "careers-freshworks"),
    ("critical", "sources", "nitt-academics"),
    ("warning", "api", "jobs-api GET /jobs/search"),
    ("warning", "api", "jobs-api GET /jobs/{job_id}"),
    ("warning", "errors", "indexer ValueError"),
    ("warning", "index", "backlog"),
    ("warning", "index", "failures"),
    ("warning", "index", "lag"),
    ("warning", "quarantine", "item 3 (careers-tcs)"),
    ("warning", "queue", "dead-letter"),
    ("warning", "queue", "pending"),
    ("warning", "services", "ingest-worker/worker-2"),
    ("warning", "services", "jobs-api/jobs-1"),
    ("warning", "sources", "careers-infosys"),
    ("warning", "sources", "careers-tcs"),
    ("warning", "sources", "careers-zoho"),
}
# rag-api /rag/query also raises a second critical alert for p95 latency.
EXPECTED_COUNTS = {"critical": 7, "warning": 14}


def run_sql_file(path: Path) -> None:
    # psql meta-commands (\set) are not SQL; psycopg runs the rest.
    sql = "\n".join(l for l in path.read_text().splitlines() if not l.startswith("\\"))
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql)


class FakeQueue:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload):
        self.published.append((topic, payload))


@pytest.fixture()
def client(monkeypatch):
    run_sql_file(ROOT / "admin/testdata/base_schema.sql")
    run_sql_file(ROOT / "admin/sql/observability.sql")
    run_sql_file(ROOT / "admin/testdata/seed.sql")

    from fastapi.testclient import TestClient
    from admin import app as admin_app

    monkeypatch.setattr(admin_app, "DSN", DSN)
    monkeypatch.setattr(admin_app, "ADMIN_PASSWORD", AUTH[1])
    monkeypatch.setattr(admin_app, "QUEUE_FIXTURE",
                        str(ROOT / "admin/testdata/queue_fixture.json"))
    fake = FakeQueue()
    monkeypatch.setattr(admin_app, "_get_queue", lambda: fake)
    c = TestClient(admin_app.app)
    c.fake_queue = fake
    return c


def alerts(client):
    r = client.get("/admin/api/health", auth=AUTH)
    return r, {(a["severity"], a["area"], a["subject"]) for a in r.json()["alerts"]}


def test_requires_login(client):
    assert client.get("/admin").status_code == 401
    assert client.get("/admin", auth=("admin", "wrong")).status_code == 401


def test_detects_every_seeded_failure(client):
    r, found = alerts(client)
    assert r.status_code == 503                      # critical -> 503 for monitors
    body = r.json()
    assert {"critical": body["critical"], "warning": body["warning"]} == EXPECTED_COUNTS
    assert found == EXPECTED, (f"missing: {EXPECTED - found}\n"
                               f"unexpected: {found - EXPECTED}")


def test_no_false_alarms(client):
    _, found = alerts(client)
    subjects = {s for _, _, s in found}
    assert "nitt-notices" not in subjects            # healthy source
    assert "nitt-hostel" not in subjects             # paused source
    assert "jobs-api GET /jobs" not in subjects      # 1% errors is under the line
    assert not any("apply" in s for s in subjects)   # too little traffic to judge
    assert not any("ConnectionResetError" in s for s in subjects)  # acknowledged


def test_dashboard_renders(client):
    html = client.get("/admin", auth=AUTH).text
    assert "7 critical, 14 warnings" in html
    assert "careers-tcs" in html and "silent" in html
    assert "ConnectionResetError" not in html        # acknowledged group is hidden
    assert "dedupe: two notices" not in html         # resolved quarantine item


def test_post_from_another_site_is_blocked(client):
    r = client.post("/admin/source/careers-tcs/pause", auth=AUTH,
                    headers={"origin": "https://evil.example"})
    assert r.status_code == 403


def test_pause_then_resume(client):
    r = client.post("/admin/source/careers-tcs/pause", auth=AUTH,
                    headers=SAME_ORIGIN, follow_redirects=False)
    assert r.status_code == 303
    _, found = alerts(client)
    assert ("warning", "sources", "careers-tcs") not in found
    client.post("/admin/source/careers-tcs/resume", auth=AUTH, headers=SAME_ORIGIN)
    _, found = alerts(client)
    assert ("warning", "sources", "careers-tcs") in found
    assert client.post("/admin/source/nope/pause", auth=AUTH,
                       headers=SAME_ORIGIN).status_code == 404


def test_acknowledge_hides_error_until_it_recurs(client):
    client.post("/admin/errors/indexer-dim-mismatch/ack", auth=AUTH, headers=SAME_ORIGIN)
    _, found = alerts(client)
    assert ("warning", "errors", "indexer ValueError") not in found
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("""INSERT INTO app_error (occurred_at, service, component, error_type,
                                               message, fingerprint)
                        VALUES (now() + interval '1 second', 'indexer', 'embed_batch',
                                'ValueError', 'embedding dimension mismatch', 'indexer-dim-mismatch')""")
    _, found = alerts(client)
    assert ("warning", "errors", "indexer ValueError") in found


def test_replay_keeps_item_until_worker_confirms(client):
    r = client.post("/admin/quarantine/1/replay", auth=AUTH, headers=SAME_ORIGIN,
                    follow_redirects=False)
    assert r.status_code == 303
    topic, payload = client.fake_queue.published[-1]
    assert topic == "snapshot.replay"
    assert payload["event_id"] == "replay-1-1" and payload["quarantine_id"] == 1
    assert "Replay again" in client.get("/admin", auth=AUTH).text  # still listed

    client.post("/admin/quarantine/1/replay", auth=AUTH, headers=SAME_ORIGIN)
    assert client.fake_queue.published[-1][1]["event_id"] == "replay-1-2"

    # what the replay worker does on success
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("UPDATE quarantine SET resolved = true WHERE id = 1")
    assert client.post("/admin/quarantine/1/replay", auth=AUTH,
                       headers=SAME_ORIGIN).status_code == 404
