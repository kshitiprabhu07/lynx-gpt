from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
import psycopg
from psycopg.rows import dict_row

from lynx.config import POSTGRES_DSN, REDIS_URL
from lynx.queue.streams import WorkQueue

app = FastAPI(title="Lynx GPT — Admin Console")
templates = Jinja2Templates(directory="admin/templates")
queue = WorkQueue(REDIS_URL, group="admin", consumer="console")

HEALTH_SQL = """
SELECT s.id, s.kind, s.state, s.cadence_min, s.owner,
       MAX(f.started_at) FILTER (WHERE f.status = 'success') AS last_success,
       COUNT(*) FILTER (WHERE f.status = 'failed'
                        AND f.started_at > now() - interval '24 hours') AS failures_24h,
       COALESCE(AVG(f.items_found) FILTER (WHERE f.status = 'success'), 0) AS avg_items,
       EXTRACT(EPOCH FROM (now() - MAX(f.started_at)
               FILTER (WHERE f.status = 'success'))) / 60 AS stale_minutes
FROM source s LEFT JOIN fetch_run f ON f.source_id = s.id
GROUP BY s.id, s.kind, s.state, s.cadence_min, s.owner
ORDER BY stale_minutes DESC NULLS FIRST;
"""


def q(sql, params=()):
    with psycopg.connect(POSTGRES_DSN, row_factory=dict_row) as conn:
        return conn.execute(sql, params).fetchall()


@app.get("/admin")
def dashboard(request: Request):
    sources = q(HEALTH_SQL)
    quarantined = q("""SELECT q.*, s.kind FROM quarantine q
                       JOIN source s ON s.id = q.source_id
                       WHERE NOT q.resolved
                       ORDER BY q.created_at DESC LIMIT 50""")
    for s in sources:
        stale = s["stale_minutes"]
        s["health"] = ("never run" if stale is None
                       else "stale" if stale > s["cadence_min"] * 3
                       else "degraded" if s["failures_24h"] > 0
                       else "ok")
    return templates.TemplateResponse(request, "dashboard.html", {
        "sources": sources,
        "quarantined": quarantined,
        "queue": queue.depth(),
    })


@app.post("/admin/source/{source_id}/pause")
def pause(source_id: str):
    with psycopg.connect(POSTGRES_DSN) as conn:
        conn.execute("UPDATE source SET state='paused' WHERE id=%s", (source_id,))
    return RedirectResponse("/admin", status_code=303)


@app.post("/admin/quarantine/{item_id}/replay")
def replay(item_id: int):
    """Replay from the raw snapshot — no re-fetch. This is why snapshots exist."""
    rows = q("SELECT source_id, payload_ref FROM quarantine WHERE id=%s", (item_id,))
    if rows:
        queue.publish("snapshot.replay", {
            "event_id": f"replay-{item_id}",
            "source_id": rows[0]["source_id"],
            "snapshot_ref": rows[0]["payload_ref"],
        })
        with psycopg.connect(POSTGRES_DSN) as conn:
            conn.execute("UPDATE quarantine SET resolved=true WHERE id=%s", (item_id,))
    return RedirectResponse("/admin", status_code=303)