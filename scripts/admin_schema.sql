CREATE TABLE IF NOT EXISTS source (
    id            TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    base_url      TEXT NOT NULL,
    cadence_min   INT  NOT NULL DEFAULT 60,
    state         TEXT NOT NULL DEFAULT 'active',
    adapter_ver   TEXT,
    owner         TEXT
);

CREATE TABLE IF NOT EXISTS fetch_run (
    id            BIGSERIAL PRIMARY KEY,
    source_id     TEXT REFERENCES source(id),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    status        TEXT NOT NULL,
    http_status   INT,
    items_found   INT DEFAULT 0,
    error_class   TEXT
);

CREATE TABLE IF NOT EXISTS quarantine (
    id            BIGSERIAL PRIMARY KEY,
    source_id     TEXT REFERENCES source(id),
    reason        TEXT NOT NULL,
    payload_ref   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved      BOOLEAN NOT NULL DEFAULT false
);

-- Demo data: one healthy source, one stale, one failing.
INSERT INTO source (id, kind, base_url, cadence_min, state, adapter_ver, owner) VALUES
  ('nitt_notices', 'nitt',    'https://www.nitt.edu/home/', 30, 'active', 'v1.2', 'shreeya'),
  ('acme_careers', 'workday', 'https://acme.wd1.myworkdayjobs.com', 120, 'active', 'v1.0', 'sriniketh'),
  ('globex_jobs',  'jsonld',  'https://globex.example.com/careers', 60, 'active', 'v0.9', 'shreeya')
ON CONFLICT (id) DO NOTHING;

INSERT INTO fetch_run (source_id, started_at, status, http_status, items_found) VALUES
  ('nitt_notices', now() - interval '10 minutes', 'success', 200, 42),
  ('acme_careers', now() - interval '9 hours',    'success', 200, 15),
  ('acme_careers', now() - interval '1 hour',     'failed',  503, 0),
  ('globex_jobs',  now() - interval '20 minutes', 'success', 200, 0);

INSERT INTO quarantine (source_id, reason, payload_ref) VALUES
  ('globex_jobs', 'zero items extracted — possible selector drift', 'snapshots/globex/2026-09-13T08:00Z'),
  ('acme_careers', 'MIME type not allowed: application/zip', 'snapshots/acme/2026-09-13T07:00Z');