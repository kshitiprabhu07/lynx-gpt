-- Stand-ins for the tables Lynx GPT already has, so the console can be tested
-- on an empty database. Do NOT run this on the real database; use the real
-- migrations there. Column names match what admin/app.py queries.

CREATE TABLE IF NOT EXISTS source (
    id           text PRIMARY KEY,
    kind         text NOT NULL,                  -- notice | jobs
    state        text NOT NULL DEFAULT 'active', -- active | paused
    cadence_min  int  NOT NULL,
    owner        text
);

CREATE TABLE IF NOT EXISTS fetch_run (
    id           bigserial PRIMARY KEY,
    source_id    text NOT NULL REFERENCES source(id),
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz,
    status       text NOT NULL,                  -- success | failed | running
    items_found  int  NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS quarantine (
    id           bigserial PRIMARY KEY,
    source_id    text NOT NULL REFERENCES source(id),
    reason       text NOT NULL,
    payload_ref  text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    resolved     boolean NOT NULL DEFAULT false
);

-- Ingested documents and their indexing state. If the data layer's table is
-- named differently, change INDEX_SQL in admin/app.py, not this file.
CREATE TABLE IF NOT EXISTS document (
    id           bigserial PRIMARY KEY,
    source_id    text NOT NULL REFERENCES source(id),
    ingested_at  timestamptz NOT NULL DEFAULT now(),
    indexed_at   timestamptz,
    index_error  text
);
