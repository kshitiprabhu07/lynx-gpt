-- Observability tables for the Lynx GPT admin console.
-- Safe to run on the real database: every statement is idempotent and only
-- adds tables/columns. Run it after the existing schema (source, fetch_run,
-- quarantine) is in place.

-- Every service (ingest workers, indexer, RAG API, Jobs API, notifier,
-- scheduler) writes its errors here through lynx.observability.report.
CREATE TABLE IF NOT EXISTS app_error (
    id           bigserial PRIMARY KEY,
    occurred_at  timestamptz NOT NULL DEFAULT now(),
    service      text        NOT NULL,          -- rag-api, jobs-api, indexer, ...
    component    text,                          -- route template, adapter, function
    severity     text        NOT NULL DEFAULT 'error'
                 CHECK (severity IN ('warning', 'error', 'critical')),
    error_type   text        NOT NULL,          -- exception class name
    message      text        NOT NULL,
    fingerprint  text        NOT NULL,          -- groups repeats of the same error
    request_id   text,                          -- ties the error to an api_request row
    context      jsonb
);
CREATE INDEX IF NOT EXISTS app_error_time_idx ON app_error (occurred_at DESC);
CREATE INDEX IF NOT EXISTS app_error_fp_idx   ON app_error (fingerprint, occurred_at DESC);

-- "Acknowledge" hides an error group until it happens again.
CREATE TABLE IF NOT EXISTS error_ack (
    fingerprint  text PRIMARY KEY,
    acked_at     timestamptz NOT NULL DEFAULT now(),
    acked_by     text
);

-- One row per HTTP request to the RAG and Jobs APIs, written by the
-- middleware in lynx.observability.report. Keep about a week of it:
--   DELETE FROM api_request WHERE ts < now() - interval '7 days';
CREATE TABLE IF NOT EXISTS api_request (
    id          bigserial PRIMARY KEY,
    ts          timestamptz NOT NULL DEFAULT now(),
    service     text NOT NULL,
    route       text NOT NULL,      -- route template (/jobs/{job_id}), never the raw path
    method      text NOT NULL,
    status      int  NOT NULL,
    latency_ms  int  NOT NULL,
    request_id  text
);
CREATE INDEX IF NOT EXISTS api_request_ts_idx ON api_request (ts DESC);

-- Each long-running process upserts its own row every ~30 s.
CREATE TABLE IF NOT EXISTS service_heartbeat (
    service    text NOT NULL,
    instance   text NOT NULL,
    last_seen  timestamptz NOT NULL DEFAULT now(),
    version    text,
    PRIMARY KEY (service, instance)
);

-- Adapters should store the failure reason on failed runs.
ALTER TABLE fetch_run  ADD COLUMN IF NOT EXISTS error text;

-- A replayed item now stays visible until the worker confirms it succeeded.
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS replayed_at  timestamptz;
ALTER TABLE quarantine ADD COLUMN IF NOT EXISTS replay_count int NOT NULL DEFAULT 0;
