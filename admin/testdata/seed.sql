-- Dummy data for the Lynx GPT admin console.
-- All timestamps are relative to now(), so the scenarios stay valid whenever
-- you run it. It TRUNCATES tables, so it refuses to run unless the database
-- name contains "test".
--
--   psql "$LYNX_TEST_DSN" -f admin/testdata/seed.sql
--
-- The scenario each block sets up is noted above it; README.md lists the
-- alerts you should see.

\set ON_ERROR_STOP on

DO $$
BEGIN
  IF current_database() NOT LIKE '%test%' THEN
    RAISE EXCEPTION 'seed.sql will not run on "%": the database name must contain "test"',
                    current_database();
  END IF;
END $$;

TRUNCATE api_request, app_error, error_ack, service_heartbeat,
         quarantine, fetch_run, document, source RESTART IDENTITY CASCADE;

-- --------------------------------------------------------------------------
-- Sources
-- --------------------------------------------------------------------------
INSERT INTO source (id, kind, state, cadence_min, owner) VALUES
  ('nitt-notices',       'notice', 'active', 30,  'Shreeya'),    -- healthy
  ('nitt-academics',     'notice', 'active', 60,  'Shreeya'),    -- stale
  ('careers-zoho',       'jobs',   'active', 60,  'Sriniketh'),  -- degraded
  ('careers-freshworks', 'jobs',   'active', 60,  'Sriniketh'),  -- failing
  ('careers-tcs',        'jobs',   'active', 120, 'Sriniketh'),  -- silent (0 items)
  ('nitt-hostel',        'notice', 'paused', 60,  'Shreeya'),    -- paused, no alert
  ('careers-infosys',    'jobs',   'active', 60,  'Sriniketh');  -- never run

-- Healthy: every 30 min for 24 h, 3-8 items each time.
INSERT INTO fetch_run (source_id, started_at, finished_at, status, items_found)
SELECT 'nitt-notices', t, t + interval '18 seconds', 'success', 3 + n % 6
FROM generate_series(0, 47) n
CROSS JOIN LATERAL (SELECT now() - interval '5 minutes' - n * interval '30 minutes' AS t) x;

-- Stale: hourly successes that stopped 5 hours ago (cadence 60 -> limit 180 min).
INSERT INTO fetch_run (source_id, started_at, finished_at, status, items_found)
SELECT 'nitt-academics', t, t + interval '25 seconds', 'success', 1 + n % 3
FROM generate_series(5, 28) n
CROSS JOIN LATERAL (SELECT now() - n * interval '1 hour' AS t) x;

-- Degraded: 2 timeouts in the last 24 h, latest run fine.
INSERT INTO fetch_run (source_id, started_at, finished_at, status, items_found, error)
SELECT 'careers-zoho', t, t + interval '20 seconds',
       CASE WHEN n IN (7, 15) THEN 'failed' ELSE 'success' END,
       CASE WHEN n IN (7, 15) THEN 0 ELSE 8 + n % 4 END,
       CASE WHEN n IN (7, 15) THEN 'ReadTimeout: careers.zohocorp.com did not respond within 20s' END
FROM generate_series(0, 23) n
CROSS JOIN LATERAL (SELECT now() - interval '10 minutes' - n * interval '1 hour' AS t) x;

-- Failing: the last 4 runs hit bot protection.
INSERT INTO fetch_run (source_id, started_at, finished_at, status, items_found, error)
SELECT 'careers-freshworks', t, t + interval '3 seconds',
       CASE WHEN n < 4 THEN 'failed' ELSE 'success' END,
       CASE WHEN n < 4 THEN 0 ELSE 5 + n % 3 END,
       CASE WHEN n < 4 THEN 'HTTPStatusError: 403 Forbidden (bot-protection page returned)' END
FROM generate_series(0, 23) n
CROSS JOIN LATERAL (SELECT now() - interval '10 minutes' - n * interval '1 hour' AS t) x;

-- Silent: runs "succeed" but the last two found nothing (usually 10-14).
INSERT INTO fetch_run (source_id, started_at, finished_at, status, items_found)
SELECT 'careers-tcs', t, t + interval '40 seconds', 'success',
       CASE WHEN n < 2 THEN 0 ELSE 10 + n % 5 END
FROM generate_series(0, 11) n
CROSS JOIN LATERAL (SELECT now() - interval '15 minutes' - n * interval '2 hours' AS t) x;

-- Paused two days ago: shown greyed out, no alert.
INSERT INTO fetch_run (source_id, started_at, finished_at, status, items_found)
SELECT 'nitt-hostel', t, t + interval '15 seconds', 'success', 2
FROM generate_series(48, 60) n
CROSS JOIN LATERAL (SELECT now() - n * interval '1 hour' AS t) x;

-- careers-infosys: no runs at all.

-- --------------------------------------------------------------------------
-- Quarantine
-- --------------------------------------------------------------------------
INSERT INTO quarantine (source_id, reason, payload_ref, created_at, resolved, replayed_at, replay_count) VALUES
  ('careers-zoho', 'schema: posting is missing required field "apply_url"',
   'snapshots/careers-zoho/8f3a1c.html.gz', now() - interval '3 hours', false, NULL, 0),
  ('nitt-notices', 'parse: PDF attachment could not be decoded (encrypted)',
   'snapshots/nitt-notices/2b91e0.pdf', now() - interval '6 hours', false, NULL, 0),
  -- replayed 40 min ago, never confirmed -> "replay stuck"
  ('careers-tcs', 'schema: 0 postings parsed from a non-empty page',
   'snapshots/careers-tcs/c07d44.html.gz', now() - interval '2 hours', false,
   now() - interval '40 minutes', 1),
  -- resolved: must not appear
  ('nitt-notices', 'dedupe: two notices with the same hash and different titles',
   'snapshots/nitt-notices/77aa02.html.gz', now() - interval '2 days', true,
   now() - interval '2 days', 1);

-- --------------------------------------------------------------------------
-- Application errors
-- --------------------------------------------------------------------------
-- Burst: embedding timeouts behind /rag/query, 27 in the last hour.
-- Also seen 2 days ago, so it is a known error that is now spiking.
INSERT INTO app_error (occurred_at, service, component, severity, error_type, message, fingerprint, request_id)
SELECT now() - n * interval '2 minutes', 'rag-api', 'POST /rag/query', 'error', 'ReadTimeout',
       'timed out after 10s calling embedding service', 'rag-embed-timeout',
       'req-' || lpad(n::text, 4, '0')
FROM generate_series(0, 26) n;
INSERT INTO app_error (occurred_at, service, component, severity, error_type, message, fingerprint)
SELECT now() - interval '2 days' - n * interval '5 minutes', 'rag-api', 'POST /rag/query', 'error',
       'ReadTimeout', 'timed out after 10s calling embedding service', 'rag-embed-timeout'
FROM generate_series(1, 3) n;

-- New: indexer started failing 35 min ago (6 times; below the burst limit).
INSERT INTO app_error (occurred_at, service, component, severity, error_type, message, fingerprint)
SELECT now() - interval '5 minutes' - n * interval '6 minutes', 'indexer', 'embed_batch', 'error',
       'ValueError', 'embedding dimension mismatch: expected 768, got 384', 'indexer-dim-mismatch'
FROM generate_series(0, 5) n;

-- Old, low-volume bug: listed in the table but raises no alert.
INSERT INTO app_error (occurred_at, service, component, severity, error_type, message, fingerprint, request_id) VALUES
  (now() - interval '5 days',  'jobs-api', 'GET /jobs/{job_id}', 'error', 'KeyError', '''company''', 'jobs-keyerror-company', 'req-a101'),
  (now() - interval '20 hours','jobs-api', 'GET /jobs/{job_id}', 'error', 'KeyError', '''company''', 'jobs-keyerror-company', 'req-a7f2'),
  (now() - interval '9 hours', 'jobs-api', 'GET /jobs/{job_id}', 'error', 'KeyError', '''company''', 'jobs-keyerror-company', 'req-b310'),
  (now() - interval '3 hours', 'jobs-api', 'GET /jobs/{job_id}', 'error', 'KeyError', '''company''', 'jobs-keyerror-company', 'req-c044');

-- Critical severity: one occurrence is enough.
INSERT INTO app_error (occurred_at, service, component, severity, error_type, message, fingerprint) VALUES
  (now() - interval '2 hours', 'notifier', 'send_digest', 'critical', 'SMTPAuthenticationError',
   '535 authentication failed for the digest sender account', 'notifier-smtp-auth');

-- Acknowledged after its last occurrence: must be hidden.
INSERT INTO app_error (occurred_at, service, component, severity, error_type, message, fingerprint)
SELECT now() - interval '6 hours' - n * interval '30 minutes', 'ingest-worker', 'xreadgroup', 'error',
       'ConnectionResetError', 'connection reset by peer while reading from redis', 'ingest-redis-reset'
FROM generate_series(0, 3) n;
INSERT INTO error_ack (fingerprint, acked_at, acked_by)
VALUES ('ingest-redis-reset', now() - interval '5 hours', 'admin');

-- --------------------------------------------------------------------------
-- API requests, last hour
-- --------------------------------------------------------------------------
-- /rag/query: 1 in 8 requests times out (12.5% 5xx), p95 = 10 s.
INSERT INTO api_request (ts, service, route, method, status, latency_ms, request_id)
SELECT now() - n * interval '9 seconds', 'rag-api', '/rag/query', 'POST',
       CASE WHEN n % 8 = 0 THEN 504 WHEN n % 37 = 0 THEN 422 ELSE 200 END,
       CASE WHEN n % 8 = 0 THEN 10000 ELSE 700 + (n % 9) * 90 END,
       'req-' || lpad(n::text, 4, '0')
FROM generate_series(0, 399) n;

-- /rag/health: fine.
INSERT INTO api_request (ts, service, route, method, status, latency_ms)
SELECT now() - n * interval '30 seconds', 'rag-api', '/rag/health', 'GET', 200, 4 + n % 3
FROM generate_series(0, 119) n;

-- /jobs: 1% 5xx, under the 2% warning line.
INSERT INTO api_request (ts, service, route, method, status, latency_ms)
SELECT now() - n * interval '12 seconds', 'jobs-api', '/jobs', 'GET',
       CASE WHEN n % 100 = 0 THEN 500 ELSE 200 END, 120 + (n % 10) * 25
FROM generate_series(0, 299) n;

-- /jobs/{job_id}: 4% 5xx (warning) plus some ordinary 404s.
INSERT INTO api_request (ts, service, route, method, status, latency_ms)
SELECT now() - n * interval '24 seconds', 'jobs-api', '/jobs/{job_id}', 'GET',
       CASE WHEN n % 25 = 0 THEN 500 WHEN n % 10 = 3 THEN 404 ELSE 200 END, 60 + (n % 7) * 10
FROM generate_series(0, 149) n;

-- /jobs/search: no errors but slow (p95 ~3.5 s, warning).
INSERT INTO api_request (ts, service, route, method, status, latency_ms)
SELECT now() - n * interval '18 seconds', 'jobs-api', '/jobs/search', 'GET', 200, 400 + (n % 20) * 170
FROM generate_series(0, 199) n;

-- Too little traffic to judge: 2 of 3 failed, but no alert.
INSERT INTO api_request (ts, service, route, method, status, latency_ms) VALUES
  (now() - interval '10 minutes', 'jobs-api', '/jobs/{job_id}/apply', 'POST', 500, 300),
  (now() - interval '20 minutes', 'jobs-api', '/jobs/{job_id}/apply', 'POST', 500, 280),
  (now() - interval '30 minutes', 'jobs-api', '/jobs/{job_id}/apply', 'POST', 201, 150);

-- --------------------------------------------------------------------------
-- Heartbeats (no row at all for "scheduler")
-- --------------------------------------------------------------------------
INSERT INTO service_heartbeat (service, instance, last_seen, version) VALUES
  ('ingest-worker', 'worker-1', now() - interval '20 seconds', 'a1c9e2f'),
  ('ingest-worker', 'worker-2', now() - interval '15 minutes', 'a1c9e2f'),  -- dead, but worker-1 is up
  ('indexer',       'indexer-1', now() - interval '25 seconds', 'a1c9e2f'),
  ('rag-api',       'rag-1',     now() - interval '10 seconds', 'a1c9e2f'),
  ('jobs-api',      'jobs-1',    now() - interval '3 minutes',  'a1c9e2f'), -- late
  ('notifier',      'notifier-1',now() - interval '40 seconds', 'a1c9e2f');

-- --------------------------------------------------------------------------
-- Documents / indexing
-- --------------------------------------------------------------------------
-- 200 indexed normally.
INSERT INTO document (source_id, ingested_at, indexed_at)
SELECT 'nitt-notices', now() - n * interval '10 minutes',
       now() - n * interval '10 minutes' + interval '40 seconds'
FROM generate_series(10, 209) n;
-- 75 waiting, the oldest for 95 min (backlog + lag warnings).
INSERT INTO document (source_id, ingested_at)
SELECT 'careers-zoho', now() - interval '95 minutes' + n * interval '1 minute'
FROM generate_series(0, 74) n;
-- 4 failed with the same error the indexer is reporting.
INSERT INTO document (source_id, ingested_at, index_error)
SELECT 'careers-tcs', now() - interval '50 minutes',
       'ValueError: embedding dimension mismatch: expected 768, got 384'
FROM generate_series(1, 4);
