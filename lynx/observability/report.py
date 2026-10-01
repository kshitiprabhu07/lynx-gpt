"""How the rest of Lynx GPT feeds the admin console.

    from lynx.observability import report

    # RAG / Jobs API (FastAPI): logs every request and every unhandled error
    report.install(app, service="rag-api")

    # Workers, indexer, notifier, scheduler
    with report.capture("indexer", component="embed_batch", batch_id=bid):
        embed(batch)

    # Every long-running process, every ~30 s
    report.heartbeat("indexer", version=GIT_SHA)

Reporting must never take the application down with it: every function
here swallows its own failures and logs them instead.

Each call opens a short-lived connection, which is fine for the demo.
If request volume grows, switch _conn() to a psycopg_pool.ConnectionPool.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import socket
import time
import traceback
import uuid
from contextlib import contextmanager

import psycopg
from psycopg.types.json import Jsonb

from lynx.config import POSTGRES_DSN

log = logging.getLogger("lynx.observability")
INSTANCE = os.environ.get("LYNX_INSTANCE", socket.gethostname())

# Strip the parts of a message that change between occurrences, so
# "timeout after 10.2s for /jobs/5512" and "... 9.8s for /jobs/77" group together.
_VOLATILE = [
    (re.compile(r"https?://\S+"), "<url>"),
    (re.compile(r"\b[0-9a-f]{8,}\b", re.I), "<hex>"),
    (re.compile(r"\d+(\.\d+)?"), "<n>"),        # also inside "10.2s", "http2"
    (re.compile(r"'[^']*'|\"[^\"]*\""), "<str>"),
]


def _normalise(message: str) -> str:
    for pattern, repl in _VOLATILE:
        message = pattern.sub(repl, message)
    return message[:300]


def _top_frame(exc: BaseException | None) -> str:
    """file:function of the innermost frame. Line numbers are left out so a
    redeploy that shifts code does not split one error into two groups."""
    if exc is None or exc.__traceback__ is None:
        return ""
    frame = traceback.extract_tb(exc.__traceback__)[-1]
    return f"{os.path.basename(frame.filename)}:{frame.name}"


def fingerprint(service: str, error_type: str, message: str, frame: str = "") -> str:
    key = f"{service}|{error_type}|{frame}|{_normalise(message)}"
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def _conn():
    return psycopg.connect(POSTGRES_DSN, connect_timeout=3, autocommit=True)


def record_error(service: str, exc: BaseException | None = None, *,
                 error_type: str | None = None, message: str | None = None,
                 component: str | None = None, severity: str = "error",
                 request_id: str | None = None, **context) -> None:
    """Record an exception, or a non-exception problem via error_type+message."""
    try:
        etype = error_type or (type(exc).__name__ if exc else "Error")
        msg = message or (str(exc) if exc else "") or etype
        fp = fingerprint(service, etype, msg, _top_frame(exc))
        if exc is not None:
            context.setdefault("traceback", "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__))[-4000:])
        context.setdefault("instance", INSTANCE)
        with _conn() as conn:
            conn.execute(
                """INSERT INTO app_error (service, component, severity, error_type,
                                          message, fingerprint, request_id, context)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (service, component, severity, etype, msg[:2000], fp, request_id,
                 Jsonb(context)))
    except Exception:
        log.exception("could not record error for %s", service)


def record_request(service: str, route: str, method: str, status: int,
                   latency_ms: int, request_id: str | None = None) -> None:
    try:
        with _conn() as conn:
            conn.execute(
                """INSERT INTO api_request (service, route, method, status, latency_ms, request_id)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (service, route, method, status, latency_ms, request_id))
    except Exception:
        log.exception("could not record request for %s", service)


def heartbeat(service: str, version: str | None = None) -> None:
    try:
        with _conn() as conn:
            conn.execute(
                """INSERT INTO service_heartbeat (service, instance, last_seen, version)
                   VALUES (%s, %s, now(), %s)
                   ON CONFLICT (service, instance)
                   DO UPDATE SET last_seen = now(), version = EXCLUDED.version""",
                (service, INSTANCE, version))
    except Exception:
        log.exception("could not send heartbeat for %s", service)


@contextmanager
def capture(service: str, component: str | None = None, *, reraise: bool = True, **context):
    """Record any exception raised inside the block, then re-raise it."""
    try:
        yield
    except Exception as exc:
        record_error(service, exc, component=component, **context)
        if reraise:
            raise


def install(app, service: str) -> None:
    """FastAPI/Starlette middleware: one api_request row per request, and an
    app_error row for every unhandled exception. Adds an X-Request-ID header
    so a user-reported failure can be found in the console."""
    from starlette.concurrency import run_in_threadpool
    from starlette.routing import Match

    def route_template(request) -> str:
        # Use /jobs/{job_id}, not /jobs/5512, or every id becomes its own row.
        for route in request.app.router.routes:
            match, _ = route.matches(request.scope)
            if match == Match.FULL:
                return getattr(route, "path", request.url.path)
        return "<unmatched>"

    @app.middleware("http")
    async def observe(request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
        route = route_template(request)
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["x-request-id"] = request_id
            return response
        except Exception as exc:
            await run_in_threadpool(record_error, service, exc,
                                    component=f"{request.method} {route}",
                                    request_id=request_id)
            raise
        finally:
            ms = int((time.perf_counter() - start) * 1000)
            await run_in_threadpool(record_request, service, route, request.method,
                                    status, ms, request_id)
