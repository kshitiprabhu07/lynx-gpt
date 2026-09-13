from fastapi import Request
from fastapi.responses import JSONResponse

EXEMPT_PATHS = {"/health", "/metrics"}   # never rate-limit your own probes


def install_rate_limit(app, limiter):
    @app.middleware("http")
    async def _rate_limit(request: Request, call_next):
        if request.url.path in EXEMPT_PATHS:
            return await call_next(request)

        client_ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or (request.client.host if request.client else "unknown")
        )
        allowed, remaining = limiter.check(client_ip)

        if not allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded",
                    "retry_after_s": limiter.window_ms // 1000,
                },
                headers={
                    "Retry-After": str(limiter.window_ms // 1000),
                    "X-RateLimit-Limit": str(limiter.limit),
                    "X-RateLimit-Remaining": "0",
                },
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(limiter.limit)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        return response