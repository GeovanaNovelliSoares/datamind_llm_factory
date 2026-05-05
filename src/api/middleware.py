"""Request context middleware — request_id, structured logging, timing."""
import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = structlog.get_logger(__name__)


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = str(uuid.uuid4())
        start = time.perf_counter()

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )
        request.state.request_id = request_id
        logger.info("request_started")

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000
            logger.error("request_failed", duration_ms=round(duration_ms, 2), error=str(exc))
            raise

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info("request_finished", status_code=response.status_code, duration_ms=round(duration_ms, 2))

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Process-Time-Ms"] = str(round(duration_ms, 2))
        return response
